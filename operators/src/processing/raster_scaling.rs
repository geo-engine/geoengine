use crate::engine::{
    ExecutionContext, InitializedRasterOperator, Operator, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, SingleRasterSource, TypedRasterQueryProcessor,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::raster::{scale_pixel, unscale_pixel, MapPixelsParallel, NoDataValue};
use geoengine_datatypes::raster::{Pixel, RasterProperties, RasterTile2D};
use geoengine_datatypes::{
    primitives::Measurement,
    raster::{RasterDataType, RasterPropertiesKey},
};
use num::FromPrimitive;
use num_traits::AsPrimitive;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RasterUnscaleParams {
    scale_with: PropertiesKeyOrValue,
    offset_by: PropertiesKeyOrValue,
    output_no_data_value: Option<f64>,
    output_type: Option<RasterDataType>,
    output_measurement: Option<Measurement>,
    scaling_mode: ScalingMode,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub enum ScalingMode {
    Scale,
    Unscale,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
enum PropertiesKeyOrValue {
    MetadataKey(RasterPropertiesKey),
    Value(f64),
}

/// The radiance operator converts a raw MSG raster into radiance.
/// This is done by applying the following formula to every pixel:
///
/// `p_new = offset + p_old * slope`
///
/// Here, `p_old` and `p_new` refer to the old and new pixel values,
/// while slope and offset are properties attached to the input
/// raster.
/// The exact names of the properties are:
///
/// - offset: `msg.calibration_offset`
/// - slope: `msg.calibration_slope`
pub type RasterUnscaleOperator = Operator<RasterUnscaleParams, SingleRasterSource>;

pub struct InitializedRasterUnscaleOperator {
    scale_with: PropertiesKeyOrValue,
    offset_by: PropertiesKeyOrValue,
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    scaling_mode: ScalingMode,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for RasterUnscaleOperator {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let input = self.sources.raster.initialize(context).await?;
        let in_desc = input.result_descriptor();

        let out_desc = RasterResultDescriptor {
            spatial_reference: in_desc.spatial_reference,
            data_type: self.params.output_type.unwrap_or(in_desc.data_type),
            measurement: self
                .params
                .output_measurement
                .unwrap_or(in_desc.measurement.clone()),
            no_data_value: self.params.output_no_data_value,
        };

        let initialized_operator = InitializedRasterUnscaleOperator {
            scale_with: self.params.scale_with,
            offset_by: self.params.offset_by,
            result_descriptor: out_desc,
            source: input,
            scaling_mode: self.params.scaling_mode,
        };

        Ok(initialized_operator.boxed())
    }
}

impl InitializedRasterOperator for InitializedRasterUnscaleOperator {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let out_data_type = self.result_descriptor.data_type;
        let scale_with = self.scale_with.clone();
        let offset_by = self.offset_by.clone();
        let no_data_value = self.result_descriptor.no_data_value;
        let source = self.source.query_processor()?;
        let scaling_mode = self.scaling_mode;

        let res_op = call_on_generic_raster_processor!(source, source_proc => {
            call_generic_raster_processor!(out_data_type,
                RasterUnscaleProcessor::create_boxed(scale_with, offset_by, no_data_value, source_proc, scaling_mode)
            )
        });

        Ok(res_op)
    }
}

struct RasterUnscaleProcessor<Q, In, Out>
where
    Q: RasterQueryProcessor<RasterType = In>,
{
    source: Q,
    scale_with: PropertiesKeyOrValue,
    offset_by: PropertiesKeyOrValue,
    no_data_value: Option<Out>,
    scaling_mode: ScalingMode,
}

impl<Q, In, Out> RasterUnscaleProcessor<Q, In, Out>
where
    Q: RasterQueryProcessor<RasterType = In> + 'static,
    In: Pixel,
    Out: Pixel,
    Out: FromPrimitive,
    In: AsPrimitive<Out> + FromPrimitive,
    f64: AsPrimitive<Out> + AsPrimitive<In>,
{
    pub fn create_boxed(
        scale_with: PropertiesKeyOrValue,
        offset_by: PropertiesKeyOrValue,
        no_data_value_option: Option<f64>,
        source: Q,
        scaling_mode: ScalingMode,
    ) -> Box<dyn RasterQueryProcessor<RasterType = Out>> {
        let no_data_value = no_data_value_option.map(|v| v.as_());
        RasterUnscaleProcessor {
            source,
            scale_with,
            offset_by,
            no_data_value,
            scaling_mode,
        }
        .boxed()
    }

    async fn process_tile_async(
        &self,
        tile: RasterTile2D<In>,
        pool: Arc<ThreadPool>,
    ) -> Result<RasterTile2D<Out>> {
        match self.scaling_mode {
            ScalingMode::Scale => self.scale_tile_async(tile, pool).await,
            ScalingMode::Unscale => self.unscale_tile_async(tile, pool).await,
        }
    }

    async fn unscale_tile_async(
        &self,
        tile: RasterTile2D<In>,
        pool: Arc<ThreadPool>,
    ) -> Result<RasterTile2D<Out>> {
        let offset_by = Self::prop_value_out(&self.offset_by, &tile.properties)?;
        let scale_with = Self::prop_value_out(&self.scale_with, &tile.properties)?;

        let no_data_value = self
            .no_data_value
            .or(tile
                .no_data_value()
                .and_then(|n| unscale_pixel(n, scale_with, offset_by)))
            .unwrap_or_else(Out::zero);

        let res_tile = crate::util::spawn_blocking_with_thread_pool(pool, move || {
            tile.map_pixels_parallel(|p| unscale_pixel(p, scale_with, offset_by), no_data_value)
        })
        .await?;

        Ok(res_tile)
    }

    async fn scale_tile_async(
        &self,
        tile: RasterTile2D<In>,
        pool: Arc<ThreadPool>,
    ) -> Result<RasterTile2D<Out>> {
        let offset_by = Self::prop_value_in(&self.offset_by, &tile.properties)?;
        let scale_with = Self::prop_value_in(&self.scale_with, &tile.properties)?;

        let no_data_value = self
            .no_data_value
            .or(tile
                .no_data_value()
                .and_then(|n| scale_pixel(n, scale_with, offset_by)))
            .unwrap_or_else(Out::zero);

        let res_tile = crate::util::spawn_blocking_with_thread_pool(pool, move || {
            tile.map_pixels_parallel(|p| scale_pixel(p, scale_with, offset_by), no_data_value)
        })
        .await?;

        Ok(res_tile)
    }

    fn prop_value_out(
        prop_key_or_value: &PropertiesKeyOrValue,
        props: &RasterProperties,
    ) -> Result<Out> {
        let value = match prop_key_or_value {
            PropertiesKeyOrValue::MetadataKey(key) => props.number_property::<Out>(key)?,
            PropertiesKeyOrValue::Value(value) => value.as_(),
        };
        Ok(value)
    }

    fn prop_value_in(
        prop_key_or_value: &PropertiesKeyOrValue,
        props: &RasterProperties,
    ) -> Result<In> {
        let value = match prop_key_or_value {
            PropertiesKeyOrValue::MetadataKey(key) => props.number_property::<In>(key)?,
            PropertiesKeyOrValue::Value(value) => value.as_(),
        };
        Ok(value)
    }
}

#[async_trait]
impl<Q, In, Out> RasterQueryProcessor for RasterUnscaleProcessor<Q, In, Out>
where
    In: Pixel + AsPrimitive<Out> + FromPrimitive,
    Out: Pixel + FromPrimitive,
    f64: AsPrimitive<Out> + AsPrimitive<In>,
    Q: RasterQueryProcessor<RasterType = In> + Sync + Send + 'static,
{
    type RasterType = Out;

    async fn raster_query<'a>(
        &'a self,
        query: geoengine_datatypes::primitives::RasterQueryRectangle,
        ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<
        futures::stream::BoxStream<
            'a,
            Result<geoengine_datatypes::raster::RasterTile2D<Self::RasterType>>,
        >,
    > {
        let src = self.source.raster_query(query, ctx).await?;
        let rs = src.and_then(move |tile| self.process_tile_async(tile, ctx.thread_pool().clone()));
        Ok(rs.boxed())
    }
}
