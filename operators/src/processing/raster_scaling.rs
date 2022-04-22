use crate::engine::{
    ExecutionContext, InitializedRasterOperator, Operator, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, SingleRasterSource, TypedRasterQueryProcessor,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::raster::{UnscaleConvert, UnscaleConvertElementsParallel};
use geoengine_datatypes::{
    primitives::Measurement,
    raster::{
        NoDataValue, Pixel, RasterDataType, RasterProperties, RasterPropertiesKey, RasterTile2D,
        ScaleConvert, ScaleConvertElementsParallel,
    },
};
use num::FromPrimitive;
use num_traits::AsPrimitive;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RasterScalingParams {
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
pub type RasterScalingOperator = Operator<RasterScalingParams, SingleRasterSource>;

pub struct InitializedRasterScalingOperator {
    scale_with: PropertiesKeyOrValue,
    offset_by: PropertiesKeyOrValue,
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    scaling_mode: ScalingMode,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for RasterScalingOperator {
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

        let initialized_operator = InitializedRasterScalingOperator {
            scale_with: self.params.scale_with,
            offset_by: self.params.offset_by,
            result_descriptor: out_desc,
            source: input,
            scaling_mode: self.params.scaling_mode,
        };

        Ok(initialized_operator.boxed())
    }
}

impl InitializedRasterOperator for InitializedRasterScalingOperator {
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

        let res_op = match scaling_mode {
            ScalingMode::Scale => {
                call_on_generic_raster_processor!(source, source_proc => {
                call_generic_raster_processor!(out_data_type,
                    RasterScaleProcessor::create_boxed(scale_with, offset_by, no_data_value, source_proc)
                )})
            }

            ScalingMode::Unscale => {
                call_on_generic_raster_processor!(source, source_proc => {
                call_generic_raster_processor!(out_data_type,
                    RasterUnscaleProcessor::create_boxed(scale_with, offset_by, no_data_value, source_proc)
                )})
            }
        };

        Ok(res_op)
    }
}

struct RasterScaleProcessor<Q, In, Out>
where
    Q: RasterQueryProcessor<RasterType = In>,
{
    source: Q,
    scale_with: PropertiesKeyOrValue,
    offset_by: PropertiesKeyOrValue,
    no_data_value: Option<Out>,
}

impl<Q, In, Out> RasterScaleProcessor<Q, In, Out>
where
    Q: RasterQueryProcessor<RasterType = In> + 'static,
    In: Pixel + ScaleConvert<Out>,
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
    ) -> Box<dyn RasterQueryProcessor<RasterType = Out>> {
        let no_data_value = no_data_value_option.map(|v| v.as_());
        RasterScaleProcessor {
            source,
            scale_with,
            offset_by,
            no_data_value,
        }
        .boxed()
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
                .and_then(|n| n.scale_convert(scale_with, offset_by)))
            .unwrap_or_else(Out::zero);

        let res_tile = crate::util::spawn_blocking_with_thread_pool(pool, move || {
            tile.scale_convert_elements_parallel(scale_with, offset_by, no_data_value)
        })
        .await?;

        Ok(res_tile)
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
impl<Q, In, Out> RasterQueryProcessor for RasterScaleProcessor<Q, In, Out>
where
    In: Pixel + AsPrimitive<Out> + FromPrimitive,
    Out: Pixel + FromPrimitive,
    f64: AsPrimitive<Out> + AsPrimitive<In>,
    Q: RasterQueryProcessor<RasterType = In> + Sync + Send + 'static,
    In: Pixel + ScaleConvert<Out>,
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
        let rs = src.and_then(move |tile| self.scale_tile_async(tile, ctx.thread_pool().clone()));
        Ok(rs.boxed())
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
}

impl<Q, In, Out> RasterUnscaleProcessor<Q, In, Out>
where
    Q: RasterQueryProcessor<RasterType = In> + 'static,
    In: Pixel,
    Out: Pixel,
    Out: UnscaleConvert<In> + FromPrimitive,
    In: AsPrimitive<Out> + FromPrimitive,
    f64: AsPrimitive<Out> + AsPrimitive<In>,
{
    pub fn create_boxed(
        scale_with: PropertiesKeyOrValue,
        offset_by: PropertiesKeyOrValue,
        no_data_value_option: Option<f64>,
        source: Q,
    ) -> Box<dyn RasterQueryProcessor<RasterType = Out>> {
        let no_data_value = no_data_value_option.map(|v| v.as_());
        RasterUnscaleProcessor {
            source,
            scale_with,
            offset_by,
            no_data_value,
        }
        .boxed()
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
                .and_then(|n| UnscaleConvert::unscale_convert(n, scale_with, offset_by)))
            .unwrap_or_else(Out::zero);

        let res_tile = crate::util::spawn_blocking_with_thread_pool(pool, move || {
            tile.unscale_convert_elements_parallel(scale_with, offset_by, no_data_value)
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
}

#[async_trait]
impl<Q, In, Out> RasterQueryProcessor for RasterUnscaleProcessor<Q, In, Out>
where
    In: Pixel + AsPrimitive<Out> + FromPrimitive,
    Out: Pixel + FromPrimitive + UnscaleConvert<In>,
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
        let rs = src.and_then(move |tile| self.unscale_tile_async(tile, ctx.thread_pool().clone()));
        Ok(rs.boxed())
    }
}
