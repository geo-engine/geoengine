use crate::engine::{
    ExecutionContext, InitializedRasterOperator, Operator, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, SingleRasterSource, TypedRasterQueryProcessor,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::raster::{Scale, ScaleElementsParallel, Unscale, UnscaleElementsParallel};
use geoengine_datatypes::{
    primitives::Measurement,
    raster::{Pixel, RasterProperties, RasterPropertiesKey, RasterTile2D},
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
    output_no_data_value: f64,
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

/// The raster scaling operator scales/unscales the values of a raster by a given scale factor and offset.
/// This is done by applying the following formulas to every pixel.
/// For unscaling the formula is:
///
/// `p_new = p_old * slope + offset`
///
/// For scaling the formula is:
///
/// `p_new = (p_old - offset) / slope`
///
/// `p_old` and `p_new` refer to the old and new pixel values,
/// The slope and offset values are either properties attached to the input raster or a fixed value.
///
/// An example for Meteosat Second Generation properties is:
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

        let out_no_data_value = self.params.output_no_data_value;

        let out_desc = RasterResultDescriptor {
            spatial_reference: in_desc.spatial_reference,
            data_type: in_desc.data_type,
            measurement: self
                .params
                .output_measurement
                .unwrap_or(in_desc.measurement.clone()),
            no_data_value: Some(out_no_data_value),
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
        let scale_with = self.scale_with.clone();
        let offset_by = self.offset_by.clone();
        let no_data_value = self.result_descriptor.no_data_value;
        let source = self.source.query_processor()?;
        let scaling_mode = self.scaling_mode;

        let res_op = call_on_generic_raster_processor!(source, source_proc => { TypedRasterQueryProcessor::from(RasterScalingProcessor::create_boxed(scale_with, offset_by,  source_proc, no_data_value, scaling_mode)) });

        Ok(res_op)
    }
}

struct RasterScalingProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    source: Q,
    scale_with: PropertiesKeyOrValue,
    offset_by: PropertiesKeyOrValue,
    no_data_value: P,
    scaling_mode: ScalingMode,
}

impl<Q, P> RasterScalingProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P> + 'static,
    P: Pixel + Scale + Unscale + FromPrimitive,
    f64: AsPrimitive<P>,
{
    pub fn create_boxed(
        scale_with: PropertiesKeyOrValue,
        offset_by: PropertiesKeyOrValue,
        source: Q,
        no_data_value: Option<f64>,
        scaling_mode: ScalingMode,
    ) -> Box<dyn RasterQueryProcessor<RasterType = P>> {
        RasterScalingProcessor {
            source,
            scale_with,
            offset_by,
            no_data_value: no_data_value
                .map(|v| v.as_())
                .unwrap_or(P::DEFAULT_NO_DATA_VALUE),
            scaling_mode,
        }
        .boxed()
    }
    async fn scale_tile_async(
        &self,
        tile: RasterTile2D<P>,
        pool: Arc<ThreadPool>,
    ) -> Result<RasterTile2D<P>> {
        let offset_by = Self::prop_value(&self.offset_by, &tile.properties)?;
        let scale_with = Self::prop_value(&self.scale_with, &tile.properties)?;
        let scaling_mode = self.scaling_mode;

        let _no_data_value = self.no_data_value;

        let res_tile =
            crate::util::spawn_blocking_with_thread_pool(pool, move || match scaling_mode {
                ScalingMode::Scale => tile.scale_elements_parallel(scale_with, offset_by),
                ScalingMode::Unscale => tile.unscale_elements_parallel(scale_with, offset_by),
            })
            .await?;

        Ok(res_tile)
    }

    fn prop_value(prop_key_or_value: &PropertiesKeyOrValue, props: &RasterProperties) -> Result<P> {
        let value = match prop_key_or_value {
            PropertiesKeyOrValue::MetadataKey(key) => props.number_property::<P>(key)?,
            PropertiesKeyOrValue::Value(value) => value.as_(),
        };
        Ok(value)
    }
}

#[async_trait]
impl<Q, P> RasterQueryProcessor for RasterScalingProcessor<Q, P>
where
    P: Pixel + Scale + Unscale + FromPrimitive,
    f64: AsPrimitive<P>,
    Q: RasterQueryProcessor<RasterType = P> + 'static,
{
    type RasterType = P;

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
