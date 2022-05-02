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
        RasterScalingProcessor::create(
            scale_with,
            offset_by,
            source,
            no_data_value
                .map(|v| v.as_())
                .unwrap_or(P::DEFAULT_NO_DATA_VALUE),
            scaling_mode,
        )
        .boxed()
    }

    pub fn create(
        scale_with: PropertiesKeyOrValue,
        offset_by: PropertiesKeyOrValue,
        source: Q,
        no_data_value: P,
        scaling_mode: ScalingMode,
    ) -> RasterScalingProcessor<Q, P> {
        RasterScalingProcessor {
            source,
            scale_with,
            offset_by,
            no_data_value,
            scaling_mode,
        }
    }

    async fn scale_tile_async(
        &self,
        tile: RasterTile2D<P>,
        pool: Arc<ThreadPool>,
    ) -> Result<RasterTile2D<P>> {
        let offset_by = Self::prop_value(&self.offset_by, &tile.properties)?;
        let scale_with = Self::prop_value(&self.scale_with, &tile.properties)?;
        let scaling_mode = self.scaling_mode;

        let no_data_value = self.no_data_value;

        let res_tile =
            crate::util::spawn_blocking_with_thread_pool(pool, move || match scaling_mode {
                ScalingMode::Scale => {
                    tile.scale_elements_parallel(scale_with, offset_by, no_data_value)
                }
                ScalingMode::Unscale => {
                    tile.unscale_elements_parallel(scale_with, offset_by, no_data_value)
                }
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

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{SpatialPartition2D, SpatialResolution, TimeInterval},
        raster::{Grid2D, GridOrEmpty2D, RasterDataType, TileInformation, TilingSpecification},
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use crate::{
        engine::{ChunkByteSize, MockExecutionContext},
        mock::{MockRasterSource, MockRasterSourceParams},
    };

    use super::*;

    #[tokio::test]
    async fn test_scaling() {
        let grid_shape = [2, 2].into();

        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels: grid_shape,
        };

        let no_data_value = 6;

        let raster = Grid2D::new(grid_shape, vec![7_u8, 7, 7, 6], Some(no_data_value)).unwrap();

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);
        let query_ctx = ctx.mock_query_context(ChunkByteSize::test_default());

        let mut raster_props = RasterProperties::default();
        raster_props.set_scale(2.0);
        raster_props.set_offset(1.0);

        let raster_tile = RasterTile2D::new_with_tile_info_and_properties(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: grid_shape,
            },
            raster.into(),
            raster_props,
        );

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: Some(no_data_value as f64),
                },
            },
        }
        .boxed();

        let scale_with = PropertiesKeyOrValue::MetadataKey(RasterPropertiesKey {
            domain: None,
            key: "scale".to_string(),
        });
        let offset_by = PropertiesKeyOrValue::MetadataKey(RasterPropertiesKey {
            domain: None,
            key: "offset".to_string(),
        });

        let scaling_mode = ScalingMode::Unscale;

        let output_measurement = None;

        let op = RasterScalingOperator {
            params: RasterScalingParams {
                scale_with,
                offset_by,
                output_no_data_value: 255.,
                output_measurement,
                scaling_mode,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let initialized_op = op.initialize(&ctx).await.unwrap();

        let result_descriptor = initialized_op.result_descriptor();

        assert_eq!(result_descriptor.data_type, RasterDataType::U8);
        assert_eq!(result_descriptor.measurement, Measurement::Unitless);

        let query_processor = initialized_op.query_processor().unwrap();

        let query = geoengine_datatypes::primitives::RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((0., 0.).into(), (2., -2.).into()).unwrap(),
            spatial_resolution: SpatialResolution::one(),
            time_interval: TimeInterval::default(),
        };

        let typed_processor = match query_processor {
            TypedRasterQueryProcessor::U8(rqp) => rqp,
            _ => panic!("expected TypedRasterQueryProcessor::U8"),
        };

        let stream = typed_processor
            .raster_query(query, &query_ctx)
            .await
            .unwrap();

        let results = stream.collect::<Vec<Result<RasterTile2D<u8>>>>().await;

        // assert_eq!((&results).len(), 1);

        let result_tile = results.as_slice()[0].as_ref().unwrap();

        let result_grid = result_tile.grid_array.clone();

        match result_grid {
            GridOrEmpty2D::Grid(grid) => {
                assert_eq!(grid.shape, [2, 2].into());
                assert_eq!(grid.data, &[15, 15, 15, 255]);
                assert_eq!(grid.no_data_value, Some(255));
            }
            _ => panic!("expected GridOrEmpty2D::Grid"),
        }
    }
}
