use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources, Operator,
    OperatorName, RasterBandDescriptor, RasterBandDescriptors, RasterOperator,
    RasterQueryProcessor, RasterResultDescriptor, SingleRasterSource, TypedRasterQueryProcessor,
    WorkflowOperatorPath,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};

use geoengine_datatypes::raster::{
    CheckedMulThenAddTransformation, CheckedSubThenDivTransformation, ElementScaling,
    ScalingTransformation,
};
use geoengine_datatypes::{
    primitives::Measurement,
    raster::{Pixel, RasterPropertiesKey, RasterTile2D},
};
use num::FromPrimitive;
use num_traits::AsPrimitive;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RasterScalingParams {
    slope: SlopeOffsetSelection,
    offset: SlopeOffsetSelection,
    output_measurement: Option<Measurement>,
    scaling_mode: ScalingMode,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub enum ScalingMode {
    MulSlopeAddOffset,
    SubOffsetDivSlope,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", tag = "type")]
enum SlopeOffsetSelection {
    Auto,
    MetadataKey(RasterPropertiesKey),
    Constant { value: f64 },
}

impl Default for SlopeOffsetSelection {
    fn default() -> Self {
        Self::Auto
    }
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
pub type RasterScaling = Operator<RasterScalingParams, SingleRasterSource>;

impl OperatorName for RasterScaling {
    const TYPE_NAME: &'static str = "RasterScaling";
}

pub struct InitializedRasterScalingOperator {
    name: CanonicOperatorName,
    slope: SlopeOffsetSelection,
    offset: SlopeOffsetSelection,
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    scaling_mode: ScalingMode,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for RasterScaling {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);

        let input = self.sources.initialize_sources(path, context).await?;
        let in_desc = input.raster.result_descriptor();

        // TODO: implement multi-band functionality and remove this check
        ensure!(
            in_desc.bands.len() == 1,
            crate::error::OperatorDoesNotSupportMultiBandsSourcesYet {
                operator: RasterScaling::TYPE_NAME
            }
        );

        let out_desc = RasterResultDescriptor {
            spatial_reference: in_desc.spatial_reference,
            data_type: in_desc.data_type,
            time: in_desc.time,
            geo_transform_x: in_desc.tiling_geo_transform(),
            pixel_bounds_x: in_desc.tiling_pixel_bounds(),
            bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                in_desc.bands[0].name.clone(),
                self.params
                    .output_measurement
                    .unwrap_or_else(|| in_desc.bands[0].measurement.clone()),
            )])?,
        };

        let initialized_operator = InitializedRasterScalingOperator {
            name,
            slope: self.params.slope,
            offset: self.params.offset,
            result_descriptor: out_desc,
            source: input.raster,
            scaling_mode: self.params.scaling_mode,
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(RasterScaling);
}

impl InitializedRasterOperator for InitializedRasterScalingOperator {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let slope = self.slope.clone();
        let offset = self.offset.clone();
        let source = self.source.query_processor()?;
        let scaling_mode = self.scaling_mode;

        let res = match scaling_mode {
            ScalingMode::SubOffsetDivSlope => {
                call_on_generic_raster_processor!(source, source_proc => { TypedRasterQueryProcessor::from(create_boxed_processor::<_,_, CheckedSubThenDivTransformation>(self.result_descriptor.clone(), slope, offset,  source_proc)) })
            }
            ScalingMode::MulSlopeAddOffset => {
                call_on_generic_raster_processor!(source, source_proc => { TypedRasterQueryProcessor::from(create_boxed_processor::<_,_, CheckedMulThenAddTransformation>(self.result_descriptor.clone(), slope, offset,  source_proc)) })
            }
        };

        Ok(res)
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }
}

struct RasterTransformationProcessor<Q, P, S>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    source: Q,
    result_descriptor: RasterResultDescriptor,
    slope: SlopeOffsetSelection,
    offset: SlopeOffsetSelection,
    _transformation: PhantomData<S>,
}

fn create_boxed_processor<Q, P, S>(
    result_descriptor: RasterResultDescriptor,
    slope: SlopeOffsetSelection,
    offset: SlopeOffsetSelection,
    source: Q,
) -> Box<dyn RasterQueryProcessor<RasterType = P>>
where
    Q: RasterQueryProcessor<RasterType = P> + 'static,
    P: Pixel + FromPrimitive + 'static + Default,
    f64: AsPrimitive<P>,
    S: Send + Sync + 'static + ScalingTransformation<P>,
{
    RasterTransformationProcessor::<Q, P, S>::create(result_descriptor, slope, offset, source)
        .boxed()
}

impl<Q, P, S> RasterTransformationProcessor<Q, P, S>
where
    Q: RasterQueryProcessor<RasterType = P> + 'static,
    P: Pixel + FromPrimitive + 'static + Default,
    f64: AsPrimitive<P>,
    S: Send + Sync + 'static + ScalingTransformation<P>,
{
    pub fn create(
        result_descriptor: RasterResultDescriptor,
        slope: SlopeOffsetSelection,
        offset: SlopeOffsetSelection,
        source: Q,
    ) -> RasterTransformationProcessor<Q, P, S> {
        RasterTransformationProcessor {
            source,
            result_descriptor,
            slope,
            offset,
            _transformation: PhantomData,
        }
    }

    async fn scale_tile_async(
        &self,
        tile: RasterTile2D<P>,
        _pool: Arc<ThreadPool>,
    ) -> Result<RasterTile2D<P>> {
        // either use the provided metadata/constant or the default values from the properties
        let offset = match &self.offset {
            SlopeOffsetSelection::MetadataKey(key) => tile.properties.number_property::<P>(key)?,
            SlopeOffsetSelection::Constant { value } => value.as_(),
            SlopeOffsetSelection::Auto => tile.properties.offset().as_(),
        };

        let slope = match &self.slope {
            SlopeOffsetSelection::MetadataKey(key) => tile.properties.number_property::<P>(key)?,
            SlopeOffsetSelection::Constant { value } => value.as_(),
            SlopeOffsetSelection::Auto => tile.properties.scale().as_(),
        };

        let res_tile =
            crate::util::spawn_blocking(move || tile.transform_elements::<S>(slope, offset))
                .await?;

        Ok(res_tile)
    }
}

#[async_trait]
impl<Q, P, S> RasterQueryProcessor for RasterTransformationProcessor<Q, P, S>
where
    P: Pixel + FromPrimitive + 'static + Default,
    f64: AsPrimitive<P>,
    Q: RasterQueryProcessor<RasterType = P> + 'static,
    S: Send + Sync + 'static + ScalingTransformation<P>,
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

    fn raster_result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        engine::{ChunkByteSize, MockExecutionContext},
        mock::{MockRasterSource, MockRasterSourceParams},
    };
    use geoengine_datatypes::{
        primitives::{BandSelection, CacheHint, Coordinate2D, TimeInterval},
        raster::{
            BoundedGrid, GeoTransform, Grid2D, GridBoundingBox2D, GridOrEmpty2D, GridShape,
            GridShape2D, MaskedGrid2D, RasterDataType, RasterProperties, TileInformation,
            TilingSpecification,
        },
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use super::*;

    #[tokio::test]
    async fn test_unscale() {
        let tile_size_in_pixels = GridShape2D::new_2d(2, 2);
        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: None,
            geo_transform_x: GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
            pixel_bounds_x: tile_size_in_pixels.bounding_box(),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let tiling_specification = TilingSpecification::new(tile_size_in_pixels);
        let raster =
            MaskedGrid2D::from(Grid2D::new(tile_size_in_pixels, vec![7_u8, 7, 7, 6]).unwrap());

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
                tile_size_in_pixels,
            },
            0,
            raster.into(),
            raster_props,
            CacheHint::default(),
        );

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor,
            },
        }
        .boxed();

        let scaling_mode = ScalingMode::MulSlopeAddOffset;

        let output_measurement = None;

        let op = RasterScaling {
            params: RasterScalingParams {
                slope: SlopeOffsetSelection::Auto,
                offset: SlopeOffsetSelection::Auto,
                output_measurement,
                scaling_mode,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let initialized_op = op
            .initialize(WorkflowOperatorPath::initialize_root(), &ctx)
            .await
            .unwrap();

        let result_descriptor = initialized_op.result_descriptor();

        assert_eq!(result_descriptor.data_type, RasterDataType::U8);
        assert_eq!(
            result_descriptor.bands[0].measurement,
            Measurement::Unitless
        );

        let query_processor = initialized_op.query_processor().unwrap();

        let query = geoengine_datatypes::primitives::RasterQueryRectangle::new_with_grid_bounds(
            GridBoundingBox2D::new([0, 0], [1, 1]).unwrap(),
            TimeInterval::default(),
            BandSelection::first(),
        );

        let TypedRasterQueryProcessor::U8(typed_processor) = query_processor else {
            panic!("expected TypedRasterQueryProcessor::U8");
        };

        let stream = typed_processor
            .raster_query(query, &query_ctx)
            .await
            .unwrap();

        let results = stream.collect::<Vec<Result<RasterTile2D<u8>>>>().await;

        let result_tile = results.as_slice()[0].as_ref().unwrap();

        let result_grid = result_tile.grid_array.clone();

        match result_grid {
            GridOrEmpty2D::Grid(grid) => {
                assert_eq!(grid.shape(), &GridShape::new([2, 2]));

                let res = grid.masked_element_deref_iterator().collect::<Vec<_>>();

                let expected = vec![Some(15), Some(15), Some(15), Some(13)];

                assert_eq!(res, expected);
            }
            GridOrEmpty2D::Empty(_) => panic!("expected GridOrEmpty2D::Grid"),
        }
    }

    #[tokio::test]
    async fn test_scale() {
        let tile_size_in_pixels = GridShape2D::new_2d(2, 2);
        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: None,
            geo_transform_x: GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
            pixel_bounds_x: tile_size_in_pixels.bounding_box(),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let tiling_specification = TilingSpecification::new(tile_size_in_pixels);

        let raster =
            MaskedGrid2D::from(Grid2D::new(tile_size_in_pixels, vec![15_u8, 15, 15, 13]).unwrap());

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
                tile_size_in_pixels,
            },
            0,
            raster.into(),
            raster_props,
            CacheHint::default(),
        );

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor,
            },
        }
        .boxed();

        let scaling_mode = ScalingMode::SubOffsetDivSlope;

        let output_measurement = None;

        let params = RasterScalingParams {
            slope: SlopeOffsetSelection::Auto,
            offset: SlopeOffsetSelection::Auto,
            output_measurement,
            scaling_mode,
        };

        let op = RasterScaling {
            params,
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let initialized_op = op
            .initialize(WorkflowOperatorPath::initialize_root(), &ctx)
            .await
            .unwrap();

        let result_descriptor = initialized_op.result_descriptor();

        assert_eq!(result_descriptor.data_type, RasterDataType::U8);
        assert_eq!(
            result_descriptor.bands[0].measurement,
            Measurement::Unitless
        );

        let query_processor = initialized_op.query_processor().unwrap();

        let query = geoengine_datatypes::primitives::RasterQueryRectangle::new_with_grid_bounds(
            GridBoundingBox2D::new([0, 0], [1, 1]).unwrap(),
            TimeInterval::default(),
            BandSelection::first(),
        );

        let TypedRasterQueryProcessor::U8(typed_processor) = query_processor else {
            panic!("expected TypedRasterQueryProcessor::U8");
        };

        let stream = typed_processor
            .raster_query(query, &query_ctx)
            .await
            .unwrap();

        let results = stream.collect::<Vec<Result<RasterTile2D<u8>>>>().await;

        let result_tile = results.as_slice()[0].as_ref().unwrap();

        let result_grid = result_tile.grid_array.clone();

        match result_grid {
            GridOrEmpty2D::Grid(grid) => {
                assert_eq!(grid.shape(), &GridShape::new([2, 2]));

                let res = grid.masked_element_deref_iterator().collect::<Vec<_>>();

                let expected = vec![Some(7), Some(7), Some(7), Some(6)];

                assert_eq!(res, expected);
            }
            GridOrEmpty2D::Empty(_) => panic!("expected GridOrEmpty2D::Grid"),
        }
    }
}
