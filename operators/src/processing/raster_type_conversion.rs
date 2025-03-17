use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources, Operator,
    OperatorName, QueryContext, QueryProcessor, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, SingleRasterSource, TypedRasterQueryProcessor, WorkflowOperatorPath,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::{StreamExt, TryFutureExt, TryStreamExt, stream::BoxStream};
use geoengine_datatypes::{
    primitives::{BandSelection, RasterQueryRectangle, RasterSpatialQueryRectangle},
    raster::{ConvertDataType, Pixel, RasterDataType, RasterTile2D},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RasterTypeConversionParams {
    pub output_data_type: RasterDataType,
}

/// This operator converts the type of raster data into another type. This may cause precision loss as e.g. `3.1_f32` converted to `u8` will result in `3_u8`.
/// In case the value range is to small the operator will clip the values at the bounds of the data range. An example is this: The `u32` value `10000_u32` is converted to `u8`, which has a value range of 0..256. The result is `255_u8` since this is the highest value a `u8` can represent.
pub type RasterTypeConversion = Operator<RasterTypeConversionParams, SingleRasterSource>;

impl OperatorName for RasterTypeConversion {
    const TYPE_NAME: &'static str = "RasterTypeConversion";
}

pub struct InitializedRasterTypeConversionOperator {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for RasterTypeConversion {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);

        let initialized_sources = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?;
        let in_desc = initialized_sources.raster.result_descriptor();

        let out_data_type = self.params.output_data_type;

        let out_desc = RasterResultDescriptor {
            spatial_reference: in_desc.spatial_reference,
            data_type: out_data_type,
            time: in_desc.time,
            spatial_grid: in_desc.spatial_grid,
            bands: in_desc.bands.clone(),
        };

        let initialized_operator = InitializedRasterTypeConversionOperator {
            name,
            path,
            result_descriptor: out_desc,
            source: initialized_sources.raster,
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(RasterTypeConversion);
}

impl InitializedRasterOperator for InitializedRasterTypeConversionOperator {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source = self.source.query_processor()?;
        let out_data_type = self.result_descriptor.data_type;

        let res_op = call_on_generic_raster_processor!(source, source_proc => {
            call_generic_raster_processor!(out_data_type,
                RasterTypeConversionQueryProcessor::create_boxed(source_proc)
            )
        });

        Ok(res_op)
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        RasterTypeConversion::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }
}

pub struct RasterTypeConversionQueryProcessor<
    Q: RasterQueryProcessor<RasterType = PIn>,
    PIn: Pixel,
    POut: Pixel,
> {
    query_processor: Q,
    _p_out: std::marker::PhantomData<POut>,
}

impl<Q, PIn, POut> RasterTypeConversionQueryProcessor<Q, PIn, POut>
where
    Q: 'static + RasterQueryProcessor<RasterType = PIn>,
    RasterTile2D<PIn>: ConvertDataType<RasterTile2D<POut>>,
    PIn: Pixel,
    POut: Pixel,
{
    pub fn new(query_processor: Q) -> Self {
        Self {
            query_processor,
            _p_out: std::marker::PhantomData,
        }
    }

    pub fn create_boxed(source: Q) -> Box<dyn RasterQueryProcessor<RasterType = POut>> {
        RasterTypeConversionQueryProcessor::new(source).boxed()
    }
}

#[async_trait]
impl<Q, PIn: Pixel, POut: Pixel> QueryProcessor for RasterTypeConversionQueryProcessor<Q, PIn, POut>
where
    RasterTile2D<PIn>: ConvertDataType<RasterTile2D<POut>>,
    Q: RasterQueryProcessor<RasterType = PIn>,
{
    type Output = RasterTile2D<POut>;
    type SpatialQuery = RasterSpatialQueryRectangle;
    type Selection = BandSelection;
    type ResultDescription = RasterResultDescriptor;

    async fn _query<'b>(
        &'b self,
        query: RasterQueryRectangle,
        ctx: &'b dyn QueryContext,
    ) -> Result<BoxStream<'b, Result<Self::Output>>> {
        let stream = self.query_processor.raster_query(query, ctx).await?;
        let converted_stream = stream.and_then(move |tile| {
            crate::util::spawn_blocking(|| tile.convert_data_type()).map_err(Into::into)
        });

        Ok(converted_stream.boxed())
    }

    fn result_descriptor(&self) -> &Self::ResultDescription {
        self.query_processor.raster_result_descriptor()
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{CacheHint, Coordinate2D, Measurement, TimeInterval},
        raster::{
            BoundedGrid, GeoTransform, Grid2D, GridBoundingBox2D, GridOrEmpty2D, GridShape2D,
            MaskedGrid2D, RasterDataType, TileInformation, TilingSpecification,
        },
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use crate::{
        engine::{
            ChunkByteSize, MockExecutionContext, RasterBandDescriptors, SpatialGridDescriptor,
        },
        mock::{MockRasterSource, MockRasterSourceParams},
    };

    use super::*;

    #[tokio::test]
    #[allow(clippy::float_cmp)]
    async fn test_type_conversion() {
        let tile_size_in_pixels = GridShape2D::new_2d(2, 2);
        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: None,
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
                tile_size_in_pixels.bounding_box(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };
        let tiling_specification = TilingSpecification::new(tile_size_in_pixels);

        let raster: MaskedGrid2D<u8> = Grid2D::new(tile_size_in_pixels, vec![7_u8, 7, 7, 6])
            .unwrap()
            .into();

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);
        let query_ctx = ctx.mock_query_context(ChunkByteSize::test_default());

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels,
            },
            0,
            raster.into(),
            CacheHint::default(),
        );

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor,
            },
        }
        .boxed();

        let op = RasterTypeConversion {
            params: RasterTypeConversionParams {
                output_data_type: RasterDataType::F32,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let initialized_op = op
            .initialize(WorkflowOperatorPath::initialize_root(), &ctx)
            .await
            .unwrap();

        let result_descriptor = initialized_op.result_descriptor();

        assert_eq!(result_descriptor.data_type, RasterDataType::F32);
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

        let TypedRasterQueryProcessor::F32(typed_processor) = query_processor else {
            panic!("expected TypedRasterQueryProcessor::F32");
        };

        let stream = typed_processor
            .raster_query(query, &query_ctx)
            .await
            .unwrap();

        let results = stream.collect::<Vec<Result<RasterTile2D<f32>>>>().await;

        let result_tile = results.as_slice()[0].as_ref().unwrap();

        let result_grid = result_tile.grid_array.clone();

        match result_grid {
            GridOrEmpty2D::Grid(masked_grid) => {
                assert_eq!(masked_grid.inner_grid.shape, [2, 2].into());
                assert_eq!(masked_grid.inner_grid.data, &[7., 7., 7., 6.]);
            }
            GridOrEmpty2D::Empty(_) => panic!("expected GridOrEmpty2D::Grid"),
        }
    }
}
