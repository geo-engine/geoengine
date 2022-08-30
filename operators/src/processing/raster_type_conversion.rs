use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt, TryFutureExt, TryStreamExt};
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, SpatialPartition2D},
    raster::{ConvertDataType, Pixel, RasterDataType, RasterTile2D},
};
use serde::{Deserialize, Serialize};

use crate::engine::{
    ExecutionContext, InitializedRasterOperator, Operator, QueryContext, QueryProcessor,
    RasterOperator, RasterQueryProcessor, RasterResultDescriptor, SingleRasterSource,
    TypedRasterQueryProcessor,
};
use crate::util::Result;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RasterTypeConversionParams {
    output_data_type: RasterDataType,
}

pub type RasterTypeConversion = Operator<RasterTypeConversionParams, SingleRasterSource>;

pub struct InitializedRasterTypeConversionOperator {
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for RasterTypeConversion {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let input = self.sources.raster.initialize(context).await?;
        let in_desc = input.result_descriptor();

        let out_data_type = self.params.output_data_type;

        let out_desc = RasterResultDescriptor {
            spatial_reference: in_desc.spatial_reference,
            data_type: out_data_type,
            measurement: in_desc.measurement.clone(),
            bbox: in_desc.bbox,
            time: in_desc.time,
        };

        let initialized_operator = InitializedRasterTypeConversionOperator {
            result_descriptor: out_desc,
            source: input,
        };

        Ok(initialized_operator.boxed())
    }
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
    type SpatialBounds = SpatialPartition2D;

    async fn query<'b>(
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
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{Measurement, SpatialPartition2D, SpatialResolution, TimeInterval},
        raster::{
            Grid2D, GridOrEmpty2D, MaskedGrid2D, RasterDataType, TileInformation,
            TilingSpecification,
        },
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use crate::{
        engine::{ChunkByteSize, MockExecutionContext},
        mock::{MockRasterSource, MockRasterSourceParams},
    };

    use super::*;

    #[tokio::test]
    #[allow(clippy::float_cmp)]
    async fn test_type_conversion() {
        let grid_shape = [2, 2].into();

        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels: grid_shape,
        };

        let raster: MaskedGrid2D<u8> = Grid2D::new(grid_shape, vec![7_u8, 7, 7, 6]).unwrap().into();

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);
        let query_ctx = ctx.mock_query_context(ChunkByteSize::test_default());

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: grid_shape,
            },
            raster.into(),
        );

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    bbox: None,
                    time: None,
                },
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

        let initialized_op = op.initialize(&ctx).await.unwrap();

        let result_descriptor = initialized_op.result_descriptor();

        assert_eq!(result_descriptor.data_type, RasterDataType::F32);
        assert_eq!(result_descriptor.measurement, Measurement::Unitless);

        let query_processor = initialized_op.query_processor().unwrap();

        let query = geoengine_datatypes::primitives::RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((0., 0.).into(), (2., -2.).into()).unwrap(),
            spatial_resolution: SpatialResolution::one(),
            time_interval: TimeInterval::default(),
        };

        let typed_processor = match query_processor {
            TypedRasterQueryProcessor::F32(rqp) => rqp,
            _ => panic!("expected TypedRasterQueryProcessor::U8"),
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
