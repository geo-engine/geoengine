mod aggregate;
mod tile_sub_query;

use self::aggregate::{AggregateFunction, Neighborhood, StandardDeviation, Sum};
use self::tile_sub_query::NeighborhoodAggregateTileNeighborhood;
use crate::adapters::RasterSubQueryAdapter;
use crate::engine::{
    CreateSpan, ExecutionContext, InitializedRasterOperator, Operator, OperatorName, QueryContext,
    QueryProcessor, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
    SingleRasterSource, TypedRasterQueryProcessor,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use geoengine_datatypes::primitives::{RasterQueryRectangle, SpatialPartition2D};
use geoengine_datatypes::raster::{
    Grid2D, GridShape2D, GridSize, Pixel, RasterTile2D, TilingSpecification,
};
use num::Integer;
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use snafu::{ensure, Snafu};
use std::marker::PhantomData;
use tracing::{span, Level};

/// A neighborhood aggregate operator applies an aggregate function to each raster pixel and its surrounding.
/// For each output pixel, the aggregate function is applied to an input pixel plus its neighborhood.
pub type NeighborhoodAggregate = Operator<NeighborhoodAggregateParams, SingleRasterSource>;

impl OperatorName for NeighborhoodAggregate {
    const TYPE_NAME: &'static str = "NeighborhoodAggregate";
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
/// Parameters for the `NeighborhoodAggregate` operator.
///
/// TODO: Possible extensions:
///  - strategies for missing data at the edges
pub struct NeighborhoodAggregateParams {
    /// Defines the neighborhood for each pixel.
    /// Each dimension must be odd.
    pub neighborhood: NeighborhoodParams,
    /// Defines the aggregate function to apply to the neighborhood.
    pub aggregate_function: AggregateFunctionParams,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum AggregateFunctionParams {
    Sum,
    StandardDeviation,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum NeighborhoodParams {
    Rectangle { dimensions: [usize; 2] },
    WeightsMatrix { weights: Vec<Vec<f64>> },
}

impl NeighborhoodParams {
    fn dimensions(&self) -> GridShape2D {
        match self {
            Self::WeightsMatrix { weights } => {
                let x_size = weights.len();
                let y_size = weights.get(0).map_or(0, Vec::len);
                GridShape2D::new([x_size, y_size])
            }
            Self::Rectangle { dimensions } => GridShape2D::new(*dimensions),
        }
    }
}

impl TryFrom<NeighborhoodParams> for Neighborhood {
    type Error = NeighborhoodAggregateError;

    fn try_from(neighborhood: NeighborhoodParams) -> Result<Self, Self::Error> {
        let dimensions = neighborhood.dimensions();

        ensure!(dimensions.number_of_elements() > 0, error::DimensionsZero);
        ensure!(
            dimensions.axis_size_x().is_odd() && dimensions.axis_size_y().is_odd(),
            error::DimensionsNotOdd { actual: dimensions }
        );

        let matrix = match neighborhood {
            NeighborhoodParams::WeightsMatrix { weights } => {
                Grid2D::new(dimensions, weights.into_iter().flatten().collect())
                    .map_err(|_| NeighborhoodAggregateError::IrregularDimensions)?
            }
            NeighborhoodParams::Rectangle { .. } => Grid2D::new_filled(dimensions, 1.),
        };

        Self::new(matrix)
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(error))]
pub enum NeighborhoodAggregateError {
    #[snafu(display(
        "The maximum kernel size is {}x{}, but the kernel has size {}x{}",
        limit.axis_size_x(), limit.axis_size_y(), actual.axis_size_x(), actual.axis_size_y()
    ))]
    NeighborhoodTooLarge {
        limit: GridShape2D,
        actual: GridShape2D,
    },

    #[snafu(display(
        "The kernel must have an odd number of rows and columns, but the kernel has size {}x{}",
        actual.axis_size_x(), actual.axis_size_y()
    ))]
    DimensionsNotOdd { actual: GridShape2D },

    #[snafu(display("The input matrix has irregular dimensions"))]
    IrregularDimensions,

    #[snafu(display("The kernel matrix dimensions must not be 0"))]
    DimensionsZero,

    #[snafu(display("The kernel matrix must be rectangular"))]
    MatrixNotRectangular,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for NeighborhoodAggregate {
    async fn _initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let tiling_specification = context.tiling_specification();

        // dimensions must not be larger as the neighboring tiles
        let dimensions = self.params.neighborhood.dimensions();
        ensure!(
            dimensions.axis_size_x() <= tiling_specification.tile_size_in_pixels.axis_size_x()
                && dimensions.axis_size_y()
                    <= tiling_specification.tile_size_in_pixels.axis_size_y(),
            error::NeighborhoodTooLarge {
                limit: tiling_specification.tile_size_in_pixels,
                actual: dimensions
            }
        );

        let raster_source = self.sources.raster.initialize(context).await?;

        let initialized_operator = InitializedNeighborhoodAggregate {
            result_descriptor: raster_source.result_descriptor().clone(),
            raster_source,
            neighborhood: self.params.neighborhood.try_into()?,
            aggregate_function: self.params.aggregate_function,
            tiling_specification,
        };

        Ok(initialized_operator.boxed())
    }

    fn span(&self) -> CreateSpan {
        || span!(Level::TRACE, NeighborhoodAggregate::TYPE_NAME)
    }
}

pub struct InitializedNeighborhoodAggregate {
    result_descriptor: RasterResultDescriptor,
    raster_source: Box<dyn InitializedRasterOperator>,
    neighborhood: Neighborhood,
    aggregate_function: AggregateFunctionParams,
    tiling_specification: TilingSpecification,
}

impl InitializedRasterOperator for InitializedNeighborhoodAggregate {
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source_processor = self.raster_source.query_processor()?;

        let res = call_on_generic_raster_processor!(
            source_processor, p => match &self.aggregate_function  {
                AggregateFunctionParams::Sum => NeighborhoodAggregateProcessor::<_,_, Sum>::new(
                        p,
                        self.tiling_specification,
                        self.neighborhood.clone(),
                    ).boxed()
                    .into(),
                    AggregateFunctionParams::StandardDeviation => NeighborhoodAggregateProcessor::<_,_, StandardDeviation>::new(
                        p,
                        self.tiling_specification,
                        self.neighborhood.clone(),
                    ).boxed()
                    .into(),
            }
        );

        Ok(res)
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

pub struct NeighborhoodAggregateProcessor<Q, P, A> {
    source: Q,
    tiling_specification: TilingSpecification,
    neighborhood: Neighborhood,
    _phantom_types: PhantomData<(P, A)>,
}

impl<Q, P, A> NeighborhoodAggregateProcessor<Q, P, A>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    pub fn new(
        source: Q,
        tiling_specification: TilingSpecification,
        neighborhood: Neighborhood,
    ) -> Self {
        Self {
            source,
            tiling_specification,
            neighborhood,
            _phantom_types: PhantomData,
        }
    }
}

#[async_trait]
impl<Q, P, A> QueryProcessor for NeighborhoodAggregateProcessor<Q, P, A>
where
    Q: QueryProcessor<Output = RasterTile2D<P>, SpatialBounds = SpatialPartition2D>,
    P: Pixel,
    f64: AsPrimitive<P>,
    A: AggregateFunction + 'static,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let sub_query = NeighborhoodAggregateTileNeighborhood::<P, A>::new(
            self.neighborhood.clone(),
            self.tiling_specification,
        );

        Ok(RasterSubQueryAdapter::<'a, P, _, _>::new(
            &self.source,
            query,
            self.tiling_specification,
            ctx,
            sub_query,
        )
        .filter_and_fill())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    use crate::{
        engine::{MockExecutionContext, MockQueryContext, RasterOperator, RasterResultDescriptor},
        mock::{MockRasterSource, MockRasterSourceParams},
        source::{GdalSource, GdalSourceParameters},
        util::{gdal::add_ndvi_dataset, raster_stream_to_png::raster_stream_to_png_bytes},
    };
    use futures::StreamExt;
    use geoengine_datatypes::{
        dataset::DatasetId,
        operations::image::{Colorizer, RgbaColor},
        primitives::{
            DateTime, Measurement, RasterQueryRectangle, SpatialPartition2D, SpatialResolution,
            TimeInstance, TimeInterval,
        },
        raster::{
            Grid2D, GridOrEmpty, RasterDataType, RasterTile2D, TileInformation, TilingSpecification,
        },
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    #[test]
    fn test_serialization_matrix() {
        let operator = NeighborhoodAggregate {
            params: NeighborhoodAggregateParams {
                neighborhood: NeighborhoodParams::WeightsMatrix {
                    weights: vec![vec![1., 2., 3.], vec![4., 5., 6.], vec![7., 8., 9.]],
                },
                aggregate_function: AggregateFunctionParams::Sum,
            },
            sources: SingleRasterSource {
                raster: GdalSource {
                    params: GdalSourceParameters {
                        data: DatasetId::from_str("8d01593c-75c0-4ffa-8152-eabfe4430817")
                            .unwrap()
                            .into(),
                    },
                }
                .boxed(),
            },
        }
        .boxed();

        let serialized = serde_json::to_value(&operator).unwrap();

        assert_eq!(
            serde_json::json!({
                "type": "NeighborhoodAggregate",
                "params": {
                    "neighborhood": {
                        "type": "weightsMatrix",
                        "weights": [
                            [1.0, 2.0, 3.0],
                            [4.0, 5.0, 6.0],
                            [7.0, 8.0, 9.0]
                        ]
                    },
                    "aggregateFunction": "sum"
                },
                "sources": {
                    "raster": {
                        "type": "GdalSource",
                        "params": {
                            "data": {
                                "type": "internal",
                                "datasetId": "8d01593c-75c0-4ffa-8152-eabfe4430817"
                            }
                        }
                    }
                }
            }),
            serialized
        );

        serde_json::from_value::<NeighborhoodAggregate>(serialized).unwrap();
    }

    #[test]
    fn test_serialization_stddev() {
        let operator = NeighborhoodAggregate {
            params: NeighborhoodAggregateParams {
                neighborhood: NeighborhoodParams::Rectangle { dimensions: [3, 3] },
                aggregate_function: AggregateFunctionParams::StandardDeviation,
            },
            sources: SingleRasterSource {
                raster: GdalSource {
                    params: GdalSourceParameters {
                        data: DatasetId::from_str("8d01593c-75c0-4ffa-8152-eabfe4430817")
                            .unwrap()
                            .into(),
                    },
                }
                .boxed(),
            },
        }
        .boxed();

        let serialized = serde_json::to_value(&operator).unwrap();

        assert_eq!(
            serde_json::json!({
                "type": "NeighborhoodAggregate",
                "params": {
                    "neighborhood": {
                        "type": "rectangle",
                        "dimensions": [3, 3]
                    },
                    "aggregateFunction": "standardDeviation"
                },
                "sources": {
                    "raster": {
                        "type": "GdalSource",
                        "params": {
                            "data": {
                                "type": "internal",
                                "datasetId": "8d01593c-75c0-4ffa-8152-eabfe4430817"
                            }
                        }
                    }
                }
            }),
            serialized
        );

        serde_json::from_value::<NeighborhoodAggregate>(serialized).unwrap();
    }

    #[test]
    fn test_initialized_raster_kernel_method() {
        let neighborhood: Neighborhood = NeighborhoodParams::WeightsMatrix {
            weights: vec![vec![1.0], vec![3.0], vec![5.0]],
        }
        .try_into()
        .unwrap();

        assert_eq!(
            neighborhood.matrix(),
            &Grid2D::new([3, 1].into(), vec![1.0, 3.0, 5.0]).unwrap()
        );
    }

    #[tokio::test]
    async fn test_mean_convolution() {
        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 3].into(),
        ));

        let raster = make_raster();

        let operator = NeighborhoodAggregate {
            params: NeighborhoodAggregateParams {
                neighborhood: NeighborhoodParams::WeightsMatrix {
                    weights: vec![vec![1. / 9.; 3]; 3],
                },
                aggregate_function: AggregateFunctionParams::Sum,
            },
            sources: SingleRasterSource { raster },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await
        .unwrap();

        let processor = operator.query_processor().unwrap().get_i8().unwrap();

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((0., 3.).into(), (6., 0.).into()).unwrap(),
            time_interval: TimeInterval::new_unchecked(0, 20),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

        let result_stream = processor.query(query_rect, &query_ctx).await.unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let mut times: Vec<TimeInterval> = vec![TimeInterval::new_unchecked(0, 10); 2];
        times.append(&mut vec![TimeInterval::new_unchecked(10, 20); 2]);

        let data = vec![
            vec![0, 0, 0, 0, 8, 9, 0, 0, 0],
            vec![0, 0, 0, 10, 11, 0, 0, 0, 0],
            vec![0, 0, 0, 0, 10, 10, 0, 0, 0],
            vec![0, 0, 0, 9, 7, 0, 0, 0, 0],
        ];

        let valid = vec![
            vec![false, false, false, false, true, true, false, false, false],
            vec![false, false, false, true, true, false, false, false, false],
            vec![false, false, false, false, true, true, false, false, false],
            vec![false, false, false, true, true, false, false, false, false],
        ];

        for (i, tile) in result.into_iter().enumerate() {
            let tile = tile.into_materialized_tile();
            assert_eq!(tile.time, times[i]);
            assert_eq!(tile.grid_array.inner_grid.data, data[i]);
            assert_eq!(tile.grid_array.validity_mask.data, valid[i]);
        }
    }

    #[tokio::test]
    async fn check_make_raster() {
        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 3].into(),
        ));

        let raster = make_raster();

        let operator = raster.initialize(&exe_ctx).await.unwrap();

        let processor = operator.query_processor().unwrap().get_i8().unwrap();

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((0., 3.).into(), (6., 0.).into()).unwrap(),
            time_interval: TimeInterval::new_unchecked(0, 20),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

        let result_stream = processor.query(query_rect, &query_ctx).await.unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(result.len(), 4);

        let raster_tiles = vec![
            RasterTile2D::<i8>::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 3].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(
                    Grid2D::new([3, 3].into(), vec![1, 2, 3, 7, 8, 9, 13, 14, 15]).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 3].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(
                    Grid2D::new([3, 3].into(), vec![4, 5, 6, 10, 11, 12, 16, 17, 18]).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 3].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(
                    Grid2D::new([3, 3].into(), vec![18, 17, 16, 12, 11, 10, 6, 5, 4]).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 3].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(
                    Grid2D::new([3, 3].into(), vec![15, 14, 13, 9, 8, 7, 3, 2, 1]).unwrap(),
                ),
            ),
        ];

        assert_eq!(result, raster_tiles);
    }

    fn make_raster() -> Box<dyn RasterOperator> {
        // test raster:
        // [0, 10)
        // ||  1 |   2 |  3 ||  4 |  5 |  6 ||
        // ||  7 |   8 |  9 || 10 | 11 | 12 ||
        // || 13 |  14 | 15 || 16 | 17 | 18 ||
        //
        // [10, 20)
        // || 18 |  17 | 16 || 15 | 14 | 13 ||
        // || 12 |  11 | 10 ||  9 |  8 |  7 ||
        // ||  6 |   5 |  4 ||  3 |  2 |  1 ||
        //
        let raster_tiles = vec![
            RasterTile2D::<i8>::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 3].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(
                    Grid2D::new([3, 3].into(), vec![1, 2, 3, 7, 8, 9, 13, 14, 15]).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 3].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(
                    Grid2D::new([3, 3].into(), vec![4, 5, 6, 10, 11, 12, 16, 17, 18]).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 3].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(
                    Grid2D::new([3, 3].into(), vec![18, 17, 16, 12, 11, 10, 6, 5, 4]).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 3].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(
                    Grid2D::new([3, 3].into(), vec![15, 14, 13, 9, 8, 7, 3, 2, 1]).unwrap(),
                ),
            ),
        ];

        MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        }
        .boxed()
    }

    #[tokio::test]
    async fn test_ndvi_gaussian_filter() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);

        let operator = NeighborhoodAggregate {
            params: NeighborhoodAggregateParams {
                neighborhood: NeighborhoodParams::WeightsMatrix {
                    weights: vec![
                        vec![1. / 16., 1. / 8., 1. / 16.],
                        vec![1. / 8., 1. / 4., 1. / 8.],
                        vec![1. / 16., 1. / 8., 1. / 16.],
                    ],
                },
                aggregate_function: AggregateFunctionParams::Sum,
            },
            sources: SingleRasterSource {
                raster: GdalSource {
                    params: GdalSourceParameters { data: ndvi_id },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await
        .unwrap();

        let processor = operator.query_processor().unwrap().get_u8().unwrap();

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into())
                .unwrap(),
            time_interval: TimeInstance::from(DateTime::new_utc(2014, 1, 1, 0, 0, 0)).into(),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::white()).try_into().unwrap(),
                (255.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::pink(),
        )
        .unwrap();

        let bytes = raster_stream_to_png_bytes(
            processor,
            query_rect,
            query_ctx,
            360,
            180,
            None,
            Some(colorizer),
        )
        .await
        .unwrap();

        assert_eq!(
            bytes,
            include_bytes!("../../../../test_data/wms/gaussian_blur.png")
        );

        // Use for getting the image to compare against
        // save_test_bytes(&bytes, "gaussian_blur.png");
    }

    #[tokio::test]
    async fn test_ndvi_partial_derivative() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);

        let operator = NeighborhoodAggregate {
            params: NeighborhoodAggregateParams {
                neighborhood: NeighborhoodParams::WeightsMatrix {
                    weights: vec![vec![1., 0., -1.], vec![2., 0., -2.], vec![1., 0., -1.]],
                },
                aggregate_function: AggregateFunctionParams::Sum,
            },
            sources: SingleRasterSource {
                raster: GdalSource {
                    params: GdalSourceParameters { data: ndvi_id },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await
        .unwrap();

        let processor = operator.query_processor().unwrap().get_u8().unwrap();

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into())
                .unwrap(),
            time_interval: TimeInstance::from(DateTime::new_utc(2014, 1, 1, 0, 0, 0)).into(),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

        // let result_stream = processor.query(query_rect, &query_ctx).await.unwrap();

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::white()).try_into().unwrap(),
                (255.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::pink(),
        )
        .unwrap();

        let bytes = raster_stream_to_png_bytes(
            processor,
            query_rect,
            query_ctx,
            360,
            180,
            None,
            Some(colorizer),
        )
        .await
        .unwrap();

        assert_eq!(
            bytes,
            include_bytes!("../../../../test_data/wms/partial_derivative.png")
        );

        // Use for getting the image to compare against
        // save_test_bytes(&bytes, "sobel_filter.png");
    }
}
