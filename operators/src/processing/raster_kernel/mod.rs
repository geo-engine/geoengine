mod kernel_function;
mod tile_sub_query;

use self::kernel_function::{ConvolutionKernel, KernelFunction, StandardDeviationKernel};
use self::tile_sub_query::RasterKernelTileNeighborhood;
use crate::adapters::RasterSubQueryAdapter;
use crate::engine::{
    ExecutionContext, InitializedRasterOperator, Operator, QueryContext, QueryProcessor,
    RasterOperator, RasterQueryProcessor, RasterResultDescriptor, SingleRasterSource,
    TypedRasterQueryProcessor,
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

/// A raster kernel operator applies a kernel to a raster.
/// For each output pixel, the kernel is applied to its neighborhood.
pub type RasterKernel = Operator<RasterKernelParams, SingleRasterSource>;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RasterKernelParams {
    pub raster_kernel: RasterKernelMethod,
    // TODO: strategies for missing data at the edges
    // TODO: option to fix resolution
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum RasterKernelMethod {
    Convolution { matrix: Vec<Vec<f64>> },
    StandardDeviation { dimensions: GridShape2D },
}

#[derive(Debug, Clone)]
enum InitializedRasterKernelMethod {
    Convolution { matrix: Grid2D<f64> },
    StandardDeviation { dimensions: GridShape2D },
}

impl TryFrom<RasterKernelMethod> for InitializedRasterKernelMethod {
    type Error = RasterKernelError;

    fn try_from(kernel_method: RasterKernelMethod) -> Result<Self, Self::Error> {
        let dimensions = kernel_method.kernel_size();
        match kernel_method {
            RasterKernelMethod::Convolution { matrix } => {
                let matrix = Grid2D::new(dimensions, matrix.into_iter().flatten().collect())
                    .map_err(|_| RasterKernelError::MatrixNotRectangular)?;

                ensure!(
                    dimensions.axis_size_x().is_odd() && dimensions.axis_size_y().is_odd(),
                    error::DimensionsNotOdd { actual: dimensions }
                );

                Ok(Self::Convolution { matrix })
            }
            RasterKernelMethod::StandardDeviation { .. } => {
                ensure!(
                    dimensions.axis_size_x().is_odd() && dimensions.axis_size_y().is_odd(),
                    error::DimensionsNotOdd { actual: dimensions }
                );

                Ok(Self::StandardDeviation { dimensions })
            }
        }
    }
}

impl RasterKernelMethod {
    fn kernel_size(&self) -> GridShape2D {
        match self {
            Self::Convolution { matrix } => {
                let x_size = matrix.len();
                let y_size = matrix.get(0).map_or(0, Vec::len);
                GridShape2D::new([x_size, y_size])
            }
            Self::StandardDeviation { dimensions } => *dimensions,
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(error))]
pub enum RasterKernelError {
    #[snafu(display(
        "The maximum kernel size is {}x{}, but the kernel has size {}x{}",
        limit.axis_size_x(), limit.axis_size_y(), actual.axis_size_x(), actual.axis_size_y()
    ))]
    TooLarge {
        limit: GridShape2D,
        actual: GridShape2D,
    },

    #[snafu(display(
        "The kernel must have an odd number of rows and columns, but the kernel has size {}x{}",
        actual.axis_size_x(), actual.axis_size_y()
    ))]
    DimensionsNotOdd { actual: GridShape2D },

    #[snafu(display("The kernel matrix must be rectangular"))]
    MatrixNotRectangular,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for RasterKernel {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let tiling_specification = context.tiling_specification();

        // dimensions must not be larger as the neighboring tiles
        let dimensions = self.params.raster_kernel.kernel_size();
        ensure!(
            dimensions.axis_size_x() <= tiling_specification.tile_size_in_pixels.axis_size_x()
                && dimensions.axis_size_y()
                    <= tiling_specification.tile_size_in_pixels.axis_size_y(),
            error::TooLarge {
                limit: tiling_specification.tile_size_in_pixels,
                actual: dimensions
            }
        );

        let raster_source = self.sources.raster.initialize(context).await?;

        let initialized_operator = InitializedRasterKernel {
            result_descriptor: raster_source.result_descriptor().clone(),
            raster_source,
            raster_kernel_method: self.params.raster_kernel.try_into()?,
            tiling_specification,
        };

        Ok(initialized_operator.boxed())
    }
}

pub struct InitializedRasterKernel {
    result_descriptor: RasterResultDescriptor,
    raster_source: Box<dyn InitializedRasterOperator>,
    raster_kernel_method: InitializedRasterKernelMethod,
    tiling_specification: TilingSpecification,
}

impl InitializedRasterOperator for InitializedRasterKernel {
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source_processor = self.raster_source.query_processor()?;

        let res = call_on_generic_raster_processor!(
            source_processor, p => match &self.raster_kernel_method  {
                InitializedRasterKernelMethod::Convolution { matrix } => RasterKernelProcessor::<_,_, ConvolutionKernel>::new(
                        p,
                        self.tiling_specification,
                        ConvolutionKernel::new(matrix.clone())?
                    ).boxed()
                    .into(),
                    InitializedRasterKernelMethod::StandardDeviation { dimensions } => RasterKernelProcessor::<_,_, StandardDeviationKernel>::new(
                        p,
                        self.tiling_specification,
                        StandardDeviationKernel::new(*dimensions)?
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

pub struct RasterKernelProcessor<Q, P, F> {
    source: Q,
    tiling_specification: TilingSpecification,
    kernel_function: F,
    _phantom_types: PhantomData<P>,
}

impl<Q, P, F> RasterKernelProcessor<Q, P, F>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    pub fn new(source: Q, tiling_specification: TilingSpecification, kernel_function: F) -> Self {
        Self {
            source,
            tiling_specification,
            kernel_function,
            _phantom_types: PhantomData,
        }
    }
}

#[async_trait]
impl<Q, P, F> QueryProcessor for RasterKernelProcessor<Q, P, F>
where
    Q: QueryProcessor<Output = RasterTile2D<P>, SpatialBounds = SpatialPartition2D>,
    P: Pixel,
    f64: AsPrimitive<P>,
    F: KernelFunction<P> + 'static,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let sub_query = RasterKernelTileNeighborhood::<P, F>::new(
            self.kernel_function.clone(),
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
    use super::*;
    use futures::StreamExt;
    use geoengine_datatypes::{
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

    use crate::{
        engine::{MockExecutionContext, MockQueryContext, RasterOperator, RasterResultDescriptor},
        mock::{MockRasterSource, MockRasterSourceParams},
        source::{GdalSource, GdalSourceParameters},
        util::{gdal::add_ndvi_dataset, raster_stream_to_png::raster_stream_to_png_bytes},
    };

    #[test]
    fn test_initialized_raster_kernel_method() {
        let kernel_method = RasterKernelMethod::Convolution {
            matrix: vec![vec![1.0], vec![3.0], vec![5.0]],
        };
        let initialized_kernel_method =
            InitializedRasterKernelMethod::try_from(kernel_method).unwrap();

        if let InitializedRasterKernelMethod::Convolution { matrix } = initialized_kernel_method {
            assert_eq!(
                matrix,
                Grid2D::new([3, 1].into(), vec![1.0, 3.0, 5.0]).unwrap()
            );
        } else {
            panic!("Wrong kernel method");
        }
    }

    #[tokio::test]
    async fn test_mean_convolution() {
        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 3].into(),
        ));

        let raster = make_raster();

        let operator = RasterKernel {
            params: RasterKernelParams {
                raster_kernel: RasterKernelMethod::Convolution {
                    matrix: vec![vec![1. / 9.; 3]; 3],
                },
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

        let operator = RasterKernel {
            params: RasterKernelParams {
                raster_kernel: RasterKernelMethod::Convolution {
                    matrix: vec![
                        vec![1. / 16., 1. / 8., 1. / 16.],
                        vec![1. / 8., 1. / 4., 1. / 8.],
                        vec![1. / 16., 1. / 8., 1. / 16.],
                    ],
                },
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
            include_bytes!("../../../../test_data/wms/gaussian_blur.png")
        );

        // Use for getting the image to compare against
        // save_test_bytes(&bytes, "gaussian_blur.png");
    }

    #[tokio::test]
    async fn test_ndvi_sobel_filter() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);

        let operator = RasterKernel {
            params: RasterKernelParams {
                raster_kernel: RasterKernelMethod::Convolution {
                    matrix: vec![vec![1., 0., -1.], vec![2., 0., -2.], vec![1., 0., -1.]],
                },
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
            include_bytes!("../../../../test_data/wms/sobel_filter.png")
        );

        // Use for getting the image to compare against
        // save_test_bytes(&bytes, "sobel_filter.png");
    }
}
