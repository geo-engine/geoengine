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

impl From<RasterKernelMethod> for InitializedRasterKernelMethod {
    fn from(kernel_method: RasterKernelMethod) -> Self {
        let dimensions = kernel_method.kernel_size();
        match kernel_method {
            RasterKernelMethod::Convolution { matrix } => {
                let matrix = Grid2D::new(dimensions, matrix.into_iter().flatten().collect())
                    .expect("matrix must be rectangular");

                Self::Convolution { matrix }
            }
            RasterKernelMethod::StandardDeviation { .. } => Self::StandardDeviation { dimensions },
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
    KernelTooLarge {
        limit: GridShape2D,
        actual: GridShape2D,
    },
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
            error::KernelTooLarge {
                limit: tiling_specification.tile_size_in_pixels,
                actual: dimensions
            }
        );

        let raster_source = self.sources.raster.initialize(context).await?;

        let initialized_operator = InitializedRasterKernel {
            result_descriptor: raster_source.result_descriptor().clone(),
            raster_source,
            raster_kernel_method: self.params.raster_kernel.into(),
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
                        ConvolutionKernel::new(matrix.clone())
                    ).boxed()
                    .into(),
                    InitializedRasterKernelMethod::StandardDeviation { dimensions } => RasterKernelProcessor::<_,_, StandardDeviationKernel>::new(
                        p,
                        self.tiling_specification,
                        StandardDeviationKernel::new(*dimensions)
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
        primitives::{
            Measurement, RasterQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval,
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
    };

    #[test]
    fn test_initialized_raster_kernel_method() {
        let kernel_method = RasterKernelMethod::Convolution {
            matrix: vec![vec![1.0, 2.0], vec![3.0, 4.0], vec![5.0, 6.0]],
        };
        let initialized_kernel_method = InitializedRasterKernelMethod::from(kernel_method);

        if let InitializedRasterKernelMethod::Convolution { matrix } = initialized_kernel_method {
            assert_eq!(
                matrix,
                Grid2D::new([3, 2].into(), vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]).unwrap()
            );
        } else {
            panic!("Wrong kernel method");
        }
    }

    #[tokio::test]
    async fn test_mean_convolution() -> Result<()> {
        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [2, 2].into(),
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
        .await?;

        let processor = operator.query_processor()?.get_i8().unwrap();

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 2.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 20),
            spatial_resolution: SpatialResolution::zero_point_five(),
        };
        let query_ctx = MockQueryContext::test_default();

        let result_stream = processor.query(query_rect, &query_ctx).await?;

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>()?;

        let mut times: Vec<TimeInterval> = vec![TimeInterval::new_unchecked(0, 10); 8];
        times.append(&mut vec![TimeInterval::new_unchecked(10, 20); 8]);

        let data = vec![
            vec![1, 2, 5, 6],
            vec![2, 3, 6, 7],
            vec![3, 4, 7, 8],
            vec![4, 0, 8, 0],
            vec![5, 6, 0, 0],
            vec![6, 7, 0, 0],
            vec![7, 8, 0, 0],
            vec![8, 0, 0, 0],
            vec![8, 7, 4, 3],
            vec![7, 6, 3, 2],
            vec![6, 5, 2, 1],
            vec![5, 0, 1, 0],
            vec![4, 3, 0, 0],
            vec![3, 2, 0, 0],
            vec![2, 1, 0, 0],
            vec![1, 0, 0, 0],
        ];

        let valid = vec![
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true, false, true, false],
            vec![true, true, false, false],
            vec![true, true, false, false],
            vec![true, true, false, false],
            vec![true, false, false, false],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true, false, true, false],
            vec![true, true, false, false],
            vec![true, true, false, false],
            vec![true, true, false, false],
            vec![true, false, false, false],
        ];

        for (i, tile) in result.into_iter().enumerate() {
            let tile = tile.into_materialized_tile();
            assert_eq!(tile.time, times[i]);
            assert_eq!(tile.grid_array.inner_grid.data, data[i]);
            assert_eq!(tile.grid_array.validity_mask.data, valid[i]);
        }

        Ok(())
    }

    fn make_raster() -> Box<dyn RasterOperator> {
        // test raster:
        // [0, 10)
        // || 1 | 2 || 3 | 4 ||
        // || 5 | 6 || 7 | 8 ||
        //
        // [10, 20)
        // || 8 | 7 || 6 | 5 ||
        // || 4 | 3 || 2 | 1 ||
        let raster_tiles = vec![
            RasterTile2D::<i8>::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [2, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([2, 2].into(), vec![1, 2, 5, 6]).unwrap()),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [2, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([2, 2].into(), vec![3, 4, 7, 8]).unwrap()),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [2, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([2, 2].into(), vec![8, 7, 4, 3]).unwrap()),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [2, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([2, 2].into(), vec![6, 5, 2, 1]).unwrap()),
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
}
