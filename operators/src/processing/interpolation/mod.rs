use std::marker::PhantomData;
use std::sync::Arc;

use crate::adapters::{
    FoldTileAccu, FoldTileAccuMut, RasterSubQueryAdapter, SubQueryTileAggregator,
};
use crate::engine::{
    ExecutionContext, InitializedRasterOperator, Operator, QueryContext, QueryProcessor,
    RasterOperator, RasterQueryProcessor, RasterResultDescriptor, SingleRasterSource,
    TypedRasterQueryProcessor,
};
use crate::error;
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{Future, FutureExt, TryFuture, TryFutureExt};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, Coordinate2D, RasterQueryRectangle, SpatialPartition2D,
    SpatialPartitioned, SpatialResolution, TimeInstance, TimeInterval,
};
use geoengine_datatypes::raster::{
    Blit, EmptyGrid, GeoTransform, Grid2D, GridIndexAccess, GridIndexAccessMut, GridOrEmpty,
    GridSize, MaterializedRasterTile2D, Pixel, RasterTile2D, TileInformation, TilingSpecification,
};
use num_traits::AsPrimitive;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InterpolationParams {
    pub interpolation: InterpolationMethod,
    pub input_resolution: SpatialResolution,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum InterpolationMethod {
    NearestNeighbor,
    BiLinear,
}

pub type Interpolation = Operator<InterpolationParams, SingleRasterSource>;

#[typetag::serde]
#[async_trait]
impl RasterOperator for Interpolation {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let raster_source = self.sources.raster.initialize(context).await?;

        let initialized_operator = InitializedInterpolation {
            result_descriptor: raster_source.result_descriptor().clone(),
            raster_source,
            params: self.params,
            tiling_specification: context.tiling_specification(),
        };

        Ok(initialized_operator.boxed())
    }
}

pub struct InitializedInterpolation {
    result_descriptor: RasterResultDescriptor,
    raster_source: Box<dyn InitializedRasterOperator>,
    params: InterpolationParams,
    tiling_specification: TilingSpecification,
}

impl InitializedRasterOperator for InitializedInterpolation {
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source_processor = self.raster_source.query_processor()?;

        let res = call_on_generic_raster_processor!(
            source_processor, p => match self.params.interpolation  {
                InterpolationMethod::NearestNeighbor => InterploationProcessor::<_,_, NearestNeighbor>::new(
                        p,
                        self.params.clone(),
                        self.tiling_specification,
                        self.result_descriptor.no_data_value.unwrap().as_(),
                    ).boxed()
                    .into(),
                InterpolationMethod::BiLinear => todo!(),
            }
        );

        Ok(res)
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

pub struct InterploationProcessor<Q, P, I>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
    I: InterpolationAlgorithm<P>,
{
    source: Q,
    params: InterpolationParams,
    tiling_specification: TilingSpecification,
    no_data_value: P,
    interpolation: PhantomData<I>,
}

impl<Q, P, I> InterploationProcessor<Q, P, I>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
    I: InterpolationAlgorithm<P>,
{
    pub fn new(
        source: Q,
        params: InterpolationParams,
        tiling_specification: TilingSpecification,
        no_data_value: P,
    ) -> Self {
        Self {
            source,
            params,
            tiling_specification,
            no_data_value,
            interpolation: PhantomData,
        }
    }
}

#[async_trait]
impl<Q, P, I> QueryProcessor for InterploationProcessor<Q, P, I>
where
    Q: QueryProcessor<Output = RasterTile2D<P>, SpatialBounds = SpatialPartition2D>,
    P: Pixel,
    I: InterpolationAlgorithm<P>,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        // TODO: check input resolution < output resolution

        let sub_query = InterpolationSubQuery::<_, P, I> {
            input_resolution: self.params.input_resolution,
            fold_fn: fold_future,
            no_data_value: self.no_data_value,
            tiling_specification: self.tiling_specification,
            phantom: Default::default(),
        };

        Ok(RasterSubQueryAdapter::<'a, P, _, _>::new(
            &self.source,
            query,
            self.tiling_specification,
            ctx,
            sub_query,
        )
        .filter_and_fill(self.no_data_value)) // TODO: check if this is needed
    }
}

#[derive(Debug, Clone)]
pub struct InterpolationSubQuery<F, T, I> {
    input_resolution: SpatialResolution,
    fold_fn: F,
    no_data_value: T,
    tiling_specification: TilingSpecification,
    phantom: PhantomData<I>,
}

impl<'a, T, FoldM, FoldF, I> SubQueryTileAggregator<'a, T> for InterpolationSubQuery<FoldM, T, I>
where
    T: Pixel,
    FoldM: Send + Sync + 'a + Clone + Fn(InterpolationAccu<T, I>, RasterTile2D<T>) -> FoldF,
    FoldF: Send + TryFuture<Ok = InterpolationAccu<T, I>, Error = error::Error>,
    I: InterpolationAlgorithm<T>,
{
    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    type TileAccu = InterpolationAccu<T, I>;
    type TileAccuFuture = BoxFuture<'a, Result<Self::TileAccu>>;

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        create_accu(
            tile_info,
            query_rect,
            self.no_data_value,
            pool.clone(),
            self.tiling_specification,
        )
        .boxed()
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        // enlarge the spatial bounds in order to have the neighbor pixels for the interpolation
        let spatial_bounds = tile_info.spatial_partition();
        let enlarge: Coordinate2D = (self.input_resolution.x, -self.input_resolution.y).into();
        let spatial_bounds = SpatialPartition2D::new(
            spatial_bounds.upper_left(),
            spatial_bounds.lower_right() + enlarge,
        )?;

        Ok(Some(RasterQueryRectangle {
            spatial_bounds,
            time_interval: TimeInterval::new_instant(start_time)?,
            spatial_resolution: self.input_resolution,
        }))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

#[derive(Clone, Debug)]
pub struct InterpolationAccu<T: Pixel, I: InterpolationAlgorithm<T>> {
    pub output_tile: RasterTile2D<T>,
    pub input_tile: RasterTile2D<T>,
    pub pool: Arc<ThreadPool>,
    phantom: PhantomData<I>,
}

impl<T: Pixel, I: InterpolationAlgorithm<T>> InterpolationAccu<T, I> {
    pub fn new(
        input_tile: RasterTile2D<T>,
        output_tile: RasterTile2D<T>,
        pool: Arc<ThreadPool>,
    ) -> Self {
        InterpolationAccu {
            input_tile,
            output_tile,
            pool,
            phantom: Default::default(),
        }
    }
}

impl<T: Pixel, I: InterpolationAlgorithm<T>> FoldTileAccu for InterpolationAccu<T, I> {
    type RasterType = T;

    fn into_tile(self) -> RasterTile2D<Self::RasterType> {
        // now that we collected all the input tile pixels we perform the actual interpolation
        // TODO: this should be done in a separate thread, but requires this method to be async
        let mut output_tile = self.output_tile.into_materialized_tile();

        I::interpolate(&self.input_tile, &mut output_tile, &self.pool).unwrap(); // TODO: propagate error

        output_tile.into()
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

impl<T: Pixel, I: InterpolationAlgorithm<T>> FoldTileAccuMut for InterpolationAccu<T, I> {
    fn tile_mut(&mut self) -> &mut RasterTile2D<T> {
        &mut self.input_tile
    }
}

pub fn create_accu<T: Pixel, I: InterpolationAlgorithm<T>>(
    tile_info: TileInformation,
    query_rect: RasterQueryRectangle,
    no_data_value: T,
    pool: Arc<ThreadPool>,
    tiling_specification: TilingSpecification,
) -> impl Future<Output = Result<InterpolationAccu<T, I>>> {
    // create an accumulator as a single tile that fits all the input tiles
    crate::util::spawn_blocking(move || {
        let tiling = tiling_specification.strategy(
            query_rect.spatial_resolution.x,
            -query_rect.spatial_resolution.y,
        );

        // TODO: enlarge query_rect to be sure to have all the neighbor pixels for the edges

        let origin_coordinate = tiling
            .tile_information_iterator(query_rect.spatial_bounds)
            .next()
            .expect("a query contains at least one tile")
            .spatial_partition()
            .upper_left();

        let geo_transform = GeoTransform::new(
            origin_coordinate,
            query_rect.spatial_resolution.x,
            -query_rect.spatial_resolution.y,
        );

        let bbox = tiling.tile_grid_box(query_rect.spatial_bounds);

        let shape = [
            bbox.axis_size_y() * tiling.tile_size_in_pixels.axis_size_y(),
            bbox.axis_size_x() * tiling.tile_size_in_pixels.axis_size_x(),
        ];

        // create a non-aligned (w.r.t. the tiling specification) grid by setting the origin to the top-left of the tile and the tile-index to [0, 0]
        let grid = Grid2D::new(
            shape.into(),
            vec![no_data_value; shape[0] * shape[1]],
            Some(no_data_value),
        )
        .expect("grid creation must not fail");

        let input_tile = RasterTile2D::new(
            query_rect.time_interval,
            [0, 0].into(),
            geo_transform,
            GridOrEmpty::Grid(grid),
        );

        let output_tile = RasterTile2D::new_with_tile_info(
            query_rect.time_interval,
            tile_info,
            GridOrEmpty::Empty(EmptyGrid::new(
                tiling_specification.tile_size_in_pixels,
                no_data_value,
            )),
        );

        InterpolationAccu::new(input_tile, output_tile, pool)
    })
    .map_err(From::from)
}

pub fn fold_future<T, I>(
    accu: InterpolationAccu<T, I>,
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<InterpolationAccu<T, I>>>
where
    T: Pixel,
    I: InterpolationAlgorithm<T>,
{
    crate::util::spawn_blocking(|| fold_impl(accu, tile)).then(|x| async move {
        match x {
            Ok(r) => r,
            Err(e) => Err(e.into()),
        }
    })
}

pub fn fold_impl<T, I>(
    mut accu: InterpolationAccu<T, I>,
    tile: RasterTile2D<T>,
) -> Result<InterpolationAccu<T, I>>
where
    T: Pixel,
    I: InterpolationAlgorithm<T>,
{
    // get the time now because it is not known when the accu was created
    accu.output_tile.time = tile.time;

    // copy all input tiles into the accu to have all data for interpolation
    let mut accu_input_tile = accu.input_tile.into_materialized_tile();
    accu_input_tile.blit(tile)?;

    Ok(InterpolationAccu::new(
        accu_input_tile.into(),
        accu.output_tile,
        accu.pool,
    ))
}

pub trait InterpolationAlgorithm<P: Pixel>: Send + Sync + Clone + 'static {
    /// interpolate the given input tile into the output tile
    /// the output must be fully contained in the input tile and have an additional row and column in order
    /// to have all the required neighbor pixels.
    /// Also the output must have a finer resolution than the input
    fn interpolate(
        input: &RasterTile2D<P>,
        output: &mut MaterializedRasterTile2D<P>,
        pool: &ThreadPool,
    ) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct NearestNeighbor {}

impl<P> InterpolationAlgorithm<P> for NearestNeighbor
where
    P: Pixel,
{
    fn interpolate(
        input: &RasterTile2D<P>,
        output: &mut MaterializedRasterTile2D<P>,
        _pool: &ThreadPool,
    ) -> Result<()> {
        // TODO: parallelize

        let info_in = input.tile_information();
        let in_upper_left = info_in.spatial_partition().upper_left();
        let in_x_size = info_in.global_geo_transform.x_pixel_size();
        let in_y_size = info_in.global_geo_transform.y_pixel_size();

        let info_out = output.tile_information();
        let out_upper_left = info_out.spatial_partition().upper_left();
        let out_x_size = info_out.global_geo_transform.x_pixel_size();
        let out_y_size = info_out.global_geo_transform.y_pixel_size();

        // let x_offset = ((out_upper_left.x - in_upper_left.x) / in_x_size).floor() as isize;
        // let y_offset = ((out_upper_left.y - in_upper_left.y) / in_y_size).floor() as isize;

        for y in 0..output.grid_array.axis_size_y() {
            let out_y_coord = out_upper_left.y + y as f64 * out_y_size;
            let nearest_in_y_idx = ((out_y_coord - in_upper_left.y) / in_y_size).round() as isize;

            for x in 0..output.grid_array.axis_size_x() {
                let out_x_coord = out_upper_left.x + x as f64 * out_x_size;
                let nearest_in_x_idx =
                    ((out_x_coord - in_upper_left.x) / in_x_size).round() as isize;

                let value = input.get_at_grid_index([nearest_in_y_idx, nearest_in_x_idx])?;

                output.set_at_grid_index([y as isize, x as isize], value)?;
            }
        }

        Ok(())
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
        util::create_rayon_thread_pool,
    };

    #[test]
    fn nearest_neightbor() {
        let input = RasterTile2D::new_with_tile_info(
            Default::default(),
            TileInformation {
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 3].into(),
                global_geo_transform: GeoTransform::new((0.0, 2.0).into(), 1.0, -1.0),
            },
            GridOrEmpty::Grid(
                Grid2D::new([3, 3].into(), vec![1, 2, 3, 4, 5, 6, 7, 8, 9], Some(42)).unwrap(),
            ),
        );

        let mut output = RasterTile2D::new_with_tile_info(
            Default::default(),
            TileInformation {
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [4, 4].into(),
                global_geo_transform: GeoTransform::new((0.0, 2.0).into(), 0.5, -0.5),
            },
            GridOrEmpty::Grid(Grid2D::new([4, 4].into(), vec![42; 16], Some(42)).unwrap()),
        )
        .into_materialized_tile();

        let pool = create_rayon_thread_pool(0);

        NearestNeighbor::interpolate(&input, &mut output, &pool).unwrap();

        assert_eq!(
            output.grid_array.data,
            vec![1, 2, 2, 3, 4, 5, 5, 6, 4, 5, 5, 6, 7, 8, 8, 9]
        );
    }

    #[tokio::test]
    async fn nearest_neighbor_operator() -> Result<()> {
        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [2, 2].into(),
        ));

        let raster = make_raster();

        let operator = Interpolation {
            params: InterpolationParams {
                interpolation: InterpolationMethod::NearestNeighbor,
                input_resolution: SpatialResolution::one(),
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
            vec![4, 42, 8, 42],
            vec![5, 6, 42, 42],
            vec![6, 7, 42, 42],
            vec![7, 8, 42, 42],
            vec![8, 42, 42, 42],
            vec![8, 7, 4, 3],
            vec![7, 6, 3, 2],
            vec![6, 5, 2, 1],
            vec![5, 42, 1, 42],
            vec![4, 3, 42, 42],
            vec![3, 2, 42, 42],
            vec![2, 1, 42, 42],
            vec![1, 42, 42, 42],
        ];

        for (i, tile) in result.into_iter().enumerate() {
            let tile = tile.into_materialized_tile();
            assert_eq!(tile.time, times[i]);
            assert_eq!(tile.grid_array.data, data[i]);
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
        let no_data_value = Some(42);
        let raster_tiles = vec![
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [2, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([2, 2].into(), vec![1, 2, 5, 6], no_data_value).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [2, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([2, 2].into(), vec![3, 4, 7, 8], no_data_value).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [2, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([2, 2].into(), vec![8, 7, 4, 3], no_data_value).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [2, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([2, 2].into(), vec![6, 5, 2, 1], no_data_value).unwrap(),
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
                    no_data_value: no_data_value.map(f64::from),
                },
            },
        }
        .boxed()
    }
}
