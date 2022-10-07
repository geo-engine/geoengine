use super::aggregate::{AggregateFunction, Neighborhood};
use crate::adapters::{FoldTileAccu, SubQueryTileAggregator};
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use geoengine_datatypes::primitives::{AxisAlignedRectangle, SpatialPartitioned};
use geoengine_datatypes::raster::{
    Blit, EmptyGrid, EmptyGrid2D, FromIndexFnParallel, GeoTransform, GridIdx, GridIdx2D,
    GridIndexAccess, GridOrEmpty, GridSize,
};
use geoengine_datatypes::{
    primitives::{
        Coordinate2D, RasterQueryRectangle, SpatialPartition2D, TimeInstance, TimeInterval,
    },
    raster::{Pixel, RasterTile2D, TileInformation, TilingSpecification},
};
use num_traits::AsPrimitive;
use rayon::ThreadPool;
use std::{marker::PhantomData, sync::Arc};
use tokio::task::JoinHandle;

/// A sub-query aggregator that queries for each output tile an enlarged input tiles.
/// This means itself plus parts of the 8 surrounding tiles.
///
/// ```notest
/// --------|--------|--------
/// -      -|-      -|-      -
/// -    xx-|-xxxxxx-|-xx    -
/// --------|--------|--------
/// -    xx-|-xxxxxx-|-xx    -
/// -    xx-|-xxxxxx-|-xx    -
/// --------|--------|--------
/// -    xx-|-xxxxxx-|-xx    -
/// -      -|-      -|-      -
/// --------|--------|--------
/// ```
///
/// It then applies a kernel function to each pixel and its surrounding.
///
#[derive(Debug, Clone)]
pub struct NeighborhoodAggregateTileNeighborhood<P, A> {
    neighborhood: Neighborhood,
    aggregate_fn: A,
    tiling_specification: TilingSpecification,
    _phantom_types: PhantomData<P>,
}

impl<P, A> NeighborhoodAggregateTileNeighborhood<P, A> {
    pub fn new(
        neighborhood: Neighborhood,
        aggregate_fn: A,
        tiling_specification: TilingSpecification,
    ) -> Self {
        Self {
            neighborhood,
            aggregate_fn,
            tiling_specification,
            _phantom_types: PhantomData,
        }
    }
}

impl<'a, P, A> SubQueryTileAggregator<'a, P> for NeighborhoodAggregateTileNeighborhood<P, A>
where
    P: Pixel,
    f64: AsPrimitive<P>,
    A: AggregateFunction + 'static,
{
    type FoldFuture = FoldFuture<P, A>;

    type FoldMethod = fn(NeighborhoodAggregateAccu<P, A>, RasterTile2D<P>) -> Self::FoldFuture;

    type TileAccu = NeighborhoodAggregateAccu<P, A>;
    type TileAccuFuture = BoxFuture<'a, Result<Self::TileAccu>>;

    /// Create an enlarged tile to store the values of the neighborhood
    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        let pool = pool.clone();
        let tiling_specification = self.tiling_specification;
        let kernel_fn = self.aggregate_fn.clone();
        let neighborhood = self.neighborhood.clone();
        crate::util::spawn_blocking(move || {
            create_enlarged_tile(
                tile_info,
                query_rect,
                pool,
                tiling_specification,
                neighborhood,
                kernel_fn,
            )
        })
        .map_err(From::from)
        .boxed()
    }

    /// Enlarge the spatial bounds to all sides to have all neighboring tiles in the sub-query
    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        let spatial_bounds = tile_info.spatial_partition();

        let margin_pixels = Coordinate2D::from((
            self.neighborhood.x_radius() as f64 * tile_info.global_geo_transform.x_pixel_size(),
            self.neighborhood.y_radius() as f64 * tile_info.global_geo_transform.y_pixel_size(),
        ));

        let enlarged_spatial_bounds = SpatialPartition2D::new(
            spatial_bounds.upper_left() - margin_pixels,
            spatial_bounds.lower_right() + margin_pixels,
        )?;

        Ok(Some(RasterQueryRectangle {
            spatial_bounds: enlarged_spatial_bounds,
            time_interval: TimeInterval::new_instant(start_time)?,
            spatial_resolution: query_rect.spatial_resolution,
        }))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        |accu, tile| {
            crate::util::spawn_blocking(|| merge_tile_into_enlarged_tile(accu, tile))
                .map(flatten_result)
        }
    }
}

#[derive(Clone, Debug)]
pub struct NeighborhoodAggregateAccu<P: Pixel, A> {
    pub output_info: TileInformation,
    pub input_tile: RasterTile2D<P>,
    pub pool: Arc<ThreadPool>,
    pub neighborhood: Neighborhood,
    pub kernel_fn: A,
}

impl<P: Pixel, F> NeighborhoodAggregateAccu<P, F> {
    pub fn new(
        input_tile: RasterTile2D<P>,
        output_info: TileInformation,
        pool: Arc<ThreadPool>,
        neighborhood: Neighborhood,
        kernel_fn: F,
    ) -> Self {
        NeighborhoodAggregateAccu {
            output_info,
            input_tile,
            pool,
            neighborhood,
            kernel_fn,
        }
    }
}

#[async_trait]
impl<P, F> FoldTileAccu for NeighborhoodAggregateAccu<P, F>
where
    P: Pixel,
    f64: AsPrimitive<P>,
    F: AggregateFunction + 'static,
{
    type RasterType = P;

    /// now that we collected all the input tile pixels we perform the actual raster kernel
    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>> {
        let neighborhood = self.neighborhood.clone();
        let kernel_fn = self.kernel_fn.clone();
        let output_tile = crate::util::spawn_blocking_with_thread_pool(self.pool, move || {
            apply_kernel_for_each_inner_pixel(
                &self.input_tile,
                &self.output_info,
                &neighborhood,
                &kernel_fn,
            )
        })
        .await?;

        Ok(output_tile)
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

/// Apply kernel function to all pixels of the inner input tile in the 9x9 grid
fn apply_kernel_for_each_inner_pixel<P, A>(
    input: &RasterTile2D<P>,
    info_out: &TileInformation,
    neighborhood: &Neighborhood,
    aggregate_fn: &A,
) -> RasterTile2D<P>
where
    P: Pixel,
    f64: AsPrimitive<P>,
    A: AggregateFunction,
{
    if input.is_empty() {
        return RasterTile2D::new_with_tile_info(
            input.time,
            *info_out,
            EmptyGrid::new(info_out.tile_size_in_pixels).into(),
        );
    }

    let map_fn = |gidx: GridIdx2D| {
        let GridIdx([y, x]) = gidx;

        let mut kernel_matrix =
            Vec::<Option<f64>>::with_capacity(neighborhood.matrix().number_of_elements());

        let y_stop = y + neighborhood.y_width() as isize;
        let x_stop = x + neighborhood.x_width() as isize;
        // copy row-by-row all pixels in x direction into kernel matrix
        for y_index in y..y_stop {
            for x_index in x..x_stop {
                kernel_matrix.push(
                    input
                        .get_at_grid_index_unchecked([y_index, x_index])
                        .map(AsPrimitive::as_),
                );
            }
        }

        aggregate_fn.apply(&neighborhood.apply(kernel_matrix))
    };

    // TODO: this will check for empty tiles. Change to MaskedGrid::from(…) to avoid this.
    let out_data = GridOrEmpty::from_index_fn_parallel(&info_out.tile_size_in_pixels, map_fn);

    RasterTile2D::new(
        input.time,
        info_out.global_tile_position,
        info_out.global_geo_transform,
        out_data,
    )
}

fn create_enlarged_tile<P: Pixel, A: AggregateFunction>(
    tile_info: TileInformation,
    query_rect: RasterQueryRectangle,
    pool: Arc<ThreadPool>,
    tiling_specification: TilingSpecification,
    neighborhood: Neighborhood,
    kernel_fn: A,
) -> NeighborhoodAggregateAccu<P, A> {
    // create an accumulator as a single tile that fits all the input tiles + some margin for the kernel size

    let tiling = tiling_specification.strategy(
        query_rect.spatial_resolution.x,
        -query_rect.spatial_resolution.y,
    );

    let origin_coordinate = query_rect.spatial_bounds.upper_left();

    let geo_transform = GeoTransform::new(
        origin_coordinate,
        query_rect.spatial_resolution.x,
        -query_rect.spatial_resolution.y,
    );

    let shape = [
        tiling.tile_size_in_pixels.axis_size_y() + 2 * neighborhood.y_radius(),
        tiling.tile_size_in_pixels.axis_size_x() + 2 * neighborhood.x_radius(),
    ];

    // create a non-aligned (w.r.t. the tiling specification) grid by setting the origin to the top-left of the tile and the tile-index to [0, 0]
    let grid = EmptyGrid2D::new(shape.into());

    let input_tile = RasterTile2D::new(
        query_rect.time_interval,
        [0, 0].into(),
        geo_transform,
        GridOrEmpty::from(grid),
    );

    NeighborhoodAggregateAccu::new(input_tile, tile_info, pool, neighborhood, kernel_fn)
}

type FoldFutureFn<P, F> = fn(
    Result<Result<NeighborhoodAggregateAccu<P, F>>, tokio::task::JoinError>,
) -> Result<NeighborhoodAggregateAccu<P, F>>;
type FoldFuture<P, F> =
    futures::future::Map<JoinHandle<Result<NeighborhoodAggregateAccu<P, F>>>, FoldFutureFn<P, F>>;

/// Turn a result of results into a result
fn flatten_result<P: Pixel, F: AggregateFunction>(
    result: Result<Result<NeighborhoodAggregateAccu<P, F>>, tokio::task::JoinError>,
) -> Result<NeighborhoodAggregateAccu<P, F>>
where
    f64: AsPrimitive<P>,
{
    match result {
        Ok(r) => r,
        Err(e) => Err(e.into()),
    }
}

/// Merge, step by step, the 9 input tiles into the larger accumulator tile
pub fn merge_tile_into_enlarged_tile<P: Pixel, F: AggregateFunction>(
    mut accu: NeighborhoodAggregateAccu<P, F>,
    tile: RasterTile2D<P>,
) -> Result<NeighborhoodAggregateAccu<P, F>>
where
    f64: AsPrimitive<P>,
{
    // get the time now because it is not known when the accu was created
    accu.input_tile.time = tile.time;

    // if the tile is empty, we can skip it
    if tile.is_empty() {
        return Ok(accu);
    }

    // copy all input tiles into the accu to have all data for raster kernel
    let mut accu_input_tile = accu.input_tile.into_materialized_tile();
    accu_input_tile.blit(tile)?;

    let accu_input_tile: RasterTile2D<P> = accu_input_tile.into();

    Ok(NeighborhoodAggregateAccu::new(
        accu_input_tile,
        accu.output_info,
        accu.pool,
        accu.neighborhood,
        accu.kernel_fn,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        engine::MockExecutionContext,
        processing::neighborhood_aggregate::{aggregate::StandardDeviation, NeighborhoodParams},
    };
    use geoengine_datatypes::{
        primitives::SpatialResolution, raster::TilingStrategy, util::test::TestDefault,
    };

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_create_enlarged_tile() {
        let execution_context = MockExecutionContext::test_default();

        let spatial_resolution = SpatialResolution::one();
        let tiling_strategy = TilingStrategy::new_with_tiling_spec(
            execution_context.tiling_specification,
            spatial_resolution.x,
            -spatial_resolution.y,
        );

        let spatial_partition = SpatialPartition2D::new((0., 1.).into(), (1., 0.).into()).unwrap();
        let tile_info = tiling_strategy
            .tile_information_iterator(spatial_partition)
            .next()
            .unwrap();

        let qrect = RasterQueryRectangle {
            spatial_bounds: tile_info.spatial_partition(),
            time_interval: TimeInstance::from_millis(0).unwrap().into(),
            spatial_resolution,
        };

        let aggregator = NeighborhoodAggregateTileNeighborhood::<u8, _>::new(
            NeighborhoodParams::Rectangle { dimensions: [5, 5] }
                .try_into()
                .unwrap(),
            StandardDeviation::new(),
            execution_context.tiling_specification,
        );

        let tile_query_rectangle = aggregator
            .tile_query_rectangle(tile_info, qrect, qrect.time_interval.start())
            .unwrap()
            .unwrap();

        assert_eq!(
            tile_info.spatial_partition(),
            SpatialPartition2D::new((0., 512.).into(), (512., 0.).into()).unwrap()
        );
        assert_eq!(
            tile_query_rectangle.spatial_bounds,
            SpatialPartition2D::new((-2., 514.).into(), (514., -2.).into()).unwrap()
        );

        let accu = create_enlarged_tile::<u8, _>(
            tile_info,
            tile_query_rectangle,
            execution_context.thread_pool.clone(),
            execution_context.tiling_specification,
            aggregator.neighborhood,
            aggregator.aggregate_fn,
        );

        assert_eq!(tile_info.tile_size_in_pixels.axis_size(), [512, 512]);
        assert_eq!(
            accu.input_tile.grid_array.shape_ref().axis_size(),
            [512 + 2 + 2, 512 + 2 + 2]
        );

        assert_eq!(accu.input_tile.tile_geo_transform().x_pixel_size(), 1.);
        assert_eq!(accu.input_tile.tile_geo_transform().y_pixel_size(), -1.);
    }
}
