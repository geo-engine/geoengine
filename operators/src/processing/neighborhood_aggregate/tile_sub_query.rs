use super::aggregate::{AggregateFunction, Neighborhood};
use crate::adapters::{FoldTileAccu, SubQueryTileAggregator};
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use geoengine_datatypes::primitives::CacheHint;
use geoengine_datatypes::raster::{
    ChangeGridBounds, FromIndexFnParallel, GridBlit, GridBoundingBox2D, GridContains, GridIdx,
    GridIdx2D, GridIndexAccess, GridOrEmpty, GridSize, TilingStrategy,
};
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, TimeInstance, TimeInterval},
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
    tiling_specification: TilingSpecification,
    _phantom_types: PhantomData<(P, A)>,
}

impl<P, A> NeighborhoodAggregateTileNeighborhood<P, A> {
    pub fn new(neighborhood: Neighborhood, tiling_specification: TilingSpecification) -> Self {
        Self {
            neighborhood,
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
        let neighborhood = self.neighborhood.clone();
        crate::util::spawn_blocking(move || {
            create_enlarged_tile(
                tile_info,
                &query_rect,
                pool,
                tiling_specification,
                neighborhood,
            )
        })
        .map_err(From::from)
        .boxed()
    }

    /// Enlarge the spatial bounds to all sides to have all neighboring tiles in the sub-query
    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
        band_idx: u32,
    ) -> Result<Option<RasterQueryRectangle>> {
        let pixel_bounds = tile_info.global_pixel_bounds();

        let margin_y = self.neighborhood.y_radius() as isize;
        let margin_x = self.neighborhood.x_radius() as isize;

        let larger_bounds = GridBoundingBox2D::new_min_max(
            pixel_bounds.y_min() - margin_y,
            pixel_bounds.y_max() + margin_y,
            pixel_bounds.x_min() - margin_x,
            pixel_bounds.x_max() + margin_x,
        )?;

        Ok(Some(RasterQueryRectangle::new_with_grid_bounds(
            larger_bounds,
            TimeInterval::new_instant(start_time)?,
            band_idx.into(),
        )))
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
    pub accu_grid: GridOrEmpty<GridBoundingBox2D, P>,
    pub accu_time: TimeInterval,
    pub accu_cache_hint: CacheHint,
    pub accu_band: u32,
    pub pool: Arc<ThreadPool>,
    pub neighborhood: Neighborhood,
    phantom_aggregate_fn: PhantomData<A>,
}

impl<P: Pixel, A> NeighborhoodAggregateAccu<P, A> {
    pub fn new(
        accu_grid: GridOrEmpty<GridBoundingBox2D, P>,
        accu_time: TimeInterval,
        accu_cache_hint: CacheHint,
        accu_band: u32,
        output_info: TileInformation,
        pool: Arc<ThreadPool>,
        neighborhood: Neighborhood,
    ) -> Self {
        NeighborhoodAggregateAccu {
            output_info,
            accu_grid,
            accu_time,
            accu_cache_hint,
            accu_band,
            pool,
            neighborhood,
            phantom_aggregate_fn: PhantomData,
        }
    }
}

#[async_trait]
impl<P, A> FoldTileAccu for NeighborhoodAggregateAccu<P, A>
where
    P: Pixel,
    f64: AsPrimitive<P>,
    A: AggregateFunction + 'static,
{
    type RasterType = P;

    /// now that we collected all the input tile pixels we perform the actual raster kernel
    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>> {
        let neighborhood = self.neighborhood.clone();
        let output_tile = crate::util::spawn_blocking_with_thread_pool(self.pool, move || {
            apply_kernel_for_each_inner_pixel::<P, A>(
                &self.accu_grid,
                &self.accu_time,
                self.accu_cache_hint,
                self.accu_band,
                &self.output_info,
                &neighborhood,
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
    accu_grid: &GridOrEmpty<GridBoundingBox2D, P>,
    accu_time: &TimeInterval,
    accu_cache_hint: CacheHint,
    accu_band: u32,
    info_out: &TileInformation,
    neighborhood: &Neighborhood,
) -> RasterTile2D<P>
where
    P: Pixel,
    f64: AsPrimitive<P>,
    A: AggregateFunction,
{
    if accu_grid.is_empty() {
        return RasterTile2D::new_with_tile_info(
            *accu_time,
            *info_out,
            0, // TODO
            GridOrEmpty::new_empty_shape(info_out.tile_size_in_pixels),
            accu_cache_hint, // TODO: is this correct? Was CacheHint::max_duration() before
        );
    }

    let map_fn = |gidx: GridIdx2D| {
        let GridIdx([y, x]) = gidx;

        let mut neighborhood_matrix =
            Vec::<Option<f64>>::with_capacity(neighborhood.matrix().number_of_elements());

        let y_start = y - neighborhood.y_radius() as isize;
        let x_start = x - neighborhood.x_radius() as isize;

        let y_stop = y + neighborhood.y_radius() as isize;
        let x_stop = x + neighborhood.x_radius() as isize;
        // copy row-by-row all pixels in x direction into kernel matrix
        for y_index in y_start..=y_stop {
            for x_index in x_start..=x_stop {
                neighborhood_matrix.push(
                    accu_grid
                        .get_at_grid_index_unchecked([y_index, x_index])
                        .map(AsPrimitive::as_),
                );
            }
        }

        A::apply(&neighborhood.apply(neighborhood_matrix))
    };

    let out_pixel_bounds = info_out.global_pixel_bounds();

    debug_assert!(accu_grid.shape_ref().contains(&out_pixel_bounds));

    // TODO: this will check for empty tiles. Change to MaskedGrid::from(…) to avoid this.
    let out_data = GridOrEmpty::from_index_fn_parallel(&out_pixel_bounds, map_fn);

    debug_assert_eq!(
        out_data.shape_ref().axis_size(),
        info_out.tile_size_in_pixels.axis_size()
    );

    RasterTile2D::new(
        *accu_time,
        info_out.global_tile_position,
        accu_band,
        info_out.global_geo_transform,
        out_data.unbounded(),
        accu_cache_hint.clone_with_current_datetime(),
    )
}

fn create_enlarged_tile<P: Pixel, A: AggregateFunction>(
    tile_info: TileInformation,
    query_rect: &RasterQueryRectangle,
    pool: Arc<ThreadPool>,
    tiling_specification: TilingSpecification,
    neighborhood: Neighborhood,
) -> NeighborhoodAggregateAccu<P, A> {
    // create an accumulator as a single tile that fits all the input tiles + some margin for the kernel size

    let tiling = TilingStrategy::new(
        tiling_specification.tile_size_in_pixels,
        tile_info.global_geo_transform,
    );

    let target_tile_start =
        tiling_specification.tile_idx_to_global_pixel_idx(tile_info.global_tile_position);
    let accu_start = target_tile_start
        - GridIdx([
            neighborhood.y_radius() as isize,
            neighborhood.x_radius() as isize,
        ]);
    let accu_end = accu_start
        + GridIdx2D::new_y_x(
            tiling.tile_size_in_pixels.y() as isize + 2 * neighborhood.y_radius() as isize - 1, // -1 because the end is inclusive
            tiling.tile_size_in_pixels.x() as isize + 2 * neighborhood.x_radius() as isize - 1,
        );

    let accu_bounds = GridBoundingBox2D::new(accu_start, accu_end)
        .expect("accu bounds must be valid because they are calculated from valid bounds");

    // create a non-aligned (w.r.t. the tiling specification) grid by setting the origin to the top-left of the tile and the tile-index to [0, 0]
    let grid = GridOrEmpty::new_empty_shape(accu_bounds);

    NeighborhoodAggregateAccu::new(
        grid,
        query_rect.time_interval,
        CacheHint::max_duration(),
        0,
        tile_info,
        pool,
        neighborhood,
    )
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
    accu.accu_time = tile.time;
    accu.accu_cache_hint = tile.cache_hint;

    // if the tile is empty, we can skip it
    if tile.is_empty() {
        return Ok(accu);
    }

    // copy all input tiles into the accu to have all data for raster kernel
    let x = tile.into_inner_positioned_grid();

    accu.accu_grid.grid_blit_from(&x);

    Ok(accu)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        engine::MockExecutionContext,
        processing::neighborhood_aggregate::{
            aggregate::{StandardDeviation, Sum},
            NeighborhoodParams,
        },
    };
    use geoengine_datatypes::{
        primitives::BandSelection,
        raster::{GeoTransform, GridBoundingBox2D, SpatialGridDefinition, TilingStrategy},
        util::test::TestDefault,
    };

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_create_enlarged_tile() {
        let execution_context =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::test_default());

        let spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new_with_coordinate_x_y(0., 1., 0., -1.),
            GridBoundingBox2D::new([-2, 0], [-1, 1]).unwrap(),
        );

        let tiling_strategy = TilingStrategy::new(
            execution_context.tiling_specification.tile_size_in_pixels,
            spatial_grid.geo_transform(),
        );

        let tile_info = tiling_strategy
            .tile_information_iterator_from_grid_bounds(spatial_grid.grid_bounds())
            .next()
            .unwrap();

        let qrect = RasterQueryRectangle::new_with_grid_bounds(
            tile_info.global_pixel_bounds(),
            TimeInstance::from_millis(0).unwrap().into(),
            BandSelection::first(),
        );

        let aggregator = NeighborhoodAggregateTileNeighborhood::<u8, StandardDeviation>::new(
            NeighborhoodParams::Rectangle { dimensions: [5, 5] }
                .try_into()
                .unwrap(),
            execution_context.tiling_specification,
        );

        let tile_query_rectangle = aggregator
            .tile_query_rectangle(tile_info, qrect.clone(), qrect.time_interval.start(), 0)
            .unwrap()
            .unwrap();

        assert_eq!(
            tile_info.global_pixel_bounds(),
            GridBoundingBox2D::new([-512, 0], [-1, 511]).unwrap()
        );

        assert_eq!(
            tile_query_rectangle.spatial_query().grid_bounds(),
            GridBoundingBox2D::new([-514, -2], [1, 513]).unwrap()
        );

        let accu = create_enlarged_tile::<u8, Sum>(
            tile_info,
            &tile_query_rectangle,
            execution_context.thread_pool.clone(),
            execution_context.tiling_specification,
            aggregator.neighborhood,
        );

        assert_eq!(tile_info.tile_size_in_pixels.axis_size(), [512, 512]);
        assert_eq!(
            accu.accu_grid.shape_ref().axis_size(),
            [512 + 2 + 2, 512 + 2 + 2]
        );
    }
}
