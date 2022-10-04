use super::kernel_function::KernelFunction;
use crate::adapters::{FoldTileAccu, SubQueryTileAggregator};
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use geoengine_datatypes::primitives::{AxisAlignedRectangle, SpatialPartitioned};
use geoengine_datatypes::raster::{Blit, EmptyGrid2D, GeoTransform, GridOrEmpty, GridSize};
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

#[derive(Debug, Clone)]
pub struct RasterKernelTileNeighborhood<P, F> {
    kernel_fn: F,
    tiling_specification: TilingSpecification,
    _phantom_types: PhantomData<P>,
}

impl<P, F> RasterKernelTileNeighborhood<P, F> {
    pub fn new(kernel_fn: F, tiling_specification: TilingSpecification) -> Self {
        Self {
            kernel_fn,
            tiling_specification,
            _phantom_types: PhantomData,
        }
    }
}

impl<'a, P, F> SubQueryTileAggregator<'a, P> for RasterKernelTileNeighborhood<P, F>
where
    P: Pixel,
    f64: AsPrimitive<P>,
    F: KernelFunction<P> + 'static,
    // FoldM: Send + Sync + 'a + Clone + Fn(RasterKernelAccu<P>, RasterTile2D<P>) -> FoldF,
    // FoldF: Send + TryFuture<Ok = RasterKernelAccu<P>, Error = crate::error::Error>,
{
    type FoldFuture = FoldFuture<P, F>;

    type FoldMethod = fn(RasterKernelAccu<P, F>, RasterTile2D<P>) -> Self::FoldFuture;

    type TileAccu = RasterKernelAccu<P, F>;
    type TileAccuFuture = BoxFuture<'a, Result<Self::TileAccu>>;

    /// Create a 9x9 tile to store the values of the neighborhood
    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        let pool = pool.clone();
        let tiling_specification = self.tiling_specification;
        let kernel_fn = self.kernel_fn.clone();
        crate::util::spawn_blocking(move || {
            create_9x9_tile(tile_info, query_rect, pool, tiling_specification, kernel_fn)
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
        let one_pixel = Coordinate2D::from((
            tile_info.global_geo_transform.x_pixel_size(),
            -tile_info.global_geo_transform.y_pixel_size(),
        ));

        let enlarged_spatial_bounds = SpatialPartition2D::new(
            spatial_bounds.upper_left() - one_pixel,
            spatial_bounds.lower_right() + one_pixel,
        )?;

        Ok(Some(RasterQueryRectangle {
            spatial_bounds: enlarged_spatial_bounds,
            time_interval: TimeInterval::new_instant(start_time)?,
            spatial_resolution: query_rect.spatial_resolution,
        }))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        |accu, tile| {
            crate::util::spawn_blocking(|| merge_tile_into_9x9_tile(accu, tile)).map(flatten_result)
        }
    }
}

#[derive(Clone, Debug)]
pub struct RasterKernelAccu<P: Pixel, F> {
    pub output_info: TileInformation,
    pub input_tile: RasterTile2D<P>,
    pub pool: Arc<ThreadPool>,
    pub kernel_fn: F,
}

impl<P: Pixel, F> RasterKernelAccu<P, F> {
    pub fn new(
        input_tile: RasterTile2D<P>,
        output_info: TileInformation,
        pool: Arc<ThreadPool>,
        kernel_fn: F,
    ) -> Self {
        RasterKernelAccu {
            output_info,
            input_tile,
            pool,
            kernel_fn,
        }
    }
}

#[async_trait]
impl<P: Pixel, F: KernelFunction<P>> FoldTileAccu for RasterKernelAccu<P, F>
where
    f64: AsPrimitive<P>,
{
    type RasterType = P;

    /// now that we collected all the input tile pixels we perform the actual raster kernel
    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>> {
        let output_tile = crate::util::spawn_blocking_with_thread_pool(self.pool, move || {
            apply_kernel_for_each_inner_pixel()
        })
        .await??;

        Ok(output_tile)
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

/// Apply kernel function to all pixels of the inner input tile in the 9x9 grid
fn apply_kernel_for_each_inner_pixel<P: Pixel>() -> Result<RasterTile2D<P>> {
    todo!("implement")
}

fn create_9x9_tile<P: Pixel, F: KernelFunction<P>>(
    tile_info: TileInformation,
    query_rect: RasterQueryRectangle,
    pool: Arc<ThreadPool>,
    tiling_specification: TilingSpecification,
    kernel_fn: F,
) -> RasterKernelAccu<P, F>
where
    f64: AsPrimitive<P>,
{
    // create an accumulator as a single 9x9 tile that fits all the input tiles

    let tiling = tiling_specification.strategy(
        query_rect.spatial_resolution.x,
        -query_rect.spatial_resolution.y,
    );

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
    let grid = EmptyGrid2D::new(shape.into());

    let input_tile = RasterTile2D::new(
        query_rect.time_interval,
        [0, 0].into(),
        geo_transform,
        GridOrEmpty::from(grid),
    );

    RasterKernelAccu::new(input_tile, tile_info, pool, kernel_fn)
}

type FoldFutureFn<P, F> = fn(
    Result<Result<RasterKernelAccu<P, F>>, tokio::task::JoinError>,
) -> Result<RasterKernelAccu<P, F>>;
type FoldFuture<P, F> =
    futures::future::Map<JoinHandle<Result<RasterKernelAccu<P, F>>>, FoldFutureFn<P, F>>;

/// Turn a result of results into a result
fn flatten_result<P: Pixel, F: KernelFunction<P>>(
    result: Result<Result<RasterKernelAccu<P, F>>, tokio::task::JoinError>,
) -> Result<RasterKernelAccu<P, F>>
where
    f64: AsPrimitive<P>,
{
    match result {
        Ok(r) => r,
        Err(e) => Err(e.into()),
    }
}

/// Merge, step by step, the 9 input tiles into the larger accumulator tile
pub fn merge_tile_into_9x9_tile<P: Pixel, F: KernelFunction<P>>(
    mut accu: RasterKernelAccu<P, F>,
    tile: RasterTile2D<P>,
) -> Result<RasterKernelAccu<P, F>>
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

    Ok(RasterKernelAccu::new(
        accu_input_tile.into(),
        accu.output_info,
        accu.pool,
        accu.kernel_fn,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{MockExecutionContext, MockQueryContext};
    use geoengine_datatypes::util::test::TestDefault;

    #[test]
    fn test_create_9x9_tile() {
        // let execution_context = MockExecutionContext::test_default();
        // let query_context = MockQueryContext::test_default();
        // let tile = create_9x9_tile(execution_context.tiling_specification, query_rect, pool, tiling_specification, kernel_fn)
    }
}
