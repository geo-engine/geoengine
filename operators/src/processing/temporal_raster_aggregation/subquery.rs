use super::aggregators::TemporalRasterPixelAggregator;
use crate::{
    adapters::{FoldTileAccu, SubQueryTileAggregator},
    util::Result,
};
use async_trait::async_trait;
use futures::TryFuture;
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, SpatialPartitioned, TimeInstance, TimeInterval, TimeStep},
    raster::{
        EmptyGrid2D, GeoTransform, GridIdx2D, GridIndexAccess, GridOrEmpty, GridOrEmpty2D,
        GridShapeAccess, Pixel, RasterTile2D, TileInformation, UpdateIndexedElementsParallel,
    },
};
use rayon::ThreadPool;
use std::{marker::PhantomData, sync::Arc};

/// A method to fold a tile into the accumulator.
pub async fn subquery_all_tiles_fold_fn<P: Pixel, F: TemporalRasterPixelAggregator<P> + 'static>(
    accu: TileAccumulator<P, F>,
    tile: RasterTile2D<P>,
) -> Result<TileAccumulator<P, F>> {
    crate::util::spawn_blocking_with_thread_pool(accu.pool.clone(), || {
        let mut accu = accu;
        accu.add_tile(tile)?;
        Ok(accu)
    })
    .await?
}

/// An accumulator for a time series of tiles in the same position.
#[derive(Debug, Clone)]
pub struct TileAccumulator<P: Pixel, F: TemporalRasterPixelAggregator<P>> {
    time: TimeInterval,
    tile_position: GridIdx2D,
    global_geo_transform: GeoTransform,
    state_grid: GridOrEmpty2D<F::PixelState>,
    prestine: bool,
    pool: Arc<ThreadPool>,
}

impl<P, F> TileAccumulator<P, F>
where
    P: Pixel,
    F: TemporalRasterPixelAggregator<P> + 'static,
{
    pub fn add_tile(&mut self, in_tile: RasterTile2D<P>) -> Result<()> {
        self.time = self.time.union(&in_tile.time)?;

        debug_assert!(self.state_grid.grid_shape() == in_tile.grid_shape());

        let in_tile_grid = match in_tile.grid_array {
            GridOrEmpty::Grid(g) => g,
            GridOrEmpty::Empty(_) => {
                self.prestine = false;
                return Ok(());
            }
        };

        match &mut self.state_grid {
            GridOrEmpty::Empty(_) if !self.prestine && !F::IGNORE_NO_DATA => {
                // every pixel is nodata we will keep it like this forever
            }

            GridOrEmpty::Empty(_) => {
                // TODO: handle case where this could stays empty

                let map_fn = |lin_idx: usize, _acc_values_option| {
                    let new_value_option = in_tile_grid.get_at_grid_index_unchecked(lin_idx);
                    F::initialize(new_value_option)
                };

                self.state_grid.update_indexed_elements_parallel(map_fn);
            }

            GridOrEmpty::Grid(g) => {
                let map_fn = |lin_idx: usize, acc_values_option: Option<F::PixelState>| {
                    let new_value_option = in_tile_grid.get_at_grid_index_unchecked(lin_idx);
                    F::aggregate(acc_values_option, new_value_option)
                };

                g.update_indexed_elements_parallel(map_fn);
            }
        }

        self.prestine = false;
        Ok(())
    }
}

#[async_trait]
impl<P, F> FoldTileAccu for TileAccumulator<P, F>
where
    P: Pixel,
    F: TemporalRasterPixelAggregator<P>,
{
    type RasterType = P;

    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>> {
        let TileAccumulator {
            time,
            tile_position,
            global_geo_transform,
            state_grid,
            prestine: _,
            pool: _pool,
        } = self;

        Ok(RasterTile2D::new(
            time,
            tile_position,
            global_geo_transform,
            F::into_grid(state_grid)?,
        ))
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

/// A subquery that aggregates a time series of tiles.
#[derive(Debug, Clone)]
pub struct TemporalRasterAggregationSubQuery<FoldFn, P: Pixel, F: TemporalRasterPixelAggregator<P>>
{
    pub fold_fn: FoldFn,
    pub step: TimeStep,
    pub step_reference: TimeInstance,
    pub _phantom_pixel_type: PhantomData<(P, F)>,
}

impl<'a, P, F, FoldM, FoldF> SubQueryTileAggregator<'a, P>
    for TemporalRasterAggregationSubQuery<FoldM, P, F>
where
    P: Pixel,
    F: TemporalRasterPixelAggregator<P> + 'static,
    FoldM: Send + Sync + 'static + Clone + Fn(TileAccumulator<P, F>, RasterTile2D<P>) -> FoldF,
    FoldF: Send + TryFuture<Ok = TileAccumulator<P, F>, Error = crate::error::Error>,
{
    type TileAccu = TileAccumulator<P, F>;
    type TileAccuFuture = futures::future::Ready<Result<Self::TileAccu>>;

    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        let accu = TileAccumulator {
            time: query_rect.time_interval,
            tile_position: tile_info.global_tile_position,
            global_geo_transform: tile_info.global_geo_transform,
            state_grid: EmptyGrid2D::new(tile_info.tile_size_in_pixels).into(),
            prestine: true,
            pool: pool.clone(),
        };

        futures::future::ok(accu)
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        let snapped_start = self.step.snap_relative(self.step_reference, start_time)?;
        Ok(Some(RasterQueryRectangle {
            spatial_bounds: tile_info.spatial_partition(),
            spatial_resolution: query_rect.spatial_resolution,
            time_interval: TimeInterval::new(snapped_start, (snapped_start + self.step)?)?,
        }))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}
