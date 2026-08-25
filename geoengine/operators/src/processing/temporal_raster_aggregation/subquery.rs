use crate::{
    adapters::{FoldTileAccu, SubQueryTileAggregator},
    util::{
        Result,
        pixel_aggregation::{PixelAggregator, finish_states, values_to_grid},
        spawn_blocking_with_thread_pool,
    },
};
use async_trait::async_trait;
use futures::TryFuture;
use geoengine_datatypes::{
    primitives::{CacheHint, RasterQueryRectangle, TimeInterval},
    raster::{
        GeoTransform, GridIndexAccess, GridOrEmpty, GridOrEmpty2D, GridShape2D, GridShapeAccess,
        GridSize, Pixel, RasterTile2D, TileIdx, TileInformation,
    },
};
use rayon::{
    ThreadPool,
    iter::{IndexedParallelIterator, IntoParallelRefMutIterator, ParallelIterator},
};
use std::{marker::PhantomData, sync::Arc};

/// A method to fold a tile into the accumulator.
pub async fn subquery_all_tiles_fold_fn<P: Pixel, A: PixelAggregator<P> + 'static>(
    accu: TileAccumulator<P, A>,
    tile: RasterTile2D<P>,
) -> Result<TileAccumulator<P, A>> {
    spawn_blocking_with_thread_pool(accu.pool.clone(), || {
        let mut accu = accu;
        accu.add_tile(tile);
        Ok(accu)
    })
    .await?
}

/// An accumulator for a time series of tiles in the same position.
pub struct TileAccumulator<P: Pixel, A: PixelAggregator<P>> {
    aggregator: Arc<A>,
    time: TimeInterval,
    tile_position: TileIdx,
    global_geo_transform: GeoTransform,
    tile_shape: GridShape2D,
    /// The states of the pixels, allocated when the first tile with data arrives
    states: Option<Vec<A::State>>,
    /// A tile without any data made all pixels no-data for an aggregator that propagates no-data
    all_no_data: bool,
    pool: Arc<ThreadPool>,
    cache_hint: CacheHint,
    _phantom_pixel_type: PhantomData<P>,
}

impl<P: Pixel, A: PixelAggregator<P>> Clone for TileAccumulator<P, A> {
    fn clone(&self) -> Self {
        Self {
            aggregator: self.aggregator.clone(),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            tile_shape: self.tile_shape,
            states: self.states.clone(),
            all_no_data: self.all_no_data,
            pool: self.pool.clone(),
            cache_hint: self.cache_hint,
            _phantom_pixel_type: PhantomData,
        }
    }
}

impl<P, A> TileAccumulator<P, A>
where
    P: Pixel,
    A: PixelAggregator<P>,
{
    /// Adds a tile to the accumulator, which represents the aggregate over a time interval that contains the tile.
    ///
    /// It must be called on the thread pool of the accumulator, since it updates the pixels in parallel.
    pub fn add_tile(&mut self, in_tile: RasterTile2D<P>) {
        // TODO: for tiles which are only partially contained in the aggregate time interval, investigate whether the pixels have to be scaled (e.g. if the pixel is a count, assume uniform distribution and divide it by the fraction of time that is contained in the aggregate)".

        // The tile must intersect the time of the query otherwise it includes wrong data
        debug_assert!(
            self.time.intersects(&in_tile.time),
            "Tile time {:?} does not intersect the accumulator/query time {:?}",
            in_tile.time,
            self.time
        );

        debug_assert_eq!(self.tile_shape, in_tile.grid_shape());

        let in_tile_grid = match in_tile.grid_array {
            GridOrEmpty::Grid(grid) => grid,
            GridOrEmpty::Empty(_) => {
                // An empty tile only makes the pixels no-data if no tile with data came before.
                // Later empty tiles are skipped, even if the aggregator propagates no-data.
                if self.states.is_none() && !self.aggregator.ignores_no_data() {
                    self.all_no_data = true;
                }
                return;
            }
        };

        self.cache_hint.merge_with(&in_tile.cache_hint);

        if self.all_no_data {
            // every pixel is no-data and will stay like this forever
            return;
        }

        let number_of_pixels = self.tile_shape.number_of_elements();
        let states = self
            .states
            .get_or_insert_with(|| vec![A::State::default(); number_of_pixels]);

        let aggregator = self.aggregator.as_ref();
        states
            .par_iter_mut()
            .enumerate()
            .for_each(|(lin_idx, state)| {
                aggregator.add(state, in_tile_grid.get_at_grid_index_unchecked(lin_idx));
            });
    }
}

#[async_trait]
impl<P, A> FoldTileAccu for TileAccumulator<P, A>
where
    P: Pixel,
    A: PixelAggregator<P> + 'static,
{
    type RasterType = P;

    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>> {
        let TileAccumulator {
            aggregator,
            time,
            tile_position,
            global_geo_transform,
            tile_shape,
            states,
            all_no_data,
            pool,
            cache_hint,
            _phantom_pixel_type,
        } = self;

        let grid: GridOrEmpty2D<P> = match states {
            Some(states) if !all_no_data => {
                spawn_blocking_with_thread_pool(pool, move || {
                    values_to_grid(tile_shape, &finish_states(aggregator.as_ref(), &states))
                })
                .await??
            }
            _ => GridOrEmpty::new_empty_shape(tile_shape),
        };

        Ok(RasterTile2D::new(
            time,
            tile_position,
            0,
            global_geo_transform,
            grid,
            cache_hint,
        ))
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

/// A subquery that aggregates a time series of tiles.
pub struct TemporalRasterAggregationSubQuery<FoldFn, P: Pixel, A: PixelAggregator<P>> {
    pub aggregator: Arc<A>,
    pub fold_fn: FoldFn,

    pub _phantom_pixel_type: PhantomData<P>,
}

impl<FoldFn: Clone, P: Pixel, A: PixelAggregator<P>> Clone
    for TemporalRasterAggregationSubQuery<FoldFn, P, A>
{
    fn clone(&self) -> Self {
        Self {
            aggregator: self.aggregator.clone(),
            fold_fn: self.fold_fn.clone(),
            _phantom_pixel_type: PhantomData,
        }
    }
}

impl<P, A, FoldM, FoldF> SubQueryTileAggregator<'_, P>
    for TemporalRasterAggregationSubQuery<FoldM, P, A>
where
    P: Pixel,
    A: PixelAggregator<P> + 'static,
    FoldM: Send + Sync + 'static + Clone + Fn(TileAccumulator<P, A>, RasterTile2D<P>) -> FoldF,
    FoldF: Send + TryFuture<Ok = TileAccumulator<P, A>, Error = crate::error::Error>,
{
    type TileAccu = TileAccumulator<P, A>;
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
            aggregator: self.aggregator.clone(),
            time: query_rect.time_interval(),
            tile_position: tile_info.tile_position,
            global_geo_transform: tile_info.global_geo_transform,
            tile_shape: tile_info.tile_size.grid_shape(),
            states: None,
            all_no_data: false,
            pool: pool.clone(),
            cache_hint: CacheHint::max_duration(),
            _phantom_pixel_type: PhantomData,
        };

        futures::future::ok(accu)
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        time: TimeInterval,
        band_idx: u32,
    ) -> Result<Option<RasterQueryRectangle>> {
        Ok(Some(RasterQueryRectangle::new(
            tile_info.core_pixel_bounds(),
            time, // The time is already snapped by the operator where the time stream is created.
            band_idx.into(),
        )))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}
