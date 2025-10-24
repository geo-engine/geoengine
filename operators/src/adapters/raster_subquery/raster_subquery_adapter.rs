use crate::engine::{QueryContext, RasterQueryProcessor};
use crate::error;
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{Future, stream::FusedStream};
use futures::{
    FutureExt, TryFuture, TryStreamExt, ready,
    stream::{BoxStream, TryFold},
};
use futures::{Stream, TryFutureExt};
use geoengine_datatypes::primitives::RasterQueryRectangle;
use geoengine_datatypes::primitives::TimeInterval;
use geoengine_datatypes::primitives::{BandSelectionIter, CacheHint};
use geoengine_datatypes::raster::{EmptyGrid2D, GridOrEmpty, TileInformationIter, TilingStrategy};
use geoengine_datatypes::raster::{Pixel, RasterTile2D, TileInformation};
use pin_project::pin_project;
use rayon::ThreadPool;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

#[async_trait]
pub trait FoldTileAccu {
    type RasterType: Pixel;
    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>>;
    fn thread_pool(&self) -> &Arc<ThreadPool>;
}

pub trait FoldTileAccuMut: FoldTileAccu {
    fn set_time(&mut self, new_time: TimeInterval);
    fn set_cache_hint(&mut self, new_cache_hint: CacheHint);
}

pub type RasterFold<'a, T, FoldFuture, FoldMethod, FoldTileAccu> =
    TryFold<BoxStream<'a, Result<RasterTile2D<T>>>, FoldFuture, FoldTileAccu, FoldMethod>;

type QueryAccuFuture<'a, T, A> = BoxFuture<'a, Result<(BoxStream<'a, Result<RasterTile2D<T>>>, A)>>;

type IntoTileFuture<'a, T> = BoxFuture<'a, Result<RasterTile2D<T>>>;

/// This adapter allows to generate a tile stream using sub-querys.
/// This is done using a `TileSubQuery`.
/// The sub-query is resolved for each produced tile.

#[derive(Clone, Debug)]
struct TileBandCrossProductIter {
    tile_iter: TileInformationIter,
    band_iter: BandSelectionIter,
    current_tile: Option<TileInformation>,
}

impl TileBandCrossProductIter {
    fn new(tile_iter: TileInformationIter, band_iter: BandSelectionIter) -> Self {
        let mut tile_iter = tile_iter;
        let current_tile = tile_iter.next();
        Self {
            tile_iter,
            band_iter,
            current_tile,
        }
    }

    fn reset(&mut self) {
        self.band_iter.reset();
        self.tile_iter.reset();
        self.current_tile = self.tile_iter.next();
    }
}

impl Iterator for TileBandCrossProductIter {
    type Item = (TileInformation, u32);

    fn next(&mut self) -> Option<Self::Item> {
        let current_t = self.current_tile;

        match (current_t, self.band_iter.next()) {
            (None, _) => None,
            (Some(t), Some(b)) => Some((t, b)),
            (Some(_t), None) => {
                self.band_iter.reset();
                self.current_tile = self.tile_iter.next();
                self.current_tile.map(|t| {
                    (
                        t,
                        self.band_iter
                            .next()
                            .expect("There must be at least one band"),
                    )
                })
            }
        }
    }
}

#[pin_project(project=StateInnerProjection)]
#[derive(Debug, Clone)]
enum StateInner<A, B, C, D> {
    ProgressTileBand {
        time: TimeInterval,
    },
    CreateNextTime,
    // PollingTime(#[pin] N), not needed?
    CreateNextQuery {
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
    },
    RunningQuery {
        #[pin]
        query_with_accu: A,
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
    },
    RunningFold {
        #[pin]
        fold: B,
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
    },
    RunningIntoTile {
        #[pin]
        into_tile: D,
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
    },
    ReturnResult {
        result: Option<C>,
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
    },
    Ended,
}

/// This type is needed to stop Clippy from complaining about a very complex type in the `RasterSubQueryAdapter` struct.
type StateInnerType<'a, P, FoldFuture, FoldMethod, TileAccu> = StateInner<
    QueryAccuFuture<'a, P, TileAccu>,
    RasterFold<'a, P, FoldFuture, FoldMethod, TileAccu>,
    RasterTile2D<P>,
    IntoTileFuture<'a, P>,
>;

/// This adapter allows to generate a tile stream using sub-querys.
/// This is done using a `TileSubQuery`.
/// The sub-query is resolved for each produced tile.
#[pin_project(project = RasterSubQueryAdapterProjection)]
pub struct RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery, TimeStream>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<'a, PixelType>,
    TimeStream: Stream<Item = Result<TimeInterval>>,
{
    /// The '`TimeStream`' providing the time steps to produce
    #[pin]
    time_stream: TimeStream,
    /// The `RasterQueryProcessor` to answer the sub-queries
    source_processor: &'a RasterProcessorType,
    /// The `QueryContext` to use for sub-queries
    query_ctx: &'a dyn QueryContext,
    /// The `QueryRectangle` the adapter is queried with
    query_rect_to_answer: RasterQueryRectangle,

    /// The `SubQuery` defines what this adapter does.
    sub_query: SubQuery,

    /// This `TimeInterval` is the time currently worked on
    current_time: TimeInterval,

    band_tile_iter: TileBandCrossProductIter,

    /// This current state of the adapter
    #[pin]
    state: StateInnerType<
        'a,
        PixelType,
        SubQuery::FoldFuture,
        SubQuery::FoldMethod,
        SubQuery::TileAccu,
    >,
}

impl<'a, PixelType, RasterProcessor, SubQuery, TimeStream>
    RasterSubQueryAdapter<'a, PixelType, RasterProcessor, SubQuery, TimeStream>
where
    PixelType: Pixel,
    RasterProcessor: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<'a, PixelType>,
    TimeStream: Stream<Item = Result<TimeInterval>>,
{
    pub fn new(
        source_processor: &'a RasterProcessor,
        query_rect_to_answer: RasterQueryRectangle,
        tiling_strategy: TilingStrategy,
        query_ctx: &'a dyn QueryContext,
        sub_query: SubQuery,
        time_stream: TimeStream,
    ) -> Self {
        let tile_iter = tiling_strategy
            .tile_information_iterator_from_pixel_bounds(query_rect_to_answer.spatial_bounds());
        let band_iter = BandSelectionIter::new(query_rect_to_answer.attributes().clone());
        let band_tile_iter = TileBandCrossProductIter::new(tile_iter, band_iter);

        Self {
            current_time: TimeInterval::default(), // This is overwritten in the first poll_next call!
            query_rect_to_answer,
            band_tile_iter,
            query_ctx,
            source_processor,
            state: StateInner::CreateNextTime,
            sub_query,
            time_stream,
        }
    }

    pub fn box_pin(self) -> BoxStream<'a, Result<RasterTile2D<PixelType>>>
    where
        SubQuery: Send + 'static,
        TimeStream: Send + 'a,
    {
        Box::pin(self)
    }
}

impl<'a, PixelType, RasterProcessorType, SubQuery, TimeStream> FusedStream
    for RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery, TimeStream>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<'a, PixelType> + 'static,
    TimeStream: Stream<Item = Result<TimeInterval>> + Send,
{
    fn is_terminated(&self) -> bool {
        matches!(self.state, StateInner::Ended)
    }
}

impl<'a, PixelType, RasterProcessorType, SubQuery, TimeStream> Stream
    for RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery, TimeStream>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<'a, PixelType> + 'static,
    TimeStream: Stream<Item = Result<TimeInterval>> + Send,
{
    type Item = Result<RasterTile2D<PixelType>>;

    /**************************************************************************************************************************************
     * This method uses the `StateInner` enum to keep track of the current state
     *
     * There are two cases aka transition flows that are valid:
     *  a) CreateNextQuery -> ReturnResult
     *  b) CreateNextQuery -> RunningQuery -> RunningFold -> ReturnResult
     *
     * In case a) a valid `QueryRectangle` for the target tile is produced and a stream is queryed and folded to produce a new tile.
     * In case b) no valid `QueryRectange` is produced. Therefore, all async steps are skipped and None is produced instead of a tile.
     *
     * When all tiles are queried the state transitions from ReturnResult to Ended.
     *
     * In case an Error occures the state is set to Ended AND the method returns Poll::Ready(Some(Err))).
     *************************************************************************************************************************************/
    #[allow(clippy::too_many_lines)]
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();

        // check if we ended in a previous call
        if matches!(*this.state, StateInner::Ended) {
            return Poll::Ready(None);
        }

        // check if we ended in a previous call
        if matches!(*this.state, StateInner::ProgressTileBand { .. }) {
            // The state is pinned. Project it to get access to the query stored in the context.
            let time = if let StateInnerProjection::ProgressTileBand { time } =
                this.state.as_mut().project()
            {
                *time
            } else {
                // we already checked that the state is `StateInner::RunningQuery` so this case can not happen.
                unreachable!()
            };

            if let Some((next_tile, next_band)) = this.band_tile_iter.next() {
                this.state.set(StateInner::CreateNextQuery {
                    time,
                    band: next_band,
                    tile: next_tile,
                });
            } else {
                this.state.set(StateInner::CreateNextTime);
            }
        }

        if matches!(*this.state, StateInner::CreateNextTime) {
            let time_future = ready!(this.time_stream.poll_next(cx));
            match time_future {
                None => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(None);
                }
                Some(Err(e)) => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(Some(Err(e)));
                }
                Some(Ok(time)) => {
                    *this.current_time = time;
                    this.band_tile_iter.reset();
                    let (tile, band) = this
                        .band_tile_iter
                        .next()
                        .expect("There must be at least one band/tile");
                    this.state
                        .set(StateInner::CreateNextQuery { time, band, tile });
                }
            }
        }

        // first generate a new query.
        if matches!(*this.state, StateInner::CreateNextQuery { .. }) {
            let (time, band, tile) =
                if let StateInnerProjection::CreateNextQuery { time, band, tile } =
                    this.state.as_mut().project()
                {
                    (*time, *band, *tile)
                } else {
                    // we already checked that the state is `StateInner::RunningQuery` so this case can not happen.
                    unreachable!()
                };

            match this.sub_query.tile_query_rectangle(
                tile,
                this.query_rect_to_answer.clone(),
                time,
                band,
            ) {
                Ok(Some(raster_query_rect)) => {
                    let tile_query_stream_fut = this
                        .source_processor
                        .raster_query(raster_query_rect.clone(), *this.query_ctx);

                    let tile_folding_accu_fut = this.sub_query.new_fold_accu(
                        tile,
                        raster_query_rect,
                        this.query_ctx.thread_pool(),
                    );

                    let joined_future =
                        async { futures::try_join!(tile_query_stream_fut, tile_folding_accu_fut) }
                            .boxed();

                    this.state.set(StateInner::RunningQuery {
                        query_with_accu: joined_future,
                        band,
                        tile,
                        time,
                    });
                }
                Ok(None) => this.state.set(StateInner::ReturnResult {
                    result: None,
                    band,
                    tile,
                    time,
                }),
                Err(e) => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        // A query was issued, so we check whether it is finished
        // To work in this scope we first check if the state is the one we expect. We want to set the state in this scope so we can not borrow it here!
        if matches!(*this.state, StateInner::RunningQuery { .. }) {
            // The state is pinned. Project it to get access to the query stored in the context.
            let (time, band, tile, query_with_accu) = if let StateInnerProjection::RunningQuery {
                query_with_accu,
                tile,
                band,
                time,
            } = this.state.as_mut().project()
            {
                (*time, *band, *tile, query_with_accu)
            } else {
                // we already checked that the state is `StateInner::RunningQuery` so this case can not happen.
                unreachable!()
            };

            match ready!(query_with_accu.poll(cx)) {
                Ok((query, tile_folding_accu)) => {
                    let tile_folding_stream =
                        query.try_fold(tile_folding_accu, this.sub_query.fold_method());

                    this.state.set(StateInner::RunningFold {
                        fold: tile_folding_stream,
                        time,
                        tile,
                        band,
                    });
                }
                Err(e) => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        // We are waiting for/expecting the result of the fold.
        // This block uses the same check and project pattern as above.
        if matches!(*this.state, StateInner::RunningFold { .. }) {
            let (tile, band, time, fold) = if let StateInnerProjection::RunningFold {
                fold,
                time,
                tile,
                band,
            } = this.state.as_mut().project()
            {
                (*tile, *band, *time, fold)
            } else {
                unreachable!()
            };

            match ready!(fold.poll(cx)) {
                Ok(tile_accu) => {
                    let into_tile = tile_accu.into_tile();
                    this.state.set(StateInner::RunningIntoTile {
                        into_tile,
                        time,
                        tile,
                        band,
                    });
                }
                Err(e) => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        // We are waiting for/expecting the result of `into_tile` method.
        // This block uses the same check and project pattern as above.
        if matches!(*this.state, StateInner::RunningIntoTile { .. }) {
            let (time, band, tile, into_tile) = if let StateInnerProjection::RunningIntoTile {
                into_tile,
                time,
                tile,
                band,
            } = this.state.as_mut().project()
            {
                (*time, *band, *tile, into_tile)
            } else {
                unreachable!()
            };

            match ready!(into_tile.poll(cx)) {
                Ok(mut result_tile) => {
                    // set the tile band to the running index, that is because output bands always start at zero and are consecutive, independent of the input bands.
                    result_tile.band = band;
                    this.state.set(StateInner::ReturnResult {
                        result: Some(result_tile),
                        time,
                        tile,
                        band,
                    });
                }
                Err(e) => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        debug_assert!(
            matches!(*this.state, StateInner::ReturnResult { .. }),
            "Must be in 'ReturnResult' state at this point!"
        );
        // At this stage we are in ReturnResult state. Either from a running fold or because the tile query rect was not valid.
        // This block uses the check and project pattern as above.
        let (tile, band, time, result) = if let StateInnerProjection::ReturnResult {
            result,
            time,
            tile,
            band,
        } = this.state.as_mut().project()
        {
            (*tile, *band, *time, result.take())
        } else {
            unreachable!()
        };
        // In the next poll we need to produce a new tile (if nothing else happens)
        this.state.set(StateInner::ProgressTileBand { time });

        let result_tile = result.unwrap_or_else(|| {
            RasterTile2D::new_with_tile_info(
                time,
                tile,
                band,
                GridOrEmpty::new_empty_shape(tile.tile_size_in_pixels),
                CacheHint::max_duration(),
            )
        });

        Poll::Ready(Some(Ok(result_tile)))
    }
}

/// This trait defines the behavior of the `RasterOverlapAdapter`.
pub trait SubQueryTileAggregator<'a, T>: Send
where
    T: Pixel,
{
    type FoldFuture: Send + TryFuture<Ok = Self::TileAccu, Error = error::Error>;
    type FoldMethod: 'a
        + Send
        + Sync
        + Clone
        + Fn(Self::TileAccu, RasterTile2D<T>) -> Self::FoldFuture;
    type TileAccu: FoldTileAccu<RasterType = T> + Clone + Send;
    type TileAccuFuture: Send + Future<Output = Result<Self::TileAccu>>;

    /// This method generates a new accumulator which is used to fold the `Stream` of `RasterTile2D` of a sub-query.
    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture;

    /// This method generates `Some(QueryRectangle)` for a tile-specific sub-query or `None` if the `query_rect` cannot be translated.
    /// In the latter case an `EmptyTile` will be produced for the sub query instead of querying the source.
    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        time: TimeInterval,
        band_idx: u32,
    ) -> Result<Option<RasterQueryRectangle>> {
        Ok(Some(RasterQueryRectangle::new(
            tile_info.global_pixel_bounds(),
            time,
            band_idx.into(),
        )))
    }

    /// This method generates the method which combines the accumulator and each tile of the sub-query stream in the `TryFold` stream adapter.
    fn fold_method(&self) -> Self::FoldMethod;

    fn into_raster_subquery_adapter<S, G>(
        self,
        source: &'a S,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
        tiling_strategy: TilingStrategy,
        time_stream: G,
    ) -> RasterSubQueryAdapter<'a, T, S, Self, G>
    where
        S: RasterQueryProcessor<RasterType = T>,
        G: Stream<Item = Result<TimeInterval>>,
        Self: Sized,
    {
        RasterSubQueryAdapter::<'a, T, S, Self, G>::new(
            source,
            query,
            tiling_strategy,
            ctx,
            self,
            time_stream,
        )
    }
}

#[derive(Clone, Debug)]
pub struct RasterTileAccu2D<T> {
    pub tile: RasterTile2D<T>,
    pub pool: Arc<ThreadPool>,
}

impl<T> RasterTileAccu2D<T> {
    pub fn new(tile: RasterTile2D<T>, pool: Arc<ThreadPool>) -> Self {
        RasterTileAccu2D { tile, pool }
    }
}

#[async_trait]
impl<T: Pixel> FoldTileAccu for RasterTileAccu2D<T> {
    type RasterType = T;

    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>> {
        Ok(self.tile)
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

impl<T: Pixel> FoldTileAccuMut for RasterTileAccu2D<T> {
    fn set_time(&mut self, new_time: TimeInterval) {
        self.tile.time = new_time;
    }

    fn set_cache_hint(&mut self, new_cache_hint: CacheHint) {
        self.tile.cache_hint = new_cache_hint;
    }
}

#[derive(Debug, Clone)]
pub struct TileSubQueryIdentity<F, T> {
    fold_fn: F,
    _phantom_pixel_type: PhantomData<T>,
}

impl<'a, T, FoldM, FoldF> SubQueryTileAggregator<'a, T> for TileSubQueryIdentity<FoldM, T>
where
    T: Pixel,
    FoldM: Send + Sync + 'a + Clone + Fn(RasterTileAccu2D<T>, RasterTile2D<T>) -> FoldF,
    FoldF: Send + TryFuture<Ok = RasterTileAccu2D<T>, Error = error::Error>,
{
    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    type TileAccu = RasterTileAccu2D<T>;
    type TileAccuFuture = BoxFuture<'a, Result<Self::TileAccu>>;

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        identity_accu(tile_info, &query_rect, pool.clone()).boxed()
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        time: TimeInterval,
        band_idx: u32,
    ) -> Result<Option<RasterQueryRectangle>> {
        Ok(Some(RasterQueryRectangle::new(
            tile_info.global_pixel_bounds(),
            time,
            band_idx.into(),
        )))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

pub fn identity_accu<T: Pixel>(
    tile_info: TileInformation,
    query_rect: &RasterQueryRectangle,
    pool: Arc<ThreadPool>,
) -> impl Future<Output = Result<RasterTileAccu2D<T>>> + use<T> {
    let time_interval = query_rect.time_interval();
    crate::util::spawn_blocking(move || {
        let output_raster = EmptyGrid2D::new(tile_info.tile_size_in_pixels).into();
        let output_tile = RasterTile2D::new_with_tile_info(
            time_interval,
            tile_info,
            0,
            output_raster,
            CacheHint::max_duration(),
        );
        RasterTileAccu2D::new(output_tile, pool)
    })
    .map_err(From::from)
}
