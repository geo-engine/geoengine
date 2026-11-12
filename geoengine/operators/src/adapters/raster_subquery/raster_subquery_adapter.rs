use crate::engine::{
    Footprint, FootprintBounds, FootprintPrecision, QueryContext, RasterQueryProcessor,
};
use crate::error;
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{Future, stream::FusedStream};
use futures::{
    FutureExt, TryFuture, TryStreamExt, ready,
    stream::{BoxStream, TryFold},
};
use futures::{Stream, StreamExt};
use geoengine_datatypes::primitives::RasterQueryRectangle;
use geoengine_datatypes::primitives::TimeInterval;
use geoengine_datatypes::primitives::{BandSelectionIter, CacheHint};
use geoengine_datatypes::raster::{
    GridBoundingBox2D, GridIntersection, GridOrEmpty, TileInformationBandCrossProductIter,
    TilingSpecification, TilingStrategy,
};
use geoengine_datatypes::raster::{Pixel, RasterTile2D, TileInformation};
use rayon::ThreadPool;
use std::pin::Pin;

use pin_project::pin_project;

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
    /// The pixel bounds of the source data. Sub-queries outside of it produce empty tiles without querying the source.
    source_data_bounds: GridBoundingBox2D,
    /// The tiles of the query, to find the area of the source they read
    tiling_strategy: TilingStrategy,

    /// The `SubQuery` defines what this adapter does.
    sub_query: SubQuery,

    /// This `TimeInterval` is the time currently worked on
    current_time: TimeInterval,

    /// Where the source has data per time step, in step with the time stream.
    footprint_stream: Option<BoxStream<'a, Result<Footprint>>>,
    /// An answer of the footprint stream that waits for its time step.
    next_footprint: Option<Footprint>,
    /// Where the source has data in the current time step.
    current_bounds: FootprintBounds,

    band_tile_iter: TileInformationBandCrossProductIter,

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
        let band_tile_iter = TileInformationBandCrossProductIter::new(tile_iter, band_iter);
        let source_data_bounds = source_processor
            .raster_result_descriptor()
            .spatial_grid_descriptor()
            .tiling_grid_definition(TilingSpecification::new(
                tiling_strategy.tile_size_in_pixels,
            ))
            .tiling_spatial_grid_definition()
            .grid_bounds();

        Self {
            current_time: TimeInterval::default(), // This is overwritten in the first poll_next call!
            footprint_stream: None,
            next_footprint: None,
            current_bounds: FootprintBounds::Unknown,
            query_rect_to_answer,
            source_data_bounds,
            tiling_strategy,
            band_tile_iter,
            query_ctx,
            source_processor,
            state: StateInner::CreateNextTime,
            sub_query,
            time_stream,
        }
    }

    /// Asks the source where it has data in the area the tiles of this query read, and skips the tiles
    /// without. Does nothing unless the query context turns footprint skipping on.
    ///
    /// Only helps sub-queries that read the time step they produce: a time step that is not a time step
    /// of the source is not skipped.
    pub async fn skip_tiles_without_data(self) -> Result<Self> {
        if !self.query_ctx.footprint_skipping() {
            return Ok(self);
        }
        let Some(source_query) = self.source_query_rectangle()? else {
            return Ok(self);
        };
        let footprints = self
            .source_processor
            .footprint_query(source_query, FootprintPrecision::Coverage, self.query_ctx)
            .await?;
        Ok(self.with_footprints(footprints))
    }

    /// Skips the tiles where `footprints` have no data. They must be the footprints of the source in its
    /// pixel space. A time step of the time stream that differs from their time steps is not skipped.
    #[must_use]
    pub fn with_footprints(mut self, footprints: BoxStream<'a, Result<Footprint>>) -> Self {
        self.footprint_stream = Some(footprints);
        self
    }

    /// The area of the source that the tiles of the query together read, for all bands and the whole
    /// time of the query. `None` if no tile reads the source.
    fn source_query_rectangle(&self) -> Result<Option<RasterQueryRectangle>> {
        let query = &self.query_rect_to_answer;
        let Some(band) = query.attributes().as_slice().first().copied() else {
            return Ok(None);
        };

        let mut hull: Option<GridBoundingBox2D> = None;
        for tile in self
            .tiling_strategy
            .tile_information_iterator_from_pixel_bounds(query.spatial_bounds())
        {
            // the sub-queries of a tile differ in time and band, not in space
            let Some(tile_query) = self.sub_query.tile_query_rectangle(
                tile,
                query.clone(),
                query.time_interval(),
                band,
            )?
            else {
                continue;
            };
            let bounds = tile_query.spatial_bounds();
            hull = Some(match hull {
                None => bounds,
                Some(hull) => GridBoundingBox2D::new_unchecked(
                    [
                        hull.y_min().min(bounds.y_min()),
                        hull.x_min().min(bounds.x_min()),
                    ],
                    [
                        hull.y_max().max(bounds.y_max()),
                        hull.x_max().max(bounds.x_max()),
                    ],
                ),
            });
        }

        Ok(hull.map(|bounds| {
            RasterQueryRectangle::new(bounds, query.time_interval(), query.attributes().clone())
        }))
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
            // the answer waits here if the time stream is not ready, so it is not lost
            if this.next_footprint.is_none()
                && let Some(footprints) = this.footprint_stream.as_mut()
            {
                match ready!(footprints.poll_next_unpin(cx)) {
                    Some(Ok(footprint)) => *this.next_footprint = Some(footprint),
                    Some(Err(e)) => {
                        this.state.set(StateInner::Ended);
                        return Poll::Ready(Some(Err(e)));
                    }
                    None => *this.footprint_stream = None,
                }
            }

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
                    *this.current_bounds = match this.next_footprint.take() {
                        Some(footprint) if footprint.time == time => footprint.bounds,
                        // the steps differ, so the footprints cannot be trusted from here on
                        Some(_) => {
                            *this.footprint_stream = None;
                            FootprintBounds::Unknown
                        }
                        None => FootprintBounds::Unknown,
                    };
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
                Ok(Some(raster_query_rect))
                    if raster_query_rect
                        .spatial_bounds()
                        .intersection(this.source_data_bounds)
                        .is_some()
                        && this
                            .current_bounds
                            .intersects(&raster_query_rect.spatial_bounds()) =>
                {
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
                // no query or no source data => produce an empty tile
                Ok(Some(_) | None) => this.state.set(StateInner::ReturnResult {
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
