use crate::engine::{QueryContext, RasterQueryProcessor, SubQueryHandout, SubQueryParallelism};
use crate::error;
use crate::util::Result;
use async_trait::async_trait;
use futures::future::{BoxFuture, TryFutureExt};
use futures::stream::{BoxStream, FuturesOrdered, TryStreamExt};
use futures::{Future, FutureExt, Stream, StreamExt};
use geoengine_datatypes::primitives::{
    BandSelectionIter, CacheHint, RasterQueryRectangle, TimeInterval,
};
use geoengine_datatypes::raster::{
    GridOrEmpty, Pixel, RasterTile2D, TileInformation, TileInformationBandCrossProductIter,
    TilingStrategy,
};
use rayon::ThreadPool;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

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

/// Phantom type pinning the generic params of [`RasterSubQueryAdapter`] unused in fields.
/// `fn() ->` keeps the marker covariant and unconditionally `Send` + `Sync`.
type AdapterMarker<R, S, T> = fn() -> (R, S, T);

/// Generates tiles by running ordered sub-queries, concurrently while the query's
/// [`SubQueryParallelism`] has permits left.
pub struct RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery, TimeStream>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<'a, PixelType>,
    TimeStream: Stream<Item = Result<TimeInterval>>,
{
    stream: BoxStream<'a, Result<RasterTile2D<PixelType>>>,
    _marker: std::marker::PhantomData<AdapterMarker<RasterProcessorType, SubQuery, TimeStream>>,
}

impl<'a, PixelType, RasterProcessor, SubQuery, TimeStream>
    RasterSubQueryAdapter<'a, PixelType, RasterProcessor, SubQuery, TimeStream>
where
    PixelType: Pixel,
    RasterProcessor: RasterQueryProcessor<RasterType = PixelType> + 'a,
    SubQuery: SubQueryTileAggregator<'a, PixelType> + Sync + 'a,
    TimeStream: Stream<Item = Result<TimeInterval>> + Send + 'a,
{
    /// Creates a new adapter.
    ///
    /// # Panics
    ///
    /// The returned stream unwraps a time interval it sets itself in the
    /// generation loop, so it only panics if that internal invariant is broken.
    pub fn new(
        source_processor: &'a RasterProcessor,
        query_rect_to_answer: RasterQueryRectangle,
        tiling_strategy: TilingStrategy,
        query_ctx: &'a dyn QueryContext,
        sub_query: SubQuery,
        time_stream: TimeStream,
    ) -> Self {
        let descriptor_query = query_rect_to_answer.clone();
        let descriptor_bands = query_rect_to_answer.attributes().clone();
        let sub_query = Arc::new(sub_query);
        let descriptors = futures::stream::try_unfold(
            (
                time_stream.boxed(),
                None::<TileInformationBandCrossProductIter>,
                None::<TimeInterval>,
            ),
            move |(mut time_stream, mut band_tile_iter, mut time)| {
                let descriptor_query = descriptor_query.clone();
                let descriptor_bands = descriptor_bands.clone();
                async move {
                    loop {
                        if let Some(iter) = band_tile_iter.as_mut()
                            && let Some((tile, band)) = iter.next() {
                                return Ok(Some((
                                    (time.expect("time is always set before `band_tile_iter` is produced in the same loop iteration"), tile, band),
                                    (time_stream, band_tile_iter, time),
                                )));
                            }

                        let next_time = time_stream.next().await.transpose()?;
                        let Some(next_time) = next_time else {
                            return Ok(None);
                        };
                        time = Some(next_time);
                        let tile_iter = tiling_strategy
                            .tile_information_iterator_from_pixel_bounds(
                                descriptor_query.spatial_bounds(),
                            );
                        band_tile_iter = Some(TileInformationBandCrossProductIter::new(
                            tile_iter,
                            BandSelectionIter::new(descriptor_bands.clone()),
                        ));
                    }
                }
            },
        )
        .map_ok(move |(time, tile, band)| {
            process_tile(
                source_processor,
                query_ctx,
                sub_query.clone(),
                query_rect_to_answer.clone(),
                time,
                tile,
                band,
            )
            .boxed()
        })
        .boxed();

        Self {
            stream: ParallelSubQueries {
                descriptors,
                in_flight: FuturesOrdered::new(),
                parallelism: query_ctx.subquery_parallelism(),
                done: false,
            }
            .boxed(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn box_pin(self) -> BoxStream<'a, Result<RasterTile2D<PixelType>>> {
        self.stream
    }
}

impl<'a, PixelType, RasterProcessor, SubQuery, TimeStream> Stream
    for RasterSubQueryAdapter<'a, PixelType, RasterProcessor, SubQuery, TimeStream>
where
    PixelType: Pixel,
    RasterProcessor: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<'a, PixelType> + Sync + 'a,
    TimeStream: Stream<Item = Result<TimeInterval>> + Send + 'a,
{
    type Item = Result<RasterTile2D<PixelType>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().stream.poll_next_unpin(cx)
    }
}

type TileWithHandout<P> = (RasterTile2D<P>, SubQueryHandout);

/// Runs sub-queries in order, each with a [`SubQueryHandout`] from the query's
/// [`SubQueryParallelism`]: the first running one is always handed out, so every adapter
/// makes progress; parallel ones only while permits are free, so nested adapters share one
/// pool and the outermost adapter (which asks first) gets most of the parallelism.
///
/// A handout is returned when its tile is yielded, not when the sub-query finishes, so
/// finished tiles waiting behind a slow one still count against the available parallelism.
struct ParallelSubQueries<'a, P: Pixel> {
    descriptors: BoxStream<'a, Result<BoxFuture<'a, Result<RasterTile2D<P>>>>>,
    in_flight: FuturesOrdered<BoxFuture<'a, Result<TileWithHandout<P>>>>,
    parallelism: &'a SubQueryParallelism,
    done: bool,
}

impl<P: Pixel> Stream for ParallelSubQueries<'_, P> {
    type Item = Result<RasterTile2D<P>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        while !this.done {
            let Some(handout) = this.parallelism.handout(this.in_flight.len()) else {
                break;
            };

            match this.descriptors.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(sub_query))) => this
                    .in_flight
                    .push_back(sub_query.map_ok(|tile| (tile, handout)).boxed()),
                Poll::Ready(Some(Err(e))) => {
                    // queued behind the running sub-queries to keep the output order
                    this.in_flight
                        .push_back(futures::future::ready(Err(e)).boxed());
                    this.done = true;
                }
                Poll::Ready(None) => this.done = true,
                Poll::Pending => break,
            }
        }

        match this.in_flight.poll_next_unpin(cx) {
            Poll::Ready(Some(Err(e))) => {
                this.done = true;
                this.in_flight = FuturesOrdered::new();
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(Some(Ok((tile, _handout)))) => Poll::Ready(Some(Ok(tile))),
            Poll::Ready(None) if !this.done => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

async fn process_tile<'a, P, R, S>(
    source_processor: &'a R,
    query_ctx: &'a dyn QueryContext,
    sub_query: Arc<S>,
    query_rect: RasterQueryRectangle,
    time: TimeInterval,
    tile: TileInformation,
    band: u32,
) -> Result<RasterTile2D<P>>
where
    P: Pixel,
    R: RasterQueryProcessor<RasterType = P>,
    S: SubQueryTileAggregator<'a, P> + Sync,
{
    let Some(raster_query_rect) = sub_query.tile_query_rectangle(tile, query_rect, time, band)?
    else {
        return Ok(RasterTile2D::new_with_tile_info(
            time,
            tile,
            band,
            GridOrEmpty::new_empty_shape(tile.tile_size_in_pixels),
            CacheHint::max_duration(),
        ));
    };

    let (query, accu) = futures::try_join!(
        source_processor.raster_query(raster_query_rect.clone(), query_ctx),
        sub_query.new_fold_accu(tile, raster_query_rect, query_ctx.thread_pool()),
    )?;
    let accu = query.try_fold(accu, sub_query.fold_method()).await?;
    let mut result = accu.into_tile().await?;
    result.band = band;
    Ok(result)
}

pub trait SubQueryTileAggregator<'a, T>: Send + 'a
where
    T: Pixel,
{
    type FoldFuture: Send + futures::TryFuture<Ok = Self::TileAccu, Error = error::Error>;
    type FoldMethod: 'a
        + Send
        + Sync
        + Clone
        + Fn(Self::TileAccu, RasterTile2D<T>) -> Self::FoldFuture;
    type TileAccu: FoldTileAccu<RasterType = T> + Clone + Send;
    type TileAccuFuture: Send + Future<Output = Result<Self::TileAccu>>;

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture;

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
        G: Stream<Item = Result<TimeInterval>> + Send + 'a,
        Self: Sized + Sync,
    {
        RasterSubQueryAdapter::new(source, query, tiling_strategy, ctx, self, time_stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        BoxRasterQueryProcessor, MockExecutionContext, MockQueryContext, QueryProcessor,
        RasterBandDescriptors, RasterResultDescriptor, SpatialGridDescriptor, TimeDescriptor,
    };
    use futures::future::{Ready, ready};
    use geoengine_datatypes::primitives::{BandSelection, Coordinate2D, TimeStep};
    use geoengine_datatypes::raster::{
        BoundedGrid, GeoTransform, GridBoundingBox2D, GridIdx2D, GridShape, SpatialGridDefinition,
        TilingSpecification,
    };
    use geoengine_datatypes::util::test::TestDefault;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn descriptor(size: usize) -> RasterResultDescriptor {
        RasterResultDescriptor {
            data_type: geoengine_datatypes::raster::RasterDataType::U8,
            spatial_reference: geoengine_datatypes::spatial_reference::SpatialReference::epsg_4326(
            )
            .into(),
            time: TimeDescriptor::new_regular_with_epoch(None, TimeStep::millis(5).unwrap()),
            spatial_grid: SpatialGridDescriptor::new_source(SpatialGridDefinition::new(
                GeoTransform::new(Coordinate2D::new(0., size as f64), 1., -1.).unwrap(),
                GridShape::new_2d(size, size).bounding_box(),
            )),
            bands: RasterBandDescriptors::new_single_band(),
        }
    }

    #[derive(Default)]
    struct Concurrency {
        started: AtomicUsize,
        active: AtomicUsize,
        max: AtomicUsize,
    }

    /// Counts how many queries are running at the same time.
    struct CountingLeaf {
        descriptor: RasterResultDescriptor,
        concurrency: Arc<Concurrency>,
    }

    #[async_trait]
    impl QueryProcessor for CountingLeaf {
        type Output = RasterTile2D<u8>;
        type ResultDescription = RasterResultDescriptor;
        type Selection = BandSelection;
        type SpatialBounds = GridBoundingBox2D;

        async fn _query<'a>(
            &'a self,
            query: RasterQueryRectangle,
            _ctx: &'a dyn QueryContext,
        ) -> Result<BoxStream<'a, Result<Self::Output>>> {
            let index = self.concurrency.started.fetch_add(1, Ordering::SeqCst);
            let active = self.concurrency.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.concurrency.max.fetch_max(active, Ordering::SeqCst);
            // staggered, so completions wake the adapter one at a time
            tokio::time::sleep(Duration::from_millis(2 + index as u64 % 4)).await;
            self.concurrency.active.fetch_sub(1, Ordering::SeqCst);

            let tile = RasterTile2D::new(
                query.time_interval(),
                [0, 0].into(),
                0,
                TestDefault::test_default(),
                GridOrEmpty::new_empty_shape([2, 2].into()),
                CacheHint::no_cache(),
            );
            Ok(futures::stream::once(ready(Ok(tile))).boxed())
        }

        fn result_descriptor(&self) -> &RasterResultDescriptor {
            &self.descriptor
        }
    }

    #[async_trait]
    impl RasterQueryProcessor for CountingLeaf {
        type RasterType = u8;

        async fn _time_query<'a>(
            &'a self,
            query: TimeInterval,
            _ctx: &'a dyn QueryContext,
        ) -> Result<BoxStream<'a, Result<TimeInterval>>> {
            Ok(futures::stream::once(ready(Ok(query))).boxed())
        }
    }

    /// One sub-query adapter level over `inner`.
    struct Nested {
        inner: BoxRasterQueryProcessor<u8>,
        descriptor: RasterResultDescriptor,
        slow_head: bool,
    }

    #[async_trait]
    impl QueryProcessor for Nested {
        type Output = RasterTile2D<u8>;
        type ResultDescription = RasterResultDescriptor;
        type Selection = BandSelection;
        type SpatialBounds = GridBoundingBox2D;

        async fn _query<'a>(
            &'a self,
            query: RasterQueryRectangle,
            ctx: &'a dyn QueryContext,
        ) -> Result<BoxStream<'a, Result<Self::Output>>> {
            let tiling_strategy = self
                .descriptor
                .tiling_grid_definition(ctx.tiling_specification())
                .generate_data_tiling_strategy();
            let time_stream = self.inner.time_query(query.time_interval(), ctx).await?;
            Ok(RasterSubQueryAdapter::new(
                &self.inner,
                query,
                tiling_strategy,
                ctx,
                IdentitySubQuery {
                    slow_head: self.slow_head,
                },
                time_stream,
            )
            .box_pin())
        }

        fn result_descriptor(&self) -> &RasterResultDescriptor {
            &self.descriptor
        }
    }

    #[async_trait]
    impl RasterQueryProcessor for Nested {
        type RasterType = u8;

        async fn _time_query<'a>(
            &'a self,
            query: TimeInterval,
            ctx: &'a dyn QueryContext,
        ) -> Result<BoxStream<'a, Result<TimeInterval>>> {
            self.inner.time_query(query, ctx).await
        }
    }

    #[derive(Clone)]
    struct IdentityAccu {
        tile: TileInformation,
        time: TimeInterval,
        pool: Arc<ThreadPool>,
    }

    #[async_trait]
    impl FoldTileAccu for IdentityAccu {
        type RasterType = u8;

        async fn into_tile(self) -> Result<RasterTile2D<u8>> {
            Ok(RasterTile2D::new_with_tile_info(
                self.time,
                self.tile,
                0,
                GridOrEmpty::new_empty_shape(self.tile.tile_size_in_pixels),
                CacheHint::no_cache(),
            ))
        }

        fn thread_pool(&self) -> &Arc<ThreadPool> {
            &self.pool
        }
    }

    struct IdentitySubQuery {
        /// building the accumulator of tile [0, 0], the first output tile, takes 50 ms
        slow_head: bool,
    }

    fn identity_fold(accu: IdentityAccu, _: RasterTile2D<u8>) -> Ready<Result<IdentityAccu>> {
        ready(Ok(accu))
    }

    impl SubQueryTileAggregator<'_, u8> for IdentitySubQuery {
        type FoldFuture = Ready<Result<IdentityAccu>>;
        type FoldMethod = fn(IdentityAccu, RasterTile2D<u8>) -> Self::FoldFuture;
        type TileAccu = IdentityAccu;
        type TileAccuFuture = BoxFuture<'static, Result<IdentityAccu>>;

        fn new_fold_accu(
            &self,
            tile: TileInformation,
            query_rect: RasterQueryRectangle,
            pool: &Arc<ThreadPool>,
        ) -> Self::TileAccuFuture {
            let delay = self.slow_head && tile.global_tile_position == [0, 0].into();
            let accu = IdentityAccu {
                tile,
                time: query_rect.time_interval(),
                pool: pool.clone(),
            };
            async move {
                if delay {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Ok(accu)
            }
            .boxed()
        }

        /// Every output tile reads the whole extent, so each inner level has several
        /// sub-queries and per-level fanout would multiply.
        fn tile_query_rectangle(
            &self,
            _tile: TileInformation,
            query_rect: RasterQueryRectangle,
            time: TimeInterval,
            band_idx: u32,
        ) -> Result<Option<RasterQueryRectangle>> {
            Ok(Some(RasterQueryRectangle::new(
                query_rect.spatial_bounds(),
                time,
                band_idx.into(),
            )))
        }

        fn fold_method(&self) -> Self::FoldMethod {
            identity_fold
        }
    }

    /// `depth` nested adapters over a counting leaf on a `size`×`size` raster of 2×2 tiles.
    fn nested(
        depth: usize,
        size: usize,
        slow_head: bool,
    ) -> (BoxRasterQueryProcessor<u8>, Arc<Concurrency>) {
        let concurrency = Arc::new(Concurrency::default());
        let mut processor: BoxRasterQueryProcessor<u8> = CountingLeaf {
            descriptor: descriptor(size),
            concurrency: concurrency.clone(),
        }
        .boxed();
        for _ in 0..depth {
            processor = Nested {
                inner: processor,
                descriptor: descriptor(size),
                slow_head,
            }
            .boxed();
        }
        (processor, concurrency)
    }

    fn query_ctx(parallelism: usize) -> MockQueryContext {
        let exe_ctx =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([2, 2].into()));
        let mut query_ctx = exe_ctx.mock_query_context_test_default();
        query_ctx.subquery_parallelism = SubQueryParallelism::fixed(parallelism);
        query_ctx
    }

    fn query(size: isize) -> RasterQueryRectangle {
        RasterQueryRectangle::new(
            GridBoundingBox2D::new([0, 0], [size - 1, size - 1]).unwrap(),
            TimeInterval::new_unchecked(0, 5),
            BandSelection::first(),
        )
    }

    /// Runs `depth` nested adapters over a counting leaf, returning the output tile
    /// positions and the highest number of leaf queries that ran at once.
    async fn run_nested(depth: usize, parallelism: usize) -> (Vec<GridIdx2D>, usize) {
        let (processor, concurrency) = nested(depth, 4, false);
        let query_ctx = query_ctx(parallelism);
        let tiles: Vec<_> = processor
            .raster_query(query(4), &query_ctx)
            .await
            .unwrap()
            .map(|tile| tile.unwrap().tile_position)
            .collect()
            .await;

        (tiles, concurrency.max.load(Ordering::SeqCst))
    }

    #[tokio::test]
    async fn it_runs_sequentially_with_parallelism_one() {
        let (tiles, max) = run_nested(2, 1).await;
        assert_eq!(tiles.len(), 4);
        assert_eq!(max, 1);
    }

    #[tokio::test]
    async fn it_does_not_multiply_fanout_when_nested() {
        let (sequential, _) = run_nested(3, 1).await;
        let (tiles, max) = run_nested(3, 4).await;

        assert_eq!(tiles, sequential);
        // per-level fanout would run up to 4^3 leaf queries at once
        assert!(max > 1, "expected concurrent sub-queries, got {max}");
        assert!(max <= 4, "fanout multiplied across levels: {max}");
    }

    #[tokio::test]
    async fn it_bounds_finished_tiles_waiting_behind_a_slow_one() {
        let (processor, concurrency) = nested(1, 8, true);
        let query_ctx = query_ctx(3);
        let mut tiles = processor.raster_query(query(8), &query_ctx).await.unwrap();

        tiles.next().await.unwrap().unwrap();

        // the slow head plus 2 permits; releasing permits on completion instead of on
        // yield would keep starting sub-queries while the head is still running
        let started = concurrency.started.load(Ordering::SeqCst);
        assert!(
            started <= 3,
            "{started} sub-queries started behind the slow head"
        );
    }

    #[tokio::test]
    async fn it_does_not_deadlock_with_little_parallelism() {
        let result = tokio::time::timeout(Duration::from_secs(10), run_nested(3, 2)).await;
        assert_eq!(result.expect("nested adapters deadlocked").0.len(), 4);
    }
}
