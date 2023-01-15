use crate::adapters::SparseTilesFillAdapter;
use crate::engine::{QueryContext, QueryProcessor, RasterQueryProcessor};
use crate::error;
use crate::util::Result;
use futures::future::BoxFuture;
use futures::{
    ready,
    stream::{BoxStream, TryFold},
    FutureExt, TryFuture, TryStreamExt,
};
use futures::{stream::FusedStream, Future};
use futures::{Stream, StreamExt, TryFutureExt};
use geoengine_datatypes::primitives::{
    RasterQueryRectangle, RasterSpatialQueryRectangle, SpatialQuery,
};
use geoengine_datatypes::raster::{
    EmptyGrid2D, GridBoundingBox2D, GridBounds, GridStep, TilingStrategy,
};
use geoengine_datatypes::{
    primitives::TimeInstance,
    raster::{Blit, Pixel, RasterTile2D, TileInformation},
};
use geoengine_datatypes::{primitives::TimeInterval, raster::TilingSpecification};

use pin_project::pin_project;
use rayon::ThreadPool;

use std::marker::PhantomData;
use std::sync::Arc;
use std::task::Poll;

use std::pin::Pin;

use async_trait::async_trait;

#[async_trait]
pub trait FoldTileAccu {
    type RasterType: Pixel;
    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>>;
    fn thread_pool(&self) -> &Arc<ThreadPool>;
}

pub trait FoldTileAccuMut: FoldTileAccu {
    fn tile_mut(&mut self) -> &mut RasterTile2D<Self::RasterType>;
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
    CreateNextQuery,
    RunningQuery {
        #[pin]
        query_with_accu: A,
    },
    RunningFold(#[pin] B),
    RunningIntoTile(#[pin] D),
    ReturnResult(Option<C>),
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
pub struct RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<'a, PixelType>,
{
    /// The `RasterQueryProcessor` to answer the sub-queries
    source_processor: &'a RasterProcessorType,
    /// The `QueryContext` to use for sub-queries
    query_ctx: &'a dyn QueryContext,
    /// The `QueryRectangle` the adapter is queried with
    query_rect_to_answer: RasterQueryRectangle,
    /// The `GridBoundingBox2D` that defines the tile grid space of the query.
    grid_bounds: GridBoundingBox2D,

    /// The `SubQuery` defines what this adapter does.
    sub_query: SubQuery,

    /// This `TimeInterval` is the time currently worked on
    current_time_start: TimeInstance,
    current_time_end: Option<TimeInstance>,
    /// The `GridIdx2D` currently worked on
    current_tile_spec: TileInformation,

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

impl<'a, PixelType, RasterProcessor, SubQuery>
    RasterSubQueryAdapter<'a, PixelType, RasterProcessor, SubQuery>
where
    PixelType: Pixel,
    RasterProcessor: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<'a, PixelType>,
{
    pub fn new(
        source_processor: &'a RasterProcessor,
        query_rect_to_answer: RasterQueryRectangle,
        tiling_spec: TilingSpecification,
        query_ctx: &'a dyn QueryContext,
        sub_query: SubQuery,
    ) -> Self {
        // FIXME: we should not need to create a new tiling strategy here
        let tiling_strat = TilingStrategy::new(
            tiling_spec.tile_size_in_pixels,
            query_rect_to_answer.spatial_query().geo_transform,
        );

        let grid_bounds = tiling_strat
            .raster_spatial_query_to_tling_grid_box(&query_rect_to_answer.spatial_query());

        let first_tile_spec = TileInformation {
            global_geo_transform: tiling_strat.geo_transform,
            global_tile_position: grid_bounds.min_index(),
            tile_size_in_pixels: tiling_strat.tile_size_in_pixels,
        };

        Self {
            current_tile_spec: first_tile_spec,
            current_time_end: None,
            current_time_start: query_rect_to_answer.time_interval.start(),
            grid_bounds,
            query_ctx,
            query_rect_to_answer,
            source_processor,
            state: StateInner::CreateNextQuery,
            sub_query,
        }
    }

    /// Wrap the `RasterSubQueryAdapter` with a filter and a `SparseTilesFillAdapter` to produce a `Stream` compatible with `RasterQueryProcessor`.
    pub fn filter_and_fill(self) -> BoxStream<'a, Result<RasterTile2D<PixelType>>>
    where
        Self: Stream<Item = Result<Option<RasterTile2D<PixelType>>>> + 'a,
    {
        let grid_bounds = self.grid_bounds;
        let global_geo_transform = self.current_tile_spec.global_geo_transform;
        let tile_shape = self.current_tile_spec.tile_size_in_pixels;

        let s = self.filter_map(|x| async move {
            match x {
                Ok(Some(t)) => Some(Ok(t)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        });

        let s_filled =
            SparseTilesFillAdapter::new(s, grid_bounds, global_geo_transform, tile_shape);
        s_filled.boxed()
    }

    /// Wrap `RasterSubQueryAdapter` to flatten the inner option.
    ///
    /// SAFETY: This call will cause panics if there is a None result!
    pub(crate) fn expect(self, msg: &'static str) -> BoxStream<'a, Result<RasterTile2D<PixelType>>>
    where
        Self: Stream<Item = Result<Option<RasterTile2D<PixelType>>>> + 'a,
    {
        self.map(|r| r.map(|o| o.expect(msg))).boxed()
    }
}

impl<'a, PixelType, RasterProcessorType, SubQuery> FusedStream
    for RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType: QueryProcessor<
        Output = RasterTile2D<PixelType>,
        SpatialQuery = RasterSpatialQueryRectangle,
    >,
    SubQuery: SubQueryTileAggregator<'a, PixelType> + 'static,
{
    fn is_terminated(&self) -> bool {
        matches!(self.state, StateInner::Ended)
    }
}

impl<'a, PixelType, RasterProcessorType, SubQuery> Stream
    for RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType: QueryProcessor<
        Output = RasterTile2D<PixelType>,
        SpatialQuery = RasterSpatialQueryRectangle,
    >,
    SubQuery: SubQueryTileAggregator<'a, PixelType> + 'static,
{
    type Item = Result<Option<RasterTile2D<PixelType>>>;

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

        // first generate a new query
        if matches!(*this.state, StateInner::CreateNextQuery) {
            match this.sub_query.tile_query_rectangle(
                *this.current_tile_spec,
                *this.query_rect_to_answer,
                *this.current_time_start,
            ) {
                Ok(Some(tile_query_rectangle)) => {
                    let tile_query_stream_fut = this
                        .source_processor
                        .raster_query(tile_query_rectangle, *this.query_ctx);

                    let tile_folding_accu_fut = this.sub_query.new_fold_accu(
                        *this.current_tile_spec,
                        tile_query_rectangle,
                        this.query_ctx.thread_pool(),
                    );

                    let joined_future =
                        async { futures::try_join!(tile_query_stream_fut, tile_folding_accu_fut) }
                            .boxed();

                    this.state.set(StateInner::RunningQuery {
                        query_with_accu: joined_future,
                    });
                }
                Ok(None) => this.state.set(StateInner::ReturnResult(None)),
                Err(e) => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        // A query was issued, so we check whether it is finished
        // To work in this scope we first check if the state is the one we expect. We want to set the state in this scope so we can not borrow it here!
        if matches!(*this.state, StateInner::RunningQuery { query_with_accu: _ }) {
            // The state is pinned. Project it to get access to the query stored in the context.
            let rq_res = if let StateInnerProjection::RunningQuery { query_with_accu } =
                this.state.as_mut().project()
            {
                ready!(query_with_accu.poll(cx))
            } else {
                // we already checked that the state is `StateInner::RunningQuery` so this case can not happen.
                unreachable!()
            };

            match rq_res {
                Ok((query, tile_folding_accu)) => {
                    let tile_folding_stream =
                        query.try_fold(tile_folding_accu, this.sub_query.fold_method());

                    this.state.set(StateInner::RunningFold(tile_folding_stream));
                }
                Err(e) => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(Some(Err(e)));
                }
            };
        }

        // We are waiting for/expecting the result of the fold.
        // This block uses the same check and project pattern as above.
        if matches!(*this.state, StateInner::RunningFold(_)) {
            let rf_res =
                if let StateInnerProjection::RunningFold(fold) = this.state.as_mut().project() {
                    ready!(fold.poll(cx))
                } else {
                    unreachable!()
                };

            match rf_res {
                Ok(tile_accu) => {
                    let tile = tile_accu.into_tile();
                    this.state.set(StateInner::RunningIntoTile(tile));
                }
                Err(e) => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        // We are waiting for/expecting the result of `into_tile` method.
        // This block uses the same check and project pattern as above.
        if matches!(*this.state, StateInner::RunningIntoTile(_)) {
            let rf_res = if let StateInnerProjection::RunningIntoTile(fold) =
                this.state.as_mut().project()
            {
                ready!(fold.poll(cx))
            } else {
                unreachable!()
            };

            match rf_res {
                Ok(tile) => {
                    this.state.set(StateInner::ReturnResult(Some(tile)));
                }
                Err(e) => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        // At this stage we are in ReturnResult state. Either from a running fold or because the tile query rect was not valid.
        // This block uses the check and project pattern as above.
        let tile_option = if let StateInnerProjection::ReturnResult(tile_option) =
            this.state.as_mut().project()
        {
            tile_option.take()
        } else {
            unreachable!()
        };
        // In the next poll we need to produce a new tile (if nothing else happens)
        this.state.set(StateInner::CreateNextQuery);

        // If there is a tile, set the current_time_end option.
        if let Some(tile) = &tile_option {
            debug_assert!(*this.current_time_start >= tile.time.start());
            *this.current_time_end = Some(tile.time.end());
        };

        // now do progress
        // move idx by 1
        // if the grid idx wraps around set the ne query time instance to the end time instance of the last round
        match (
            this.grid_bounds
                .inc_idx_unchecked(this.current_tile_spec.global_tile_position, 1),
            *this.current_time_end,
        ) {
            (Some(idx), _) => {
                // move the SPATIAL index further to the next tile
                this.current_tile_spec.global_tile_position = idx;
            }
            (None, None) => {
                // end the stream since we never recieved a tile from any subquery. Should only happen if we end the first grid iteration.
                // NOTE: this assumes that the input operator produces no data tiles for queries where time and space are valid but no data is avalable.
                debug_assert!(&tile_option.is_none());
                debug_assert!(
                    *this.current_time_start == this.query_rect_to_answer.time_interval.start()
                );
                this.state.set(StateInner::Ended);
            }
            (None, Some(end_time)) if end_time == *this.current_time_start => {
                // Only for time instants: reset the spatial idx to the first tile of the grid AND increase the request time by 1.
                this.current_tile_spec.global_tile_position = this.grid_bounds.min_index();
                *this.current_time_start = end_time + 1;
                *this.current_time_end = None;

                // check if the next time to request is inside the bounds we are want to answer.
                if *this.current_time_start >= this.query_rect_to_answer.time_interval.end() {
                    this.state.set(StateInner::Ended);
                }
            }
            (None, Some(end_time)) => {
                // reset the spatial idx to the first tile of the grid AND move the requested time to the last known time.
                this.current_tile_spec.global_tile_position = this.grid_bounds.min_index();
                *this.current_time_start = end_time;
                *this.current_time_end = None;

                // check if the next time to request is inside the bounds we are want to answer.
                if *this.current_time_start >= this.query_rect_to_answer.time_interval.end() {
                    this.state.set(StateInner::Ended);
                }
            }
        };

        Poll::Ready(Some(Ok(tile_option)))
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
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>>;

    /// This method generates the method which combines the accumulator and each tile of the sub-query stream in the `TryFold` stream adapter.
    fn fold_method(&self) -> Self::FoldMethod;

    fn into_raster_subquery_adapter<S>(
        self,
        source: &'a S,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
        tiling_specification: TilingSpecification,
    ) -> RasterSubQueryAdapter<'a, T, S, Self>
    where
        S: RasterQueryProcessor<RasterType = T>,
        Self: Sized,
    {
        RasterSubQueryAdapter::<'a, T, S, Self>::new(source, query, tiling_specification, ctx, self)
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
    fn tile_mut(&mut self) -> &mut RasterTile2D<T> {
        &mut self.tile
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
        identity_accu(tile_info, query_rect, pool.clone()).boxed()
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        Ok(Some(RasterQueryRectangle::with_grid_bounds_and_resolution(
            tile_info.global_pixel_bounds(),
            query_rect.spatial_query().geo_transform,
            TimeInterval::new_instant(start_time)?,
        )))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

pub fn identity_accu<T: Pixel>(
    tile_info: TileInformation,
    query_rect: RasterQueryRectangle,
    pool: Arc<ThreadPool>,
) -> impl Future<Output = Result<RasterTileAccu2D<T>>> {
    crate::util::spawn_blocking(move || {
        let output_raster = EmptyGrid2D::new(tile_info.tile_size_in_pixels).into();
        let output_tile =
            RasterTile2D::new_with_tile_info(query_rect.time_interval, tile_info, output_raster);
        RasterTileAccu2D::new(output_tile, pool)
    })
    .map_err(From::from)
}

pub fn fold_by_blit_impl<T>(
    accu: RasterTileAccu2D<T>,
    tile: RasterTile2D<T>,
) -> Result<RasterTileAccu2D<T>>
where
    T: Pixel,
{
    let mut accu_tile = accu.tile;
    let pool = accu.pool;
    let t_union = accu_tile.time.union(&tile.time)?;

    accu_tile.time = t_union;

    if tile.grid_array.is_empty() && accu_tile.grid_array.is_empty() {
        // only skip if both tiles are empty. There might be valid data in one otherwise.
        return Ok(RasterTileAccu2D::new(accu_tile, pool));
    }

    let mut materialized_tile = accu_tile.into_materialized_tile();
    materialized_tile.blit(tile)?;

    Ok(RasterTileAccu2D::new(materialized_tile.into(), pool))
}

#[allow(dead_code)]
pub fn fold_by_blit_future<T>(
    accu: RasterTileAccu2D<T>,
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<RasterTileAccu2D<T>>>
where
    T: Pixel,
{
    crate::util::spawn_blocking(|| fold_by_blit_impl(accu, tile)).then(|x| async move {
        match x {
            Ok(r) => r,
            Err(e) => Err(e.into()),
        }
    })
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{Measurement, SpatialPartition2D, SpatialResolution, TimeInterval},
        raster::{Grid, GridShape, RasterDataType},
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use crate::engine::{RasterOperator, RasterResultDescriptor};
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use futures::StreamExt;

    #[tokio::test]
    async fn identity() {
        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle::with_partition_and_resolution_and_origin(
            SpatialPartition2D::new_unchecked((0., 1.).into(), (3., 0.).into()),
            SpatialResolution::one(),
            exe_ctx.tiling_specification.origin_coordinate,
            TimeInterval::new_unchecked(0, 10),
        );

        let query_ctx = MockQueryContext::test_default();
        let tiling_strat = exe_ctx.tiling_specification;

        let op = mrs1.initialize(&exe_ctx).await.unwrap();

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let a = RasterSubQueryAdapter::new(
            &qp,
            query_rect,
            tiling_strat,
            &query_ctx,
            TileSubQueryIdentity {
                fold_fn: fold_by_blit_future,
                _phantom_pixel_type: PhantomData,
            },
        );
        let res = a
            .map(Result::unwrap)
            .map(Option::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;
        assert_eq!(data, res);
    }
}
