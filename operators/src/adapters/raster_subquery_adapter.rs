use crate::adapters::SparseTilesFillAdapter;
use crate::engine::{
    QueryContext, QueryProcessor, QueryRectangle, RasterQueryProcessor, RasterQueryRectangle,
};
use crate::error;
use crate::util::Result;
use futures::future::BoxFuture;
use futures::{
    ready,
    stream::{BoxStream, TryFold},
    FutureExt, TryFuture, TryStreamExt,
};
use futures::{stream::FusedStream, Future};
use futures::{Stream, StreamExt};
use geoengine_datatypes::primitives::{SpatialPartition2D, SpatialPartitioned};
use geoengine_datatypes::raster::{GridBoundingBox2D, GridBounds, GridStep};
use geoengine_datatypes::{
    error::Error::{GridIndexOutOfBounds, InvalidGridIndex},
    operations::reproject::{CoordinateProjection, CoordinateProjector, Reproject},
    primitives::{SpatialResolution, TimeInterval},
    raster::{
        grid_idx_iter_2d, BoundedGrid, EmptyGrid, Grid2D, MaterializedRasterTile2D, NoDataValue,
        TilingSpecification,
    },
    spatial_reference::SpatialReference,
};
use geoengine_datatypes::{
    primitives::{Coordinate2D, TimeInstance},
    raster::{
        Blit, CoordinatePixelAccess, GridIdx2D, GridIndexAccessMut, Pixel, RasterTile2D,
        TileInformation,
    },
};

use log::debug;
use pin_project::pin_project;
use std::task::Poll;

use std::pin::Pin;

pub trait FoldTileAccu {
    type RasterType: Pixel;
    fn into_tile(self) -> RasterTile2D<Self::RasterType>;
}

pub trait FoldTileAccuMut: FoldTileAccu {
    fn tile_mut(&mut self) -> &mut RasterTile2D<Self::RasterType>;
}

impl<T: Pixel> FoldTileAccu for RasterTile2D<T> {
    type RasterType = T;

    fn into_tile(self) -> RasterTile2D<Self::RasterType> {
        self
    }
}

pub type RasterFold<'a, T, FoldFuture, FoldMethod, FoldTileAccu> =
    TryFold<BoxStream<'a, Result<RasterTile2D<T>>>, FoldFuture, FoldTileAccu, FoldMethod>;

type QueryFuture<'a, T> = BoxFuture<'a, Result<BoxStream<'a, Result<RasterTile2D<T>>>>>;

/// This adapter allows to generate a tile stream using sub-querys.
/// This is done using a `TileSubQuery`.
/// The sub-query is resolved for each produced tile.

#[pin_project(project=StateInnerProjection)]
#[derive(Debug, Clone)]
enum StateInner<A, B, C> {
    CreateNextQuery,
    RunningQuery {
        #[pin]
        query: A,
        query_rect: QueryRectangle<SpatialPartition2D>,
    },
    RunningFold(#[pin] B),
    ReturnResult(Option<C>),
    Ended,
}

/// This type is needed to stop Clippy from complaining about a very complex type in the `RasterSubQueryAdapter` struct.
type StateInnerType<'a, P, FoldFuture, FoldMethod, TileAccu> = StateInner<
    QueryFuture<'a, P>,
    RasterFold<'a, P, FoldFuture, FoldMethod, TileAccu>,
    RasterTile2D<P>,
>;

/// This adapter allows to generate a tile stream using sub-querys.
/// This is done using a `TileSubQuery`.
/// The sub-query is resolved for each produced tile.
#[pin_project(project = RasterSubQueryAdapterProjection)]
pub struct RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<PixelType>,
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
    SubQuery: SubQueryTileAggregator<PixelType>,
{
    pub fn new(
        source_processor: &'a RasterProcessor,
        query_rect_to_answer: RasterQueryRectangle,
        tiling_spec: TilingSpecification,
        query_ctx: &'a dyn QueryContext,
        sub_query: SubQuery,
    ) -> Self {
        debug_assert!(query_rect_to_answer.spatial_resolution.y > 0.);

        let tiling_strat = tiling_spec.strategy(
            query_rect_to_answer.spatial_resolution.x,
            -query_rect_to_answer.spatial_resolution.y,
        );

        let grid_bounds = tiling_strat.tile_grid_box(query_rect_to_answer.spatial_partition());

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
    pub fn filter_and_fill(
        self,
        no_data_value: PixelType,
    ) -> BoxStream<'a, Result<RasterTile2D<PixelType>>>
    where
        Self: Stream<Item = Result<Option<RasterTile2D<PixelType>>>> + 'a,
    {
        let grid_bounds = self.grid_bounds.clone();
        let global_geo_transform = self.current_tile_spec.global_geo_transform;
        let tile_shape = self.current_tile_spec.tile_size_in_pixels;

        let s = self.filter_map(async move |x| match x {
            Ok(Some(t)) => Some(Ok(t)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        });

        let s_filled = SparseTilesFillAdapter::new(
            s,
            grid_bounds,
            global_geo_transform,
            tile_shape,
            no_data_value,
        );
        s_filled.boxed()
    }
}

impl<PixelType, RasterProcessorType, SubQuery> FusedStream
    for RasterSubQueryAdapter<'_, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType:
        QueryProcessor<Output = RasterTile2D<PixelType>, SpatialBounds = SpatialPartition2D>,
    SubQuery: SubQueryTileAggregator<PixelType>,
{
    fn is_terminated(&self) -> bool {
        matches!(self.state, StateInner::Ended)
    }
}

impl<'a, PixelType, RasterProcessorType, SubQuery> Stream
    for RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType:
        QueryProcessor<Output = RasterTile2D<PixelType>, SpatialBounds = SpatialPartition2D>,
    SubQuery: SubQueryTileAggregator<PixelType>,
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
                    let tile_query_stream = this
                        .source_processor
                        .raster_query(tile_query_rectangle, *this.query_ctx)
                        .boxed();

                    this.state.set(StateInner::RunningQuery {
                        query: tile_query_stream,
                        query_rect: tile_query_rectangle,
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
        if matches!(
            *this.state,
            StateInner::RunningQuery {
                query: _,
                query_rect: _
            }
        ) {
            // The state is pinned. Project it to get access to the query stored in the context.
            let rq_res = if let StateInnerProjection::RunningQuery { query, query_rect } =
                this.state.as_mut().project()
            {
                ready!(query.poll(cx)).map(|q_res| (q_res, query_rect))
            } else {
                // we already checked that the state is `StateInner::RunningQuery` so this case can not happen.
                unreachable!()
            };

            match rq_res {
                Ok((query, query_rect)) => {
                    // TODO: generation of the folding accu should be a future
                    let tile_folding_accu = this
                        .sub_query
                        .new_fold_accu(*this.current_tile_spec, *query_rect)?;

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

        // We are waiting for/expecting the result of the fold
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
                    this.state.set(StateInner::ReturnResult(Some(tile)));
                }
                Err(e) => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        // at this stage we are in ReturnResult state. Either from a running fold or because the tile query rect was not valid.
        let tile_option = if let StateInnerProjection::ReturnResult(tile_option) =
            this.state.as_mut().project()
        {
            tile_option.take()
        } else {
            unreachable!()
        };
        // in the next poll we need to produce a new tile (if nothing else happens)
        this.state.set(StateInner::CreateNextQuery);

        // if there is a tile, set the current_time_end option.
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

pub fn fold_by_blit_impl<T>(accu: RasterTile2D<T>, tile: RasterTile2D<T>) -> Result<RasterTile2D<T>>
where
    T: Pixel,
{
    let mut accu_tile = accu.into_tile();
    let t_union = accu_tile.time.union(&tile.time)?;

    accu_tile.time = t_union;

    if tile.grid_array.is_empty() && accu_tile.no_data_value() == tile.no_data_value() {
        return Ok(accu_tile);
    }

    let mut materialized_accu_tile = accu_tile.into_materialized_tile();

    match materialized_accu_tile.blit(tile) {
        Ok(_) => Ok(materialized_accu_tile.into()),
        Err(_error) => {
            // Ignore lookup errors
            //dbg!(
            //    "Skipping non-overlapping area tiles in blit method. This schould not happen but the MockSource produces all tiles!!!",
            //    error
            //);
            Ok(materialized_accu_tile.into())
        }
    }
}

#[allow(dead_code)]
pub fn fold_by_blit_future<T>(
    accu: RasterTile2D<T>,
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<RasterTile2D<T>>>
where
    T: Pixel,
{
    tokio::task::spawn_blocking(|| fold_by_blit_impl(accu, tile)).then(async move |x| match x {
        Ok(r) => r,
        Err(e) => Err(e.into()),
    })
}

#[allow(dead_code)]
pub fn fold_by_coordinate_lookup_future<T>(
    accu: TileWithProjectionCoordinates<T>,
    tile: RasterTile2D<T>,
) -> impl TryFuture<Ok = TileWithProjectionCoordinates<T>, Error = error::Error>
where
    T: Pixel,
{
    tokio::task::spawn_blocking(|| fold_by_coordinate_lookup_impl(accu, tile)).then(
        async move |x| match x {
            Ok(r) => r,
            Err(e) => Err(e.into()),
        },
    )
}

#[allow(dead_code)]
#[allow(clippy::type_complexity)]
#[allow(clippy::needless_pass_by_value)]
pub fn fold_by_coordinate_lookup_impl<T>(
    accu: TileWithProjectionCoordinates<T>,
    tile: RasterTile2D<T>,
) -> Result<TileWithProjectionCoordinates<T>>
where
    T: Pixel,
{
    let mut accu = accu;
    let t_union = accu.accu_tile.time.union(&tile.time)?;

    accu.tile_mut().time = t_union;

    if tile.grid_array.is_empty() {
        return Ok(accu);
    }

    let TileWithProjectionCoordinates { accu_tile, coords } = accu;

    let mut materialized_accu_tile = accu_tile.into_materialized_tile(); //in a fold chain the real materialization should only happen once. All other calls will be simple conversions.

    match insert_projected_pixels(&mut materialized_accu_tile, &tile, coords.iter()) {
        Ok(_) => Ok(TileWithProjectionCoordinates {
            accu_tile: materialized_accu_tile.into(),
            coords,
        }),
        Err(error) => Err(error),
    }
}

/// This method takes two tiles and a map from `GridIdx2D` to `Coordinate2D`. Then for all `GridIdx2D` we set the values from the corresponding coordinate in the source tile.
pub fn insert_projected_pixels<'a, T: Pixel, I: Iterator<Item = &'a (GridIdx2D, Coordinate2D)>>(
    target: &mut MaterializedRasterTile2D<T>,
    source: &RasterTile2D<T>,
    local_target_idx_source_coordinate_map: I,
) -> Result<()> {
    // TODO: it would be better to run the pixel wise stuff in insert_projected_pixels in parallel...
    for (idx, coord) in local_target_idx_source_coordinate_map {
        match source.pixel_value_at_coord(*coord) {
            Ok(px_value) => target.set_at_grid_index(*idx, px_value)?,
            Err(e) => match e {
                // Ignore errors where a coordinate is not inside a source tile. This is by design.
                GridIndexOutOfBounds {
                    index: _,
                    min_index: _,
                    max_index: _,
                }
                | InvalidGridIndex {
                    grid_index: _,
                    description: _,
                } => {}
                _ => return Err(error::Error::DataType { source: e }),
            },
        }
    }

    Ok(())
}

/// This trait defines the behavior of the `RasterOverlapAdapter`.
pub trait SubQueryTileAggregator<T>: Send
where
    T: Pixel,
{
    type FoldFuture: Send + TryFuture<Ok = Self::TileAccu, Error = error::Error>;
    type FoldMethod: Send + Clone + Fn(Self::TileAccu, RasterTile2D<T>) -> Self::FoldFuture;
    type TileAccu: FoldTileAccu<RasterType = T> + Clone + Send;

    /// The no-data-value to use in the resulting `RasterTile2D`
    fn result_no_data_value(&self) -> Option<T>;
    /// The initial fill-value of the accumulator (`RasterTile2D`).
    fn initial_fill_value(&self) -> T;

    /// This method generates a new accumulator which is used to fold the `Stream` of `RasterTile2D` of a sub-query.
    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
    ) -> Result<Self::TileAccu>;

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

    fn into_raster_subquery_adapter<'a, S>(
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

#[derive(Debug, Clone)]
pub struct TileSubQueryIdentity<F> {
    fold_fn: F,
}

impl<T, FoldM, FoldF> SubQueryTileAggregator<T> for TileSubQueryIdentity<FoldM>
where
    T: Pixel,
    FoldM: Send + Clone + Fn(RasterTile2D<T>, RasterTile2D<T>) -> FoldF,
    FoldF: Send + TryFuture<Ok = RasterTile2D<T>, Error = error::Error>,
{
    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    type TileAccu = RasterTile2D<T>;

    fn result_no_data_value(&self) -> Option<T> {
        Some(T::from_(0))
    }

    fn initial_fill_value(&self) -> T {
        T::from_(0)
    }

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
    ) -> Result<Self::TileAccu> {
        let output_raster = Grid2D::new_filled(
            tile_info.tile_size_in_pixels,
            self.initial_fill_value(),
            self.result_no_data_value(),
        );
        Ok(RasterTile2D::new_with_tile_info(
            query_rect.time_interval,
            tile_info,
            output_raster.into(),
        ))
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        Ok(Some(RasterQueryRectangle {
            spatial_bounds: tile_info.spatial_partition(),
            time_interval: TimeInterval::new_instant(start_time)?,
            spatial_resolution: query_rect.spatial_resolution,
        }))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

#[derive(Debug, Clone)]
pub struct TileWithProjectionCoordinates<T> {
    accu_tile: RasterTile2D<T>,
    coords: Vec<(GridIdx2D, Coordinate2D)>,
}

impl<T: Pixel> FoldTileAccu for TileWithProjectionCoordinates<T> {
    type RasterType = T;

    fn into_tile(self) -> RasterTile2D<Self::RasterType> {
        self.accu_tile
    }
}

impl<T: Pixel> FoldTileAccuMut for TileWithProjectionCoordinates<T> {
    fn tile_mut(&mut self) -> &mut RasterTile2D<Self::RasterType> {
        &mut self.accu_tile
    }
}

#[derive(Debug)]
pub struct TileReprojectionSubQuery<T, F> {
    pub in_srs: SpatialReference,
    pub out_srs: SpatialReference,
    pub no_data_and_fill_value: T,
    pub fold_fn: F,
    pub in_spatial_res: SpatialResolution,
    pub valid_bounds: SpatialPartition2D,
}

impl<T, FoldM, FoldF> SubQueryTileAggregator<T> for TileReprojectionSubQuery<T, FoldM>
where
    T: Pixel,
    FoldM: Send + Clone + Fn(TileWithProjectionCoordinates<T>, RasterTile2D<T>) -> FoldF,
    FoldF: Send + TryFuture<Ok = TileWithProjectionCoordinates<T>, Error = error::Error>,
{
    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    type TileAccu = TileWithProjectionCoordinates<T>;

    fn result_no_data_value(&self) -> Option<T> {
        Some(self.no_data_and_fill_value)
    }

    fn initial_fill_value(&self) -> T {
        self.no_data_and_fill_value
    }

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
    ) -> Result<Self::TileAccu> {
        let output_raster =
            EmptyGrid::new(tile_info.tile_size_in_pixels, self.no_data_and_fill_value);

        // get all pixel idxs and coordinates.
        let idxs: Vec<GridIdx2D> = grid_idx_iter_2d(&output_raster.bounding_box()).collect();
        let idx_coords: Vec<(GridIdx2D, Coordinate2D)> = idxs
            .iter()
            .map(|&i| {
                (
                    i,
                    tile_info
                        .tile_geo_transform()
                        .grid_idx_to_upper_left_coordinate_2d(i),
                )
            })
            .collect();

        // check if the tile to fill is contained by the valid bounds of the input projection
        let idx_coords = if self.valid_bounds.contains(&tile_info.spatial_partition()) {
            debug!("reproject whole tile");
            // use all pixel coordinates
            idx_coords
        } else if self.valid_bounds.intersects(&tile_info.spatial_partition()) {
            debug!("reproject part of tile");
            // filter the coordinates to only contain valid ones
            idx_coords
                .into_iter()
                .filter(|(_, c)| self.valid_bounds.contains_coordinate(c))
                .collect()
        } else {
            debug!("reproject empty tile");
            // fastpath to skip filter
            vec![]
        };

        // unzip to use batch projection
        let (idxs, coords): (Vec<_>, Vec<_>) = idx_coords.into_iter().unzip();
        debug!("reproject {} pixels", idxs.len());

        // project
        let proj = CoordinateProjector::from_known_srs(self.out_srs, self.in_srs)?; // TODO: check if we can store it in self. Maybe not because send/sync.
        let projected_coords = proj.project_coordinates(&coords)?;

        // zip idx and projected coordinates
        let coords: Vec<(GridIdx2D, Coordinate2D)> =
            idxs.into_iter().zip(projected_coords.into_iter()).collect();

        Ok(TileWithProjectionCoordinates {
            accu_tile: RasterTile2D::new_with_tile_info(
                query_rect.time_interval,
                tile_info,
                output_raster.into(),
            ),
            coords,
        })
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        let proj = CoordinateProjector::from_known_srs(self.out_srs, self.in_srs)?;

        let spatial_bounds = tile_info
            .spatial_partition()
            .intersection(&query_rect.spatial_partition())
            .expect("should not be empty")
            .reproject(&proj);

        if let Ok(spatial_bounds) = spatial_bounds {
            Ok(Some(RasterQueryRectangle {
                spatial_bounds,
                time_interval: TimeInterval::new_instant(start_time)?,
                spatial_resolution: self.in_spatial_res,
            }))
        } else {
            // output query rectangle is not valid in source projection => produce empty tile
            Ok(None)
        }
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
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
    use num_traits::AsPrimitive;

    #[tokio::test]
    async fn identity() {
        let no_data_value = Some(0);
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], no_data_value)
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
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 1.).into(), (3., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };

        let query_ctx = MockQueryContext::default();
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
            },
        );
        let res = a
            .map(Result::unwrap)
            .map(Option::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;
        assert_eq!(data, res);
    }

    #[tokio::test]
    async fn identity_projection() {
        let projection = SpatialReference::epsg_4326();
        let no_data_value = Some(0);

        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], no_data_value)
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
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 1.).into(), (3., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };

        let query_ctx = MockQueryContext::default();
        let tiling_strat = exe_ctx.tiling_specification;

        let op = mrs1.initialize(&exe_ctx).await.unwrap();

        let raster_res_desc: &RasterResultDescriptor = op.result_descriptor();

        let no_data_v = raster_res_desc
            .no_data_value
            .map(AsPrimitive::as_)
            .expect("must be 0 since we specified it above");

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let valid_bounds = projection.area_of_use_projected().unwrap();

        let state_gen = TileReprojectionSubQuery {
            in_srs: projection,
            out_srs: projection,
            no_data_and_fill_value: no_data_v,
            fold_fn: fold_by_coordinate_lookup_future,
            in_spatial_res: query_rect.spatial_resolution,
            valid_bounds,
        };
        let a = RasterSubQueryAdapter::new(&qp, query_rect, tiling_strat, &query_ctx, state_gen);
        let res = a
            .map(Result::unwrap)
            .map(Option::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;
        assert_eq!(data, res);
    }
}
