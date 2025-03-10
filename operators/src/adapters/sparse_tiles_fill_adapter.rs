use crate::util::Result;
use futures::{ready, Stream};
use geoengine_datatypes::{
    primitives::{CacheExpiration, CacheHint, RasterQueryRectangle, TimeInstance, TimeInterval},
    raster::{
        EmptyGrid2D, GeoTransform, GridBoundingBox2D, GridBounds, GridIdx2D, GridShape2D, GridStep,
        Pixel, RasterTile2D, TilingStrategy,
    },
};
use pin_project::pin_project;
use snafu::Snafu;
use std::{pin::Pin, task::Poll};

#[derive(Debug, Snafu)]
pub enum SparseTilesFillAdapterError {
    #[snafu(display(
        "Received tile TimeInterval ({}) starts before current TimeInterval: {}. This is probably a bug in a child operator.",
        tile_start,
        current_start
    ))]
    TileTimeIntervalStartBeforeCurrentStart {
        tile_start: TimeInterval,
        current_start: TimeInterval,
    },
    #[snafu(display(
        "Received tile TimeInterval ({}) length differs from received TimeInterval with equal start: {}. This is probably a bug in a child operator.",
        tile_interval,
        current_interval
    ))]
    TileTimeIntervalLengthMissmatch {
        tile_interval: TimeInterval,
        current_interval: TimeInterval,
    },
    #[snafu(display(
        "Received tile TimeInterval ({}) is the first in a grid run but does no time progress. (Old time: {}). This is probably a bug in a child operator.",
        tile_interval,
        current_interval
    ))]
    GridWrapMustDoTimeProgress {
        tile_interval: TimeInterval,
        current_interval: TimeInterval,
    },
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum State {
    Initial,
    PollingForNextTile,
    FillAndProduceNextTile,
    FillToEnd,
    Ended,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct FillerTimeBounds {
    start: TimeInstance,
    end: TimeInstance,
}

impl From<TimeInterval> for FillerTimeBounds {
    fn from(time: TimeInterval) -> FillerTimeBounds {
        FillerTimeBounds {
            start: time.start(),
            end: time.end(),
        }
    }
}

impl FillerTimeBounds {
    fn start(&self) -> TimeInstance {
        self.start
    }
    fn end(&self) -> TimeInstance {
        self.end
    }

    pub fn new(start: TimeInstance, end: TimeInstance) -> Self {
        Self::new_unchecked(start, end)
    }

    pub fn new_unchecked(start: TimeInstance, end: TimeInstance) -> Self {
        Self { start, end }
    }
}

#[derive(Debug, PartialEq, Clone)]
struct StateContainer<T> {
    current_idx: GridIdx2D,
    current_band_idx: u32,
    current_time: Option<TimeInterval>,
    next_tile: Option<RasterTile2D<T>>,
    no_data_grid: EmptyGrid2D<T>,
    grid_bounds: GridBoundingBox2D,
    num_bands: u32,
    global_geo_transform: GeoTransform,
    state: State,
    cache_hint: FillerTileCacheHintProvider,
    requested_time_bounds: TimeInterval,
    data_time_bounds: FillerTimeBounds,
}

struct GridIdxAndBand {
    idx: GridIdx2D,
    band_idx: u32,
}

impl<T: Pixel> StateContainer<T> {
    /// Create a new no-data `RasterTile2D` with `GridIdx` and time from the current state
    fn current_no_data_tile(&self) -> RasterTile2D<T> {
        RasterTile2D::new(
            self.current_time
                .expect("time must exist when a tile is stored."),
            self.current_idx,
            self.current_band_idx,
            self.global_geo_transform,
            self.no_data_grid.into(),
            self.cache_hint.next_hint(),
        )
    }

    /// Check if the next tile to produce is the stored one
    fn stored_tile_is_next(&self) -> bool {
        if let Some(t) = &self.next_tile {
            // The stored tile is the one we are looking for if the tile position is the next to produce
            self.grid_idx_and_band_is_the_next_to_produce(t.tile_position, t.band) &&
            // and the time equals the current state time
                (self.time_equals_current_state(t.time)
                // or it starts a new time step, and the time directly follows the current state time
                || (self.current_idx_and_band_is_first_in_grid_run() && self.time_directly_following_current_state(t.time)))
        } else {
            false
        }
    }

    /// Get the next `GridIdxAndBand` following the current state `GridIdx` and band. None if the current `GridIdx` is the max `GridIdx` of the grid.
    fn maybe_next_idx_and_band(&self) -> Option<GridIdxAndBand> {
        if self.current_band_idx + 1 < self.num_bands {
            // next band
            Some(GridIdxAndBand {
                idx: self.current_idx,
                band_idx: self.current_band_idx + 1,
            })
        } else {
            // next tile
            self.grid_bounds
                .inc_idx_unchecked(self.current_idx, 1)
                .map(|idx| GridIdxAndBand { idx, band_idx: 0 })
        }
    }

    /// Get the next `GridIdxAndBand` following the current state `GridIdx` and band. Returns the minimal `GridIdx` if the current `GridIdx` is the max `GridIdx`.
    fn wrapped_next_idx_and_band(&self) -> GridIdxAndBand {
        if self.current_band_idx + 1 < self.num_bands {
            // next band
            GridIdxAndBand {
                idx: self.current_idx,
                band_idx: self.current_band_idx + 1,
            }
        } else {
            // next tile
            GridIdxAndBand {
                idx: self
                    .grid_bounds
                    .inc_idx_unchecked(self.current_idx, 1)
                    .unwrap_or_else(|| self.min_index()),
                band_idx: 0,
            }
        }
    }

    /// Get the minimal `GridIdx` of the grid.
    fn min_index(&self) -> GridIdx2D {
        self.grid_bounds.min_index()
    }

    /// Get the maximal `GridIdx` of the grid.
    fn max_index(&self) -> GridIdx2D {
        self.grid_bounds.max_index()
    }

    /// Check if any tile is stored in the state.
    fn is_any_tile_stored(&self) -> bool {
        self.next_tile.is_some()
    }

    /// Check if a `TimeInterval` starts before the current state `TimeInterval`.
    fn time_starts_before_current_state(&self, time: TimeInterval) -> bool {
        time.start()
            < self
                .current_time
                .expect("state time is set on initialize")
                .start()
    }

    /// Check if a `TimeInterval` start equals the start of the current state `TimeInterval`.
    fn time_starts_equals_current_state(&self, time: TimeInterval) -> bool {
        time.start()
            == self
                .current_time
                .expect("state time is set on initialize")
                .start()
    }

    /// Check if a `TimeInterval` duration equals the duration of the current state `TimeInterval`.
    fn time_duration_equals_current_state(&self, time: TimeInterval) -> bool {
        time.duration_ms()
            == self
                .current_time
                .expect("state time is set on initialize")
                .duration_ms()
    }

    /// Check if a `TimeInterval` equals the current state `TimeInterval`.
    fn time_equals_current_state(&self, time: TimeInterval) -> bool {
        time == self.current_time.expect("state time is set on initialize")
    }

    /// Check if a `GridIdx` is the next to produce i.e. the current state `GridIdx`.
    fn grid_idx_and_band_is_the_next_to_produce(&self, tile_idx: GridIdx2D, band_idx: u32) -> bool {
        tile_idx == self.current_idx && band_idx == self.current_band_idx
    }

    /// Check if a `TimeInterval` is directly connected to the end of the current state `TimeInterval`.
    fn time_directly_following_current_state(&self, time: TimeInterval) -> bool {
        let current_time = self.current_time.expect("state time is set on initialize");
        if current_time.is_instant() {
            current_time.end() + 1 == time.start()
        } else {
            current_time.end() == time.start()
        }
    }

    fn next_time_interval_from_stored_tile(&self) -> Option<TimeInterval> {
        // we wrapped around. We need to do time progress.
        if let Some(tile) = &self.next_tile {
            let stored_tile_time = tile.time;

            if self.time_directly_following_current_state(stored_tile_time) {
                Some(stored_tile_time)
            } else if let Some(current_time) = self.current_time {
                // we need to fill a time gap
                if current_time.is_instant() {
                    TimeInterval::new(current_time.end() + 1, stored_tile_time.start()).ok()
                } else {
                    TimeInterval::new(current_time.end(), stored_tile_time.start()).ok()
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Check if the current state `GridIdx`  is the first of a grid run i.e. it equals the minimal `GridIdx` .
    fn current_idx_and_band_is_first_in_grid_run(&self) -> bool {
        self.current_idx == self.min_index() && self.current_band_idx == 0
    }

    /// Check if the current state `GridIdx`  is the first of a grid run i.e. it equals the minimal `GridIdx` .
    fn current_idx_and_band_is_last_in_grid_run(&self) -> bool {
        self.current_idx == self.max_index() && self.current_band_idx == self.num_bands - 1
    }

    fn set_current_time_from_initial_tile(&mut self, first_tile_time: TimeInterval) {
        // if we know a bound we must use it to set the current time
        let start_data_bound = self.data_time_bounds.start();
        let requested_start = self.requested_time_bounds.start();

        debug_assert!(
            start_data_bound <= requested_start,
            "The data bound hint start should be <= the requested start. "
        );

        if requested_start < first_tile_time.start() {
            log::debug!(
                    "The initial tile starts ({}) after the requested start bound ({}), setting the current time to the data start bound ({}) --> filling", first_tile_time.start(), requested_start, start_data_bound
                );
            self.current_time = Some(TimeInterval::new_unchecked(
                start_data_bound,
                first_tile_time.start(),
            ));
            return;
        }
        if start_data_bound > first_tile_time.start() {
            log::debug!(
                    "The initial tile time start ({}) is before the exprected time bounds ({}). This means the data overflows the filler start bound.",
                    first_tile_time.start(),
                    start_data_bound
                );
        }
        self.current_time = Some(first_tile_time);
    }

    fn set_current_time_from_data_time_bounds(&mut self) {
        assert!(self.state == State::FillToEnd);
        self.current_time = Some(TimeInterval::new_unchecked(
            self.data_time_bounds.start(),
            self.data_time_bounds.end(),
        ));
    }

    fn update_current_time(&mut self, new_time: TimeInterval) {
        debug_assert!(
            !new_time.is_instant(),
            "Tile time is the data validity and must not be an instant!"
        );

        if let Some(old_time) = self.current_time {
            if old_time == new_time {
                return;
            }

            debug_assert!(
                old_time.end() <= new_time.start(),
                "Time progress must be positive"
            );

            debug_assert!(
                !old_time.is_instant() || old_time.end() < new_time.start(),
                "Instant progress must be > 1"
            );
        }
        self.current_time = Some(new_time);
    }

    fn current_time_fill_to_end_interval(&mut self) {
        let current_time: TimeInterval = self
            .current_time
            .expect("current time must exist for fill to end state.");
        debug_assert!(current_time.end() <= self.requested_time_bounds.end());

        debug_assert!(current_time.end() < self.data_time_bounds.end());

        let new_time = if current_time.is_instant() {
            TimeInterval::new_unchecked(current_time.end() + 1, self.data_time_bounds.end())
        } else {
            TimeInterval::new_unchecked(current_time.end(), self.data_time_bounds.end())
        };
        self.update_current_time(new_time);
    }

    fn current_time_is_valid_end_bound(&self) -> bool {
        let time_requested_end = self.requested_time_bounds.end();
        let time_bounds_end = self.data_time_bounds.end();
        let current_time = self.current_time.expect("state time is set on initialize");

        if current_time.end() < time_requested_end {
            return false;
        }
        if current_time.end() > time_bounds_end {
            log::debug!(
                    "The current time end ({}) is after the exprected time bounds ({}). This means the data overflows the filler end bound.",
                    current_time.end(),
                    time_bounds_end
                );
        }

        true
    }

    fn store_tile(&mut self, tile: RasterTile2D<T>) {
        debug_assert!(self.next_tile.is_none());
        let current_time = self
            .current_time
            .expect("Time must be set when the first tile arrives");
        debug_assert!(current_time.start() <= tile.time.start());
        debug_assert!(
            current_time.start() < tile.time.start()
                || (self.current_idx.y() < tile.tile_position.y()
                    || (self.current_idx.y() == tile.tile_position.y()
                        && self.current_idx.x() < tile.tile_position.x()))
        );
        self.next_tile = Some(tile);
    }
}

#[pin_project(project=SparseTilesFillAdapterProjection)]
pub struct SparseTilesFillAdapter<T, S> {
    #[pin]
    stream: S,

    sc: StateContainer<T>,
}

impl<T, S> SparseTilesFillAdapter<T, S>
where
    T: Pixel,
    S: Stream<Item = Result<RasterTile2D<T>>>,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream: S,
        tile_grid_bounds: GridBoundingBox2D,
        num_bands: u32,
        global_geo_transform: GeoTransform,
        tile_shape: GridShape2D,
        cache_expiration: FillerTileCacheExpirationStrategy, // Specifies the cache expiration for the produced filler tiles. Set this to unlimited if the filler tiles will always be empty
        requested_time_bounds: TimeInterval,
        data_time_bounds: FillerTimeBounds,
    ) -> Self {
        debug_assert!(
            data_time_bounds.start <= requested_time_bounds.start(),
            "Data time bounds hint start should be <= requested time start."
        );
        debug_assert!(
            requested_time_bounds.end() <= data_time_bounds.end,
            "Data time bounds hint end should be >= requested time end."
        );

        SparseTilesFillAdapter {
            stream,
            sc: StateContainer {
                current_idx: tile_grid_bounds.min_index(),
                current_band_idx: 0,
                current_time: None,
                global_geo_transform,
                grid_bounds: tile_grid_bounds,
                num_bands,
                next_tile: None,
                no_data_grid: EmptyGrid2D::new(tile_shape),
                state: State::Initial,
                cache_hint: cache_expiration.into(),
                requested_time_bounds,
                data_time_bounds,
            },
        }
    }

    /// Creates a new `SparseTilesFillAdapter` that fills the gaps of the input stream with empty tiles.
    /// The input stream must be sorted by `GridIdx` and `TimeInterval`.
    /// The adaper will fill the gaps within the `query_rect_to_answer` with empty tiles.
    ///
    /// # Panics
    /// If the `query_rect_to_answer` has a different `origin_coordinate` than the `tiling_spec`.
    ///
    pub fn new_like_subquery(
        stream: S,
        query_rect_to_answer: &RasterQueryRectangle,
        tiling_strat: TilingStrategy,
        cache_expiration: FillerTileCacheExpirationStrategy,
        time_bounds: FillerTimeBounds,
    ) -> Self {
        let grid_bounds = tiling_strat
            .raster_spatial_query_to_tiling_grid_box(&query_rect_to_answer.spatial_query());
        Self::new(
            stream,
            grid_bounds,
            query_rect_to_answer.attributes.count(),
            tiling_strat.geo_transform,
            tiling_strat.tile_size_in_pixels,
            cache_expiration,
            query_rect_to_answer.time_interval,
            time_bounds,
        )
    }

    // TODO: return Result with SparseTilesFillAdapterError and map it to Error in the poll_next method if possible
    #[allow(clippy::too_many_lines, clippy::missing_panics_doc)]
    pub fn next_step(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<RasterTile2D<T>>>> {
        // TODO: we should check for unexpected bands in the input stream (like we do for time and space).

        let min_idx = self.sc.min_index();
        let wrapped_next = self.sc.wrapped_next_idx_and_band();
        let wrapped_next_idx = wrapped_next.idx;
        let wrapped_next_band = wrapped_next.band_idx;

        let mut this = self.project();

        match this.sc.state {
            // this is the initial state.
            State::Initial => {
                // there must not be a grid stored
                debug_assert!(!this.sc.is_any_tile_stored());
                // poll for a first (input) tile
                let result_tile = match ready!(this.stream.as_mut().poll_next(cx)) {
                    Some(Ok(tile)) => {
                        // now we have to inspect the time we got and the bound we need to fill. If there are bounds known, then we need to check if the tile starts with the bounds.
                        this.sc.set_current_time_from_initial_tile(tile.time);

                        this.sc
                            .cache_hint
                            .update_cache_expiration(tile.cache_hint.expires()); // save the expiration date to be used for the filler tiles, depending on the expiration strategy

                        if this.sc.time_equals_current_state(tile.time)
                            && this.sc.grid_idx_and_band_is_the_next_to_produce(
                                tile.tile_position,
                                tile.band,
                            )
                        {
                            this.sc.state = State::PollingForNextTile; // return the received tile and set state to polling for the next tile
                            tile
                        } else {
                            this.sc.store_tile(tile);
                            this.sc.state = State::FillAndProduceNextTile; // save the tile and go to fill mode
                            this.sc.current_no_data_tile()
                        }
                    }
                    // an error ouccured, stop producing anything and return the error.
                    Some(Err(e)) => {
                        this.sc.state = State::Ended;
                        return Poll::Ready(Some(Err(e)));
                    }
                    // the source never produced a tile.
                    None => {
                        debug_assert!(this.sc.current_idx == min_idx);
                        this.sc.state = State::FillToEnd;
                        this.sc.set_current_time_from_data_time_bounds();
                        this.sc.current_no_data_tile()
                    }
                };
                // move the current_idx. There is no need to do time progress here. Either a new tile triggers that or it is never needed for an empty source.
                this.sc.current_idx = wrapped_next_idx;
                this.sc.current_band_idx = wrapped_next_band;
                Poll::Ready(Some(Ok(result_tile)))
            }
            // this is the state where we are waiting for the next tile to arrive.
            State::PollingForNextTile => {
                // there must not be a tile stored
                debug_assert!(!this.sc.is_any_tile_stored());

                // There are four cases we have to handle:
                // 1. The received tile has a TimeInterval that starts before the current tile. This must not happen and is probably a bug in the source.
                // 2. The received tile has a TimeInterval that matches the TimeInterval of the state. This TimeInterval is currently produced.
                // 3. The received tile has a TimeInterval that directly continues the current TimeInterval.
                //    This TimeInterval will be produced once all tiles of the current grid have been produced.
                // 4. The received tile has a TimeInterval that starts after the current TimeInterval and is not directly connected to the current TimeInterval.
                //    Before this TimeInterval is produced, an intermediate TimeInterval is produced.

                let res = match ready!(this.stream.as_mut().poll_next(cx)) {
                    Some(Ok(tile)) => {
                        // 1. The start of the recieved TimeInterval MUST NOT BE before the start of the current TimeInterval.
                        if this.sc.time_starts_before_current_state(tile.time) {
                            this.sc.state = State::Ended;
                            return Poll::Ready(Some(Err(
                            SparseTilesFillAdapterError::TileTimeIntervalStartBeforeCurrentStart {
                                current_start: this.sc.current_time.expect("time is set in initial step"),
                                tile_start: tile.time,
                            }
                            .into(),
                        )));
                        }
                        if tile.time.start() >= this.sc.requested_time_bounds.end() {
                            log::warn!(
                                    "The tile time start ({}) is outside of the requested time bounds ({})!",
                                    tile.time.start(),
                                    this.sc.requested_time_bounds.end()
                                );
                        }

                        // 1. b) This is a new grid run but the time is not increased
                        if this.sc.current_idx_and_band_is_first_in_grid_run()
                            && this.sc.time_equals_current_state(tile.time)
                        {
                            this.sc.state = State::Ended;
                            return Poll::Ready(Some(Err(
                                SparseTilesFillAdapterError::GridWrapMustDoTimeProgress {
                                    current_interval: this
                                        .sc
                                        .current_time
                                        .expect("time is set in initial step"),
                                    tile_interval: tile.time,
                                }
                                .into(),
                            )));
                        }

                        // 2. a) The received TimeInterval with start EQUAL to the current TimeInterval MUST NOT have a different duration / end.
                        if this.sc.time_starts_equals_current_state(tile.time)
                            && !this.sc.time_duration_equals_current_state(tile.time)
                        {
                            this.sc.state = State::Ended;
                            return Poll::Ready(Some(Err(
                                SparseTilesFillAdapterError::TileTimeIntervalLengthMissmatch {
                                    tile_interval: tile.time,
                                    current_interval: this
                                        .sc
                                        .current_time
                                        .expect("time is set in initial step"),
                                }
                                .into(),
                            )));
                        };

                        this.sc
                            .cache_hint
                            .update_cache_expiration(tile.cache_hint.expires()); // save the expiration date to be used for the filler tiles, depending on the expiration strategy

                        // 2 b) The received TimeInterval with start EQUAL to the current TimeInterval MUST NOT have a different duration / end.
                        let next_tile = if this.sc.time_equals_current_state(tile.time) {
                            if this.sc.grid_idx_and_band_is_the_next_to_produce(
                                tile.tile_position,
                                tile.band,
                            ) {
                                // the tile is the next to produce. Return it and set state to polling for the next tile.
                                this.sc.state = State::PollingForNextTile;
                                tile
                            } else {
                                // the tile is not the next to produce. Save it and go to fill mode.
                                this.sc.store_tile(tile);
                                this.sc.state = State::FillAndProduceNextTile;
                                this.sc.current_no_data_tile()
                            }
                        }
                        // 3. The received tile has a TimeInterval that directly continues the current TimeInterval.
                        else if this.sc.time_directly_following_current_state(tile.time) {
                            // if the current_idx is the first in a new grid run then it is the first one with the new TimeInterval.
                            // this switches the time in the state to the time of the new tile.
                            if this.sc.current_idx_and_band_is_first_in_grid_run() {
                                if this.sc.grid_idx_and_band_is_the_next_to_produce(
                                    tile.tile_position,
                                    tile.band,
                                ) {
                                    // return the tile and set state to polling for the next tile.
                                    this.sc.update_current_time(tile.time);
                                    this.sc.state = State::PollingForNextTile;
                                    tile
                                } else {
                                    // save the tile and go to fill mode.
                                    this.sc.update_current_time(tile.time);
                                    this.sc.store_tile(tile);
                                    this.sc.state = State::FillAndProduceNextTile;
                                    this.sc.current_no_data_tile()
                                }
                            } else {
                                // the received tile is in a new TimeInterval but we still need to finish the current one. Store tile and go to fill mode.
                                this.sc.store_tile(tile);
                                this.sc.state = State::FillAndProduceNextTile;
                                this.sc.current_no_data_tile()
                            }
                        }
                        // 4. The received tile has a TimeInterval that starts after the current TimeInterval and is not directly connected to the current TimeInterval.
                        else {
                            // if the current_idx is the first in a new grid run then it is the first one with a new TimeInterval.
                            // We need to generate a fill TimeInterval since current and tile TimeInterval are not connected.
                            if this.sc.current_idx_and_band_is_first_in_grid_run() {
                                this.sc.update_current_time(TimeInterval::new(
                                    this.sc
                                        .current_time
                                        .expect("time is set in initial state")
                                        .end(),
                                    tile.time.start(),
                                )?);
                                this.sc.store_tile(tile);
                                this.sc.state = State::FillAndProduceNextTile;
                                this.sc.current_no_data_tile()
                            } else {
                                // the received tile is in a new TimeInterval but we still need to finish the current one. Store tile and go to fill mode.
                                this.sc.store_tile(tile);
                                this.sc.state = State::FillAndProduceNextTile;
                                this.sc.current_no_data_tile()
                            }
                        };
                        Some(Ok(next_tile))
                    }

                    // an error ouccured, stop producing anything and return the error.
                    Some(Err(e)) => {
                        this.sc.state = State::Ended;
                        return Poll::Ready(Some(Err(e)));
                    }
                    // the source is empty (now). Remember that.
                    None => {
                        if this.sc.current_idx_and_band_is_first_in_grid_run()
                            && this.sc.current_time_is_valid_end_bound()
                        {
                            // there was a tile and it flipped the state index to the first one. => we are done.
                            this.sc.state = State::Ended;
                            None
                        } else if this.sc.current_idx_and_band_is_last_in_grid_run()
                            && this.sc.current_time_is_valid_end_bound()
                        {
                            // this is the last tile
                            this.sc.state = State::Ended;
                            Some(Ok(this.sc.current_no_data_tile()))
                        } else if this.sc.current_idx_and_band_is_last_in_grid_run() {
                            // there was a tile and it was the last one but it does not cover the time bounds. => go to fill to end mode.
                            this.sc.state = State::FillToEnd;
                            let no_data_tile = this.sc.current_no_data_tile();
                            this.sc.current_time_fill_to_end_interval();
                            Some(Ok(no_data_tile))
                        } else if this.sc.current_idx_and_band_is_first_in_grid_run() {
                            // there was a tile and it was the last one but it does not cover the time bounds. => go to fill to end mode.
                            this.sc.state = State::FillToEnd;
                            this.sc.current_time_fill_to_end_interval();
                            Some(Ok(this.sc.current_no_data_tile()))
                        } else {
                            // there was a tile and it was not the last one. => go to fill to end mode.
                            this.sc.state = State::FillToEnd;
                            Some(Ok(this.sc.current_no_data_tile()))
                        }
                    }
                };
                // move the current_idx and band. There is no need to do time progress here. Either a new tile sets that or it is not needed to fill to the end of the grid.

                this.sc.current_idx = wrapped_next_idx;
                this.sc.current_band_idx = wrapped_next_band;

                if this.sc.current_idx_and_band_is_first_in_grid_run() {
                    if let Some(next_time) = this.sc.next_time_interval_from_stored_tile() {
                        this.sc.update_current_time(next_time);
                    }
                }

                Poll::Ready(res)
            }
            // the tile to produce is the the one stored
            State::FillAndProduceNextTile if this.sc.stored_tile_is_next() => {
                // take the tile (replace in state with NONE)
                let next_tile = this.sc.next_tile.take().expect("checked by case");
                debug_assert!(this.sc.current_idx == next_tile.tile_position);

                this.sc.update_current_time(next_tile.time);
                this.sc.current_idx = wrapped_next_idx;
                this.sc.current_band_idx = wrapped_next_band;
                this.sc.state = State::PollingForNextTile;

                Poll::Ready(Some(Ok(next_tile)))
            }
            // this is the state where we produce fill tiles until another state is reached.
            State::FillAndProduceNextTile => {
                let (next_idx, next_band, next_time) = match this.sc.maybe_next_idx_and_band() {
                    // the next GridIdx is in the current TimeInterval
                    Some(idx) => (
                        idx.idx,
                        idx.band_idx,
                        this.sc.current_time.expect("time is set by initial step"),
                    ),
                    // the next GridIdx is in the next TimeInterval
                    None => (
                        this.sc.min_index(),
                        0,
                        this.sc
                            .next_time_interval_from_stored_tile()
                            .expect("there must be a way to determine the new time"),
                    ),
                };

                let no_data_tile = this.sc.current_no_data_tile();

                this.sc.update_current_time(next_time);
                this.sc.current_idx = next_idx;
                this.sc.current_band_idx = next_band;

                Poll::Ready(Some(Ok(no_data_tile)))
            }
            // this is the last tile to produce ever
            State::FillToEnd
                if this.sc.current_idx_and_band_is_last_in_grid_run()
                    && this.sc.current_time_is_valid_end_bound() =>
            {
                this.sc.state = State::Ended;
                Poll::Ready(Some(Ok(this.sc.current_no_data_tile())))
            }
            State::FillToEnd if this.sc.current_idx_and_band_is_last_in_grid_run() => {
                // we have reached the end of the time interval but the bounds are not covered yet. We need to create a new intermediate TimeInterval.
                let no_data_tile = this.sc.current_no_data_tile();
                this.sc.current_idx = wrapped_next_idx;
                this.sc.current_band_idx = wrapped_next_band;
                this.sc.current_time_fill_to_end_interval();
                Poll::Ready(Some(Ok(no_data_tile)))
            }

            // there are more tiles to produce to fill the grid in this time step
            State::FillToEnd => {
                let no_data_tile = this.sc.current_no_data_tile();
                this.sc.current_idx = wrapped_next_idx;
                this.sc.current_band_idx = wrapped_next_band;
                Poll::Ready(Some(Ok(no_data_tile)))
            }
            State::Ended => Poll::Ready(None),
        }
    }
}

impl<T, S> Stream for SparseTilesFillAdapter<T, S>
where
    T: Pixel,
    S: Stream<Item = Result<RasterTile2D<T>>>,
{
    type Item = Result<RasterTile2D<T>>;

    #[allow(clippy::too_many_lines)]
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.sc.state == State::Ended {
            return Poll::Ready(None);
        }

        self.next_step(cx)
    }
}

/// The strategy determines how to set the cache expiration for filler tiles produced by the adapter.
///
/// It can either be a fixed value for all produced tiles, or it can be derived from the surrounding tiles that are not produced by the adapter.
/// In the latter case it will use the cache expiration of the preceeding tile if it is available, otherwise it will use the cache expiration of the following tile.
#[derive(Debug, Clone, PartialEq)]
pub enum FillerTileCacheExpirationStrategy {
    NoCache,
    FixedValue(CacheExpiration),
    DerivedFromSurroundingTiles,
}

#[derive(Debug, Clone, PartialEq)]
struct FillerTileCacheHintProvider {
    strategy: FillerTileCacheExpirationStrategy,
    state: FillerTileCacheHintState,
}

#[derive(Debug, Clone, PartialEq)]
enum FillerTileCacheHintState {
    Initial,
    FirstTile(CacheExpiration),
    OtherTile {
        current_cache_expiration: CacheExpiration,
        next_cache_expiration: CacheExpiration,
    },
}

impl FillerTileCacheHintState {
    fn cache_expiration(&self) -> Option<CacheExpiration> {
        match self {
            FillerTileCacheHintState::Initial => None,
            FillerTileCacheHintState::FirstTile(expiration) => Some(*expiration),
            FillerTileCacheHintState::OtherTile {
                current_cache_expiration,
                next_cache_expiration: _,
            } => Some(*current_cache_expiration),
        }
    }
}

impl FillerTileCacheHintProvider {
    fn update_cache_expiration(&mut self, expiration: CacheExpiration) {
        self.state = match self.state {
            FillerTileCacheHintState::Initial => FillerTileCacheHintState::FirstTile(expiration),
            FillerTileCacheHintState::FirstTile(current_cache_expiration) => {
                FillerTileCacheHintState::OtherTile {
                    current_cache_expiration,
                    next_cache_expiration: expiration,
                }
            }
            FillerTileCacheHintState::OtherTile {
                current_cache_expiration: _,
                next_cache_expiration,
            } => FillerTileCacheHintState::OtherTile {
                current_cache_expiration: next_cache_expiration,
                next_cache_expiration: expiration,
            },
        };
    }

    fn next_hint(&self) -> CacheHint {
        match self.strategy {
            FillerTileCacheExpirationStrategy::NoCache => CacheHint::no_cache(),
            FillerTileCacheExpirationStrategy::FixedValue(expiration) => expiration.into(),
            FillerTileCacheExpirationStrategy::DerivedFromSurroundingTiles => self
                .state
                .cache_expiration()
                .map_or(CacheHint::no_cache(), Into::into),
        }
    }
}

impl From<FillerTileCacheExpirationStrategy> for FillerTileCacheHintProvider {
    fn from(value: FillerTileCacheExpirationStrategy) -> Self {
        Self {
            strategy: value,
            state: FillerTileCacheHintState::Initial,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{stream, StreamExt};
    use geoengine_datatypes::{
        primitives::{CacheHint, TimeInterval},
        raster::Grid,
        util::test::TestDefault,
    };

    use super::*;

    #[tokio::test]
    async fn test_gap_overlaps_time_step() {
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            // GAP
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            // GAP
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::new_unchecked(5, 10),
            FillerTimeBounds::from(TimeInterval::new_unchecked(5, 10)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        let tile_time_positions: Vec<(GridIdx2D, TimeInterval)> = tiles
            .into_iter()
            .map(|t| {
                let g = t.unwrap();
                (g.tile_position, g.time)
            })
            .collect();

        let expected_positions = vec![
            ([-1, 0].into(), TimeInterval::new_unchecked(0, 5)),
            ([-1, 1].into(), TimeInterval::new_unchecked(0, 5)),
            ([0, 0].into(), TimeInterval::new_unchecked(0, 5)),
            ([0, 1].into(), TimeInterval::new_unchecked(0, 5)),
            ([-1, 0].into(), TimeInterval::new_unchecked(5, 10)),
            ([-1, 1].into(), TimeInterval::new_unchecked(5, 10)),
            ([0, 0].into(), TimeInterval::new_unchecked(5, 10)),
            ([0, 1].into(), TimeInterval::new_unchecked(5, 10)),
        ];

        assert_eq!(tile_time_positions, expected_positions);
    }

    #[tokio::test]
    async fn test_empty() {
        let data = vec![];
        // GAP
        // GAP
        // GAP
        // GAP

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::default(),
            FillerTimeBounds::from(TimeInterval::default()),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        let tile_time_positions: Vec<(GridIdx2D, TimeInterval)> = tiles
            .into_iter()
            .map(|t| {
                let g = t.unwrap();
                (g.tile_position, g.time)
            })
            .collect();

        let expected_positions = vec![
            ([-1, 0].into(), TimeInterval::default()),
            ([-1, 1].into(), TimeInterval::default()),
            ([0, 0].into(), TimeInterval::default()),
            ([0, 1].into(), TimeInterval::default()),
        ];

        assert_eq!(tile_time_positions, expected_positions);
    }

    #[tokio::test]
    async fn test_gaps_at_begin() {
        let data = vec![
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::new_unchecked(0, 10),
            FillerTimeBounds::from(TimeInterval::new_unchecked(0, 10)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        let tile_time_positions: Vec<(GridIdx2D, TimeInterval)> = tiles
            .into_iter()
            .map(|t| {
                let g = t.unwrap();
                (g.tile_position, g.time)
            })
            .collect();

        let expected_positions = vec![
            ([-1, 0].into(), TimeInterval::new_unchecked(0, 5)),
            ([-1, 1].into(), TimeInterval::new_unchecked(0, 5)),
            ([0, 0].into(), TimeInterval::new_unchecked(0, 5)),
            ([0, 1].into(), TimeInterval::new_unchecked(0, 5)),
            ([-1, 0].into(), TimeInterval::new_unchecked(5, 10)),
            ([-1, 1].into(), TimeInterval::new_unchecked(5, 10)),
            ([0, 0].into(), TimeInterval::new_unchecked(5, 10)),
            ([0, 1].into(), TimeInterval::new_unchecked(5, 10)),
        ];

        assert_eq!(tile_time_positions, expected_positions);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_single_gap_at_end() {
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 1, 1, 1]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![2, 2, 2, 2]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![3, 3, 3, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            // GAP
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![101, 101, 101, 110])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![102, 102, 102, 102])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![103, 103, 103, 103])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![104, 104, 104, 104])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::new_unchecked(0, 10),
            FillerTimeBounds::from(TimeInterval::new_unchecked(0, 10)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        let tile_time_positions: Vec<(GridIdx2D, TimeInterval)> = tiles
            .into_iter()
            .map(|t| {
                let g = t.unwrap();
                (g.tile_position, g.time)
            })
            .collect();

        let expected_positions = vec![
            ([-1, 0].into(), TimeInterval::new_unchecked(0, 5)),
            ([-1, 1].into(), TimeInterval::new_unchecked(0, 5)),
            ([0, 0].into(), TimeInterval::new_unchecked(0, 5)),
            ([0, 1].into(), TimeInterval::new_unchecked(0, 5)),
            ([-1, 0].into(), TimeInterval::new_unchecked(5, 10)),
            ([-1, 1].into(), TimeInterval::new_unchecked(5, 10)),
            ([0, 0].into(), TimeInterval::new_unchecked(5, 10)),
            ([0, 1].into(), TimeInterval::new_unchecked(5, 10)),
        ];

        assert_eq!(tile_time_positions, expected_positions);
    }

    #[tokio::test]
    async fn test_gaps_at_end() {
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            // GAP
            // GAP
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::new_unchecked(0, 10),
            FillerTimeBounds::from(TimeInterval::new_unchecked(0, 10)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        let tile_time_positions: Vec<(GridIdx2D, TimeInterval)> = tiles
            .into_iter()
            .map(|t| {
                let g = t.unwrap();
                (g.tile_position, g.time)
            })
            .collect();

        let expected_positions = vec![
            ([-1, 0].into(), TimeInterval::new_unchecked(0, 5)),
            ([-1, 1].into(), TimeInterval::new_unchecked(0, 5)),
            ([0, 0].into(), TimeInterval::new_unchecked(0, 5)),
            ([0, 1].into(), TimeInterval::new_unchecked(0, 5)),
            ([-1, 0].into(), TimeInterval::new_unchecked(5, 10)),
            ([-1, 1].into(), TimeInterval::new_unchecked(5, 10)),
            ([0, 0].into(), TimeInterval::new_unchecked(5, 10)),
            ([0, 1].into(), TimeInterval::new_unchecked(5, 10)),
        ];

        assert_eq!(tile_time_positions, expected_positions);
    }

    #[tokio::test]
    async fn test_one_cell_grid() {
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([0, 0], [0, 0]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::new_unchecked(0, 10),
            FillerTimeBounds::from(TimeInterval::new_unchecked(0, 10)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        let tile_time_positions: Vec<(GridIdx2D, TimeInterval)> = tiles
            .into_iter()
            .map(|t| {
                let g = t.unwrap();
                (g.tile_position, g.time)
            })
            .collect();

        let expected_positions = vec![
            ([0, 0].into(), TimeInterval::new_unchecked(0, 5)),
            ([0, 0].into(), TimeInterval::new_unchecked(5, 10)),
        ];

        assert_eq!(tile_time_positions, expected_positions);
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_no_gaps() {
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::new_unchecked(0, 10),
            FillerTimeBounds::from(TimeInterval::new_unchecked(0, 10)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        let tile_time_positions: Vec<(GridIdx2D, TimeInterval)> = tiles
            .into_iter()
            .map(|t| {
                let g = t.unwrap();
                (g.tile_position, g.time)
            })
            .collect();

        let expected_positions = vec![
            ([-1, 0].into(), TimeInterval::new_unchecked(0, 5)),
            ([-1, 1].into(), TimeInterval::new_unchecked(0, 5)),
            ([0, 0].into(), TimeInterval::new_unchecked(0, 5)),
            ([0, 1].into(), TimeInterval::new_unchecked(0, 5)),
            ([-1, 0].into(), TimeInterval::new_unchecked(5, 10)),
            ([-1, 1].into(), TimeInterval::new_unchecked(5, 10)),
            ([0, 0].into(), TimeInterval::new_unchecked(5, 10)),
            ([0, 1].into(), TimeInterval::new_unchecked(5, 10)),
        ];

        assert_eq!(tile_time_positions, expected_positions);
    }

    #[tokio::test]
    async fn test_min_max_time() {
        let data = vec![
            RasterTile2D {
                time: TimeInterval::default(),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::default(),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::default(),
            FillerTimeBounds::from(TimeInterval::default()),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        let tile_time_positions: Vec<(GridIdx2D, TimeInterval)> = tiles
            .into_iter()
            .map(|t| {
                let g = t.unwrap();
                (g.tile_position, g.time)
            })
            .collect();

        let expected_positions = vec![
            ([-1, 0].into(), TimeInterval::default()),
            ([-1, 1].into(), TimeInterval::default()),
            ([0, 0].into(), TimeInterval::default()),
            ([0, 1].into(), TimeInterval::default()),
        ];

        assert_eq!(tile_time_positions, expected_positions);
    }

    #[tokio::test]
    async fn test_error() {
        let data = vec![
            Ok(RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            }),
            Err(crate::error::Error::NoSpatialBoundsAvailable),
        ];

        let result_data = data.into_iter();

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::new_unchecked(0, 5),
            FillerTimeBounds::from(TimeInterval::new_unchecked(0, 5)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        assert_eq!(tiles.len(), 2);
        assert!(tiles[0].is_ok());
        assert!(tiles[1].is_err());
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_timeinterval_gap() {
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::new_unchecked(0, 15),
            FillerTimeBounds::from(TimeInterval::new_unchecked(0, 15)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        let tile_time_positions: Vec<(GridIdx2D, TimeInterval)> = tiles
            .into_iter()
            .map(|t| {
                let g = t.unwrap();
                (g.tile_position, g.time)
            })
            .collect();

        let expected_positions = vec![
            ([-1, 0].into(), TimeInterval::new_unchecked(0, 5)),
            ([-1, 1].into(), TimeInterval::new_unchecked(0, 5)),
            ([0, 0].into(), TimeInterval::new_unchecked(0, 5)),
            ([0, 1].into(), TimeInterval::new_unchecked(0, 5)),
            ([-1, 0].into(), TimeInterval::new_unchecked(5, 10)),
            ([-1, 1].into(), TimeInterval::new_unchecked(5, 10)),
            ([0, 0].into(), TimeInterval::new_unchecked(5, 10)),
            ([0, 1].into(), TimeInterval::new_unchecked(5, 10)),
            ([-1, 0].into(), TimeInterval::new_unchecked(10, 15)),
            ([-1, 1].into(), TimeInterval::new_unchecked(10, 15)),
            ([0, 0].into(), TimeInterval::new_unchecked(10, 15)),
            ([0, 1].into(), TimeInterval::new_unchecked(10, 15)),
        ];

        assert_eq!(tile_time_positions, expected_positions);
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_timeinterval_gap_and_end_start_gap() {
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::new_unchecked(0, 15),
            FillerTimeBounds::from(TimeInterval::new_unchecked(0, 15)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        let tile_time_positions: Vec<(GridIdx2D, TimeInterval, bool)> = tiles
            .into_iter()
            .map(|t| {
                let g = t.unwrap();
                (g.tile_position, g.time, g.is_empty())
            })
            .collect();

        let expected_positions = vec![
            ([-1, 0].into(), TimeInterval::new_unchecked(0, 5), true),
            ([-1, 1].into(), TimeInterval::new_unchecked(0, 5), false),
            ([0, 0].into(), TimeInterval::new_unchecked(0, 5), true),
            ([0, 1].into(), TimeInterval::new_unchecked(0, 5), true),
            ([-1, 0].into(), TimeInterval::new_unchecked(5, 10), true),
            ([-1, 1].into(), TimeInterval::new_unchecked(5, 10), true),
            ([0, 0].into(), TimeInterval::new_unchecked(5, 10), true),
            ([0, 1].into(), TimeInterval::new_unchecked(5, 10), true),
            ([-1, 0].into(), TimeInterval::new_unchecked(10, 15), true),
            ([-1, 1].into(), TimeInterval::new_unchecked(10, 15), true),
            ([0, 0].into(), TimeInterval::new_unchecked(10, 15), false),
            ([0, 1].into(), TimeInterval::new_unchecked(10, 15), true),
        ];

        assert_eq!(tile_time_positions, expected_positions);
    }

    #[tokio::test]
    async fn it_sets_no_cache() {
        let cache_hint1 = CacheHint::seconds(0);
        let cache_hint2 = CacheHint::seconds(60 * 60 * 24);

        let data = vec![
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: cache_hint1,
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: cache_hint2,
            },
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: cache_hint1,
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: cache_hint1,
            },
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::new_unchecked(0, 10),
            FillerTimeBounds::from(TimeInterval::new_unchecked(0, 10)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        let cache_expirations: Vec<bool> = tiles
            .into_iter()
            .map(|t| {
                let g = t.unwrap();
                g.cache_hint.is_expired()
            })
            .collect();

        let expected_expirations = vec![
            true,                     // GAP
            true,                     // GAP
            cache_hint1.is_expired(), // data
            cache_hint2.is_expired(), // data
            true,                     // GAP
            true,                     // GAP
            cache_hint1.is_expired(), // data
            cache_hint1.is_expired(), // data
        ];

        assert_eq!(cache_expirations, expected_expirations);
    }

    #[tokio::test]
    async fn it_derives_cache_hint_from_surrounding_tiles() {
        let cache_hint1 = CacheHint::seconds(60);
        let cache_hint2 = CacheHint::seconds(60 * 60 * 24);

        let data = vec![
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: cache_hint1,
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: cache_hint2,
            },
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: cache_hint1,
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: cache_hint1,
            },
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::DerivedFromSurroundingTiles,
            TimeInterval::new_unchecked(0, 10),
            FillerTimeBounds::from(TimeInterval::new_unchecked(0, 10)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        let cache_expirations: Vec<CacheExpiration> = tiles
            .into_iter()
            .map(|t| {
                let g = t.unwrap();
                g.cache_hint.expires()
            })
            .collect();

        let expected_expirations = vec![
            cache_hint1.expires(), // GAP (use first real tile)
            cache_hint1.expires(), // GAP (use first real tile)
            cache_hint1.expires(), // data
            cache_hint2.expires(), // data
            cache_hint2.expires(), // GAP (use previous tile)
            cache_hint2.expires(), // GAP (use previous tile)
            cache_hint1.expires(), // data
            cache_hint1.expires(), // data
        ];

        assert_eq!(cache_expirations, expected_expirations);
    }

    #[tokio::test]
    async fn it_derives_cache_hint_from_no_tiles() {
        let data = vec![
            // no surrounding tiles
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::DerivedFromSurroundingTiles,
            TimeInterval::new_unchecked(0, 10),
            FillerTimeBounds::from(TimeInterval::new_unchecked(0, 10)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        assert_eq!(tiles.len(), 4);

        // as there are no surrounding tiles, we have no further information and fall back to not caching
        for tile in tiles {
            assert!(tile.as_ref().unwrap().cache_hint.is_expired());
        }
    }

    #[tokio::test]
    async fn it_fills_gap_around_timestep() {
        let cache_hint1 = CacheHint::seconds(0);
        let cache_hint2 = CacheHint::seconds(60 * 60 * 24);

        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: cache_hint1,
            },
            // GAP t [0,5) pos [0, 0]
            // GAP t [5,10) pos [-1, 0]
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: cache_hint2,
            },
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 0]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::new_unchecked(0, 10),
            FillerTimeBounds::from(TimeInterval::new_unchecked(0, 10)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;
        let tiles = tiles.into_iter().map(Result::unwrap).collect::<Vec<_>>();

        assert_eq!(tiles[0].time, TimeInterval::new_unchecked(0, 5));
        assert_eq!(tiles[0].tile_position, [-1, 0].into());
        assert!(!tiles[0].is_empty());

        assert_eq!(tiles[1].time, TimeInterval::new_unchecked(0, 5));
        assert_eq!(tiles[1].tile_position, [0, 0].into());
        assert!(tiles[1].is_empty());

        assert_eq!(tiles[2].time, TimeInterval::new_unchecked(5, 10));
        assert_eq!(tiles[2].tile_position, [-1, 0].into());
        assert!(tiles[2].is_empty());

        assert_eq!(tiles[3].time, TimeInterval::new_unchecked(5, 10));
        assert_eq!(tiles[3].tile_position, [0, 0].into());
        assert!(!tiles[3].is_empty());

        assert_eq!(tiles.len(), 4);
    }

    #[tokio::test]
    async fn it_fills_multiband_streams() {
        let cache_hint1 = CacheHint::seconds(0);
        let cache_hint2 = CacheHint::seconds(60 * 60 * 24);

        let data = vec![
            // GAP t [0,5) pos[-1, 0] b 0
            // GAP t [0,5) pos[-1, 0] b 1
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: cache_hint1,
            },
            // GAP t [0,5) pos [0, 0] b 1
            // GAP t [5,10) pos [-1, 0] b 0
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: cache_hint2,
            },
            // GAP t [5,10) pos [0, 0] b 0
            // GAP t [5,10) pos [0, 0] b 1
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 0]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            2,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::new_unchecked(0, 10),
            FillerTimeBounds::from(TimeInterval::new_unchecked(0, 10)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;
        let tiles = tiles.into_iter().map(Result::unwrap).collect::<Vec<_>>();

        assert_eq!(tiles[0].time, TimeInterval::new_unchecked(0, 5));
        assert_eq!(tiles[0].tile_position, [-1, 0].into());
        assert_eq!(tiles[0].band, 0);
        assert!(tiles[0].is_empty());

        assert_eq!(tiles[1].time, TimeInterval::new_unchecked(0, 5));
        assert_eq!(tiles[1].tile_position, [-1, 0].into());
        assert_eq!(tiles[1].band, 1);
        assert!(tiles[1].is_empty());

        assert_eq!(tiles[2].time, TimeInterval::new_unchecked(0, 5));
        assert_eq!(tiles[2].tile_position, [0, 0].into());
        assert_eq!(tiles[2].band, 0);
        assert!(!tiles[2].is_empty());

        assert_eq!(tiles[3].time, TimeInterval::new_unchecked(0, 5));
        assert_eq!(tiles[3].tile_position, [0, 0].into());
        assert_eq!(tiles[3].band, 1);
        assert!(tiles[3].is_empty());

        assert_eq!(tiles[4].time, TimeInterval::new_unchecked(5, 10));
        assert_eq!(tiles[4].tile_position, [-1, 0].into());
        assert_eq!(tiles[4].band, 0);
        assert!(tiles[4].is_empty());

        assert_eq!(tiles[5].time, TimeInterval::new_unchecked(5, 10));
        assert_eq!(tiles[5].tile_position, [-1, 0].into());
        assert_eq!(tiles[5].band, 1);
        assert!(!tiles[5].is_empty());

        assert_eq!(tiles[6].time, TimeInterval::new_unchecked(5, 10));
        assert_eq!(tiles[6].tile_position, [0, 0].into());
        assert_eq!(tiles[6].band, 0);
        assert!(tiles[6].is_empty());

        assert_eq!(tiles[7].time, TimeInterval::new_unchecked(5, 10));
        assert_eq!(tiles[7].tile_position, [0, 0].into());
        assert_eq!(tiles[7].band, 1);
        assert!(tiles[7].is_empty());

        assert_eq!(tiles.len(), 8);
    }

    #[tokio::test]
    async fn it_detects_non_increasing_intervals() {
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::new_unchecked(0, 5),
            FillerTimeBounds::from(TimeInterval::new_unchecked(0, 5)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        assert_eq!(tiles.len(), 5);
        assert!(tiles[4].is_err());
    }

    #[tokio::test]
    async fn it_detects_non_increasing_instants() {
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 0),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 0),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 0),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 0),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 0),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 0),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 0),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 0),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::new_unchecked(0, 1),
            FillerTimeBounds::from(TimeInterval::new_unchecked(0, 1)),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        assert_eq!(tiles.len(), 5);
        assert!(tiles[4].is_err());
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_timeinterval_fill_data_bounds() {
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            1,
            global_geo_transform,
            tile_shape,
            FillerTileCacheExpirationStrategy::NoCache,
            TimeInterval::new_unchecked(0, 20),
            FillerTimeBounds::from(TimeInterval::default()),
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        let tile_time_positions: Vec<(GridIdx2D, TimeInterval, bool)> = tiles
            .into_iter()
            .map(|t| {
                let g = t.unwrap();
                (g.tile_position, g.time, g.is_empty())
            })
            .collect();

        let expected_positions = vec![
            (
                [-1, 0].into(),
                TimeInterval::new_unchecked(TimeInstance::MIN, 5),
                true,
            ),
            (
                [-1, 1].into(),
                TimeInterval::new_unchecked(TimeInstance::MIN, 5),
                true,
            ),
            (
                [0, 0].into(),
                TimeInterval::new_unchecked(TimeInstance::MIN, 5),
                true,
            ),
            (
                [0, 1].into(),
                TimeInterval::new_unchecked(TimeInstance::MIN, 5),
                true,
            ),
            ([-1, 0].into(), TimeInterval::new_unchecked(5, 10), true),
            ([-1, 1].into(), TimeInterval::new_unchecked(5, 10), false),
            ([0, 0].into(), TimeInterval::new_unchecked(5, 10), true),
            ([0, 1].into(), TimeInterval::new_unchecked(5, 10), true),
            ([-1, 0].into(), TimeInterval::new_unchecked(10, 15), true),
            ([-1, 1].into(), TimeInterval::new_unchecked(10, 15), true),
            ([0, 0].into(), TimeInterval::new_unchecked(10, 15), false),
            ([0, 1].into(), TimeInterval::new_unchecked(10, 15), true),
            (
                [-1, 0].into(),
                TimeInterval::new_unchecked(15, TimeInstance::MAX),
                true,
            ),
            (
                [-1, 1].into(),
                TimeInterval::new_unchecked(15, TimeInstance::MAX),
                true,
            ),
            (
                [0, 0].into(),
                TimeInterval::new_unchecked(15, TimeInstance::MAX),
                true,
            ),
            (
                [0, 1].into(),
                TimeInterval::new_unchecked(15, TimeInstance::MAX),
                true,
            ),
        ];

        assert_eq!(tile_time_positions, expected_positions);
    }
}
