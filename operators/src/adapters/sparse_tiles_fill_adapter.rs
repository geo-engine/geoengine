use crate::util::Result;
use futures::{ready, Stream};
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, SpatialQuery, TimeInterval},
    raster::{
        EmptyGrid2D, GeoTransform, GridBoundingBox2D, GridBounds, GridIdx2D, GridShape2D, GridStep,
        Pixel, RasterTile2D, TilingSpecification, TilingStrategy,
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

#[derive(Debug, PartialEq, Clone)]
struct StateContainer<T> {
    current_idx: GridIdx2D,
    current_time: TimeInterval,
    next_tile: Option<RasterTile2D<T>>,
    no_data_grid: EmptyGrid2D<T>,
    grid_bounds: GridBoundingBox2D,
    global_geo_transform: GeoTransform,
    state: State,
}

impl<T: Pixel> StateContainer<T> {
    /// Create a new no-data `RasterTile2D` with `GridIdx` and time from the current state
    fn current_no_data_tile(&self) -> RasterTile2D<T> {
        RasterTile2D::new(
            self.current_time,
            self.current_idx,
            self.global_geo_transform,
            self.no_data_grid.into(),
        )
    }

    /// Check if the next tile to produce is the stored one
    fn is_next_tile_stored(&self) -> bool {
        if let Some(t) = &self.next_tile {
            // The stored tile is the one we are looking for if the tile position is the next to produce
            self.grid_idx_is_the_next_to_produce(t.tile_position) &&
            // and the time equals the current state time
                (self.time_equals_current_state(t.time)
                // or it starts a new time step, and the time directly follows the current state time
                || (self.current_idx_is_first_in_grid_run() && self.time_directly_following_current_state(t.time)))
        } else {
            false
        }
    }

    /// Get the next `GridIdx` following the current state `GridIdx`. None if the current `GridIdx` is the max `GridIdx` of the grid.
    fn maybe_next_idx(&self) -> Option<GridIdx2D> {
        self.grid_bounds.inc_idx_unchecked(self.current_idx, 1)
    }

    /// Get the next `GridIdx` following the current state Grid`GridIdx`Idx. Returns the minimal `GridIdx` if the current `GridIdx` is the max `GridIdx`.
    fn wrapped_next_idx(&self) -> GridIdx2D {
        self.grid_bounds
            .inc_idx_unchecked(self.current_idx, 1)
            .unwrap_or_else(|| self.min_index())
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
        time.start() < self.current_time.start()
    }

    /// Check if a `TimeInterval` start equals the start of the current state `TimeInterval`.
    fn time_starts_equals_current_state(&self, time: TimeInterval) -> bool {
        time.start() == self.current_time.start()
    }

    /// Check if a `TimeInterval` duration equals the duration of the current state `TimeInterval`.
    fn time_duration_equals_current_state(&self, time: TimeInterval) -> bool {
        time.duration_ms() == self.current_time.duration_ms()
    }

    /// Check if a `TimeInterval` equals the current state `TimeInterval`.
    fn time_equals_current_state(&self, time: TimeInterval) -> bool {
        time == self.current_time
    }

    /// Check if a `GridIdx` is the next to produce i.e. the current state `GridIdx`.
    fn grid_idx_is_the_next_to_produce(&self, tile_idx: GridIdx2D) -> bool {
        tile_idx == self.current_idx
    }

    /// Check if a `TimeInterval` is directly connected to the end of the current state `TimeInterval`.
    fn time_directly_following_current_state(&self, time: TimeInterval) -> bool {
        if self.current_time.is_instant() {
            self.current_time.end() + 1 == time.start()
        } else {
            self.current_time.end() == time.start()
        }
    }

    /// Check if the current state `GridIdx`  is the first of a grid run i.e. it equals the minimal `GridIdx` .
    fn current_idx_is_first_in_grid_run(&self) -> bool {
        self.current_idx == self.min_index()
    }

    /// Check if the current state `GridIdx`  is the first of a grid run i.e. it equals the minimal `GridIdx` .
    fn current_idx_is_last_in_grid_run(&self) -> bool {
        self.current_idx == self.max_index()
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
    pub fn new(
        stream: S,
        tile_grid_bounds: GridBoundingBox2D,
        global_geo_transform: GeoTransform,
        tile_shape: GridShape2D,
    ) -> Self {
        SparseTilesFillAdapter {
            stream,
            sc: StateContainer {
                current_idx: tile_grid_bounds.min_index(),
                current_time: TimeInterval::default(),
                global_geo_transform,
                grid_bounds: tile_grid_bounds,
                next_tile: None,
                no_data_grid: EmptyGrid2D::new(tile_shape),
                state: State::Initial,
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
        query_rect_to_answer: RasterQueryRectangle,
        tiling_spec: TilingSpecification,
    ) -> Self {
        assert_eq!(
            query_rect_to_answer
                .spatial_query()
                .geo_transform
                .origin_coordinate,
            tiling_spec.origin_coordinate,
            "we currently only support tiling specifications with the same origin coordinate as the query rectangle"
        );

        // FIXME: we should not need to create a new tiling strategy here
        let tiling_strat = TilingStrategy::new(
            tiling_spec.tile_size_in_pixels,
            query_rect_to_answer.spatial_query().geo_transform,
        );

        let grid_bounds = tiling_strat
            .raster_spatial_query_to_tling_grid_box(&query_rect_to_answer.spatial_query());
        Self::new(
            stream,
            grid_bounds,
            tiling_strat.geo_transform,
            tiling_spec.tile_size_in_pixels,
        )
    }

    // TODO: return Result with SparseTilesFillAdapterError and map it to Error in the poll_next method if possible
    #[allow(clippy::too_many_lines)]
    pub fn next_step(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<RasterTile2D<T>>>> {
        let min_idx = self.sc.min_index();
        let wrapped_next_idx = self.sc.wrapped_next_idx();

        let mut this = self.project();

        match this.sc.state {
            // this is the initial state.
            State::Initial => {
                // there must not be a grid stored
                debug_assert!(!this.sc.is_any_tile_stored());
                // poll for a first (input) tile
                let result_tile = match ready!(this.stream.as_mut().poll_next(cx)) {
                    Some(Ok(tile)) => {
                        // this is a the first tile ever
                        // in any case the tiles time is the first time interval /  instant we can produce
                        this.sc.current_time = tile.time;

                        if this.sc.grid_idx_is_the_next_to_produce(tile.tile_position) {
                            this.sc.state = State::PollingForNextTile; // return the received tile and set state to polling for the next tile
                            tile
                        } else {
                            this.sc.next_tile = Some(tile);
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
                        this.sc.current_no_data_tile()
                    }
                };
                // move the current_idx. There is no need to do time progress here. Either a new tile triggers that or it is never needed for an empty source.
                this.sc.current_idx = wrapped_next_idx;
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
                                current_start: this.sc.current_time,
                                tile_start: tile.time,
                            }
                            .into(),
                        )));
                        }
                        // 2. a) The received TimeInterval with start EQUAL to the current TimeInterval MUST NOT have a differend duration / end.
                        if this.sc.time_starts_equals_current_state(tile.time)
                            && !this.sc.time_duration_equals_current_state(tile.time)
                        {
                            this.sc.state = State::Ended;
                            return Poll::Ready(Some(Err(
                                SparseTilesFillAdapterError::TileTimeIntervalLengthMissmatch {
                                    tile_interval: tile.time,
                                    current_interval: this.sc.current_time,
                                }
                                .into(),
                            )));
                        };

                        // 2 b) The received TimeInterval with start EQUAL to the current TimeInterval MUST NOT have a different duration / end.
                        let next_tile = if this.sc.time_equals_current_state(tile.time) {
                            if this.sc.grid_idx_is_the_next_to_produce(tile.tile_position) {
                                // the tile is the next to produce. Return it and set state to polling for the next tile.
                                this.sc.state = State::PollingForNextTile;
                                tile
                            } else {
                                // the tile is not the next to produce. Save it and go to fill mode.
                                this.sc.next_tile = Some(tile);
                                this.sc.state = State::FillAndProduceNextTile;
                                this.sc.current_no_data_tile()
                            }
                        }
                        // 3. The received tile has a TimeInterval that directly continues the current TimeInterval.
                        else if this.sc.time_directly_following_current_state(tile.time) {
                            // if the current_idx is the first in a new grid run then it is the first one with the new TimeInterval.
                            // this switches the time in the state to the time of the new tile.
                            if this.sc.current_idx_is_first_in_grid_run() {
                                if this.sc.grid_idx_is_the_next_to_produce(tile.tile_position) {
                                    // return the tile and set state to polling for the next tile.
                                    this.sc.current_time = tile.time;
                                    this.sc.state = State::PollingForNextTile;
                                    tile
                                } else {
                                    // save the tile and go to fill mode.
                                    this.sc.current_time = tile.time;
                                    this.sc.next_tile = Some(tile);
                                    this.sc.state = State::FillAndProduceNextTile;
                                    this.sc.current_no_data_tile()
                                }
                            } else {
                                // the revieved tile is in a new TimeInterval but we still need to finish the current one. Store tile and go to fill mode.
                                this.sc.next_tile = Some(tile);
                                this.sc.state = State::FillAndProduceNextTile;
                                this.sc.current_no_data_tile()
                            }
                        }
                        // 4. The received tile has a TimeInterval that starts after the current TimeInterval and is not directly connected to the current TimeInterval.
                        else {
                            // if the current_idx is the first in a new grid run then it is the first one with a new TimeInterval.
                            // We need to generate a fill TimeInterval since current and tile TimeInterval are not connedted.
                            if this.sc.current_idx_is_first_in_grid_run() {
                                this.sc.current_time = TimeInterval::new(
                                    this.sc.current_time.end(),
                                    tile.time.start(),
                                )?;
                                this.sc.next_tile = Some(tile);
                                this.sc.state = State::FillAndProduceNextTile;
                                this.sc.current_no_data_tile()
                            } else {
                                // the received tile is in a new TimeInterval but we still need to finish the current one. Store tile and go to fill mode.
                                this.sc.next_tile = Some(tile);
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
                        if this.sc.current_idx_is_first_in_grid_run() {
                            // there was a tile and it flipped the state index to the first one. => we are done.
                            this.sc.state = State::Ended;
                            None
                        } else if this.sc.current_idx_is_last_in_grid_run() {
                            // this is the last tile
                            this.sc.state = State::Ended;
                            Some(Ok(this.sc.current_no_data_tile()))
                        } else {
                            // there was a tile and it was not the last one. => go to fill to end mode.
                            this.sc.state = State::FillToEnd;
                            Some(Ok(this.sc.current_no_data_tile()))
                        }
                    }
                };
                // move the current_idx. There is no need to do time progress here. Either a new tile sets that or it is not needed to fill to the end of the grid.
                this.sc.current_idx = wrapped_next_idx;
                Poll::Ready(res)
            }
            // the tile to produce is the the one stored
            State::FillAndProduceNextTile if this.sc.is_next_tile_stored() => {
                // take the tile (replace in state with NONE)
                let next_tile = this.sc.next_tile.take().expect("checked by case");
                debug_assert!(this.sc.current_idx == next_tile.tile_position);

                this.sc.current_time = next_tile.time;
                this.sc.current_idx = wrapped_next_idx;
                this.sc.state = State::PollingForNextTile;

                Poll::Ready(Some(Ok(next_tile)))
            }
            // this is the state where we produce fill tiles until another state is reached.
            State::FillAndProduceNextTile => {
                let stored_tile_time = this
                    .sc
                    .next_tile
                    .as_ref()
                    .map(|t| t.time)
                    .expect("next_tile must be set in NextTile state");

                let (next_idx, next_time) = match this.sc.maybe_next_idx() {
                    // the next GridIdx is in the current TimeInterval
                    Some(idx) => (idx, this.sc.current_time),
                    // the next GridIdx is in the next TimeInterval
                    None => {
                        if this
                            .sc
                            .time_directly_following_current_state(stored_tile_time)
                        {
                            (this.sc.min_index(), stored_tile_time)
                        } else {
                            // the next GridIdx is not in the next TimeInterval. We need to create a new intermediate TimeInterval.
                            (
                                this.sc.min_index(),
                                TimeInterval::new(
                                    this.sc.current_time.end(),
                                    stored_tile_time.start(),
                                )?,
                            )
                        }
                    }
                };

                let no_data_tile = this.sc.current_no_data_tile();

                this.sc.current_time = next_time;
                this.sc.current_idx = next_idx;

                Poll::Ready(Some(Ok(no_data_tile)))
            }
            // this is  the last tile to produce ever
            State::FillToEnd if this.sc.current_idx_is_last_in_grid_run() => {
                this.sc.state = State::Ended;
                Poll::Ready(Some(Ok(this.sc.current_no_data_tile())))
            }
            // there are more tiles to produce to fill the grid
            State::FillToEnd => {
                let no_data_tile = this.sc.current_no_data_tile();
                this.sc.current_idx = wrapped_next_idx;
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

#[cfg(test)]
mod tests {
    use futures::{stream, StreamExt};
    use geoengine_datatypes::{primitives::TimeInterval, raster::Grid, util::test::TestDefault};

    use super::*;

    #[tokio::test]
    async fn test_gap_overlaps_time_step() {
        let data = vec![
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
            // GAP
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
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
            global_geo_transform,
            tile_shape,
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
            global_geo_transform,
            tile_shape,
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
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
            },
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
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
            global_geo_transform,
            tile_shape,
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
    async fn test_single_gap_at_end() {
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 1, 1, 1]).unwrap().into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![2, 2, 2, 2]).unwrap().into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![3, 3, 3, 3]).unwrap().into(),
                properties: Default::default(),
            },
            // GAP
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![101, 101, 101, 110])
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![102, 102, 102, 102])
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![103, 103, 103, 103])
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![104, 104, 104, 104])
                    .unwrap()
                    .into(),
                properties: Default::default(),
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
            global_geo_transform,
            tile_shape,
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
            // GAP
            // GAP
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
            global_geo_transform,
            tile_shape,
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
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
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
            global_geo_transform,
            tile_shape,
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
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
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
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
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
            global_geo_transform,
            tile_shape,
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
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
            },
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::default(),
                tile_position: [0, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
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
            global_geo_transform,
            tile_shape,
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
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
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
            global_geo_transform,
            tile_shape,
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
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [0, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
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
            global_geo_transform,
            tile_shape,
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
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
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
            global_geo_transform,
            tile_shape,
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
}
