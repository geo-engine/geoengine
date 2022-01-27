use crate::util::Result;
use futures::{ready, Stream};
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, SpatialPartitioned, TimeInterval},
    raster::{
        EmptyGrid2D, GeoTransform, GridBoundingBox2D, GridBounds, GridIdx2D, GridShape2D, GridStep,
        Pixel, RasterTile2D, TilingSpecification,
    },
};
use pin_project::pin_project;
use snafu::Snafu;
use std::{pin::Pin, task::Poll};

#[derive(Debug, Snafu)]
pub enum SparseTilesFillAdapterError {
    #[snafu(display(
        "Received tile TimeInterval ({}) starts before current TimeInterval: {}",
        tile_start,
        current_start
    ))]
    TileTimeIntervalStartBeforeCurrentStart {
        tile_start: TimeInterval,
        current_start: TimeInterval,
    },
    #[snafu(display(
        "Received tile TimeInterval ({}) length differs from received TimeInterval with equal start: {}",
        tile_interval,
        current_interval
    ))]
    TileTimeIntervalLengthMissmatch {
        tile_interval: TimeInterval,
        current_interval: TimeInterval,
    },
    #[snafu(display(
        "Received tile TimeInterval ({}) is the first in a grid run but does no time progress. (Old time: {})",
        tile_interval,
        current_interval
    ))]
    GridWrapMustDoTimeProgress {
        tile_interval: TimeInterval,
        current_interval: TimeInterval,
    },
}

#[derive(Debug, PartialEq, Clone, Copy)]
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
    fn current_no_data_tile(&self) -> RasterTile2D<T> {
        RasterTile2D::new(
            self.current_time,
            self.current_idx,
            self.global_geo_transform,
            self.no_data_grid.into(),
        )
    }

    fn is_next_tile_stored(&self) -> bool {
        if let Some(t) = &self.next_tile {
            t.tile_position == self.current_idx && t.time == self.current_time
        } else {
            false
        }
    }

    fn maybe_next_idx(&self) -> Option<GridIdx2D> {
        self.grid_bounds.inc_idx_unchecked(self.current_idx, 1)
    }

    fn wrapped_next_idx(&self) -> GridIdx2D {
        self.grid_bounds
            .inc_idx_unchecked(self.current_idx, 1)
            .unwrap_or_else(|| self.min_index())
    }

    fn min_index(&self) -> GridIdx2D {
        self.grid_bounds.min_index()
    }

    fn max_index(&self) -> GridIdx2D {
        self.grid_bounds.max_index()
    }

    fn is_any_tile_stored(&self) -> bool {
        self.next_tile.is_some()
    }

    fn time_starts_before_current_state(&self, time: TimeInterval) -> bool {
        time.start() < self.current_time.start()
    }

    fn time_starts_equals_current_state(&self, time: TimeInterval) -> bool {
        time.start() == self.current_time.start()
    }

    fn time_duration_equals_current_state(&self, time: TimeInterval) -> bool {
        time.duration_ms() == self.current_time.duration_ms()
    }

    fn time_equals_current_state(&self, time: TimeInterval) -> bool {
        time == self.current_time
    }

    fn grid_idx_is_the_next_to_produce(&self, tile_idx: GridIdx2D) -> bool {
        tile_idx == self.current_idx
    }
    fn time_directly_following_current_state(&self, time: TimeInterval) -> bool {
        if self.current_time.is_instant() {
            self.current_time.end() + 1 == time.start()
        } else {
            self.current_time.end() == time.start()
        }
    }
    fn current_idx_is_first_in_grid_run(&self) -> bool {
        self.current_idx == self.min_index()
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
        no_data_value: T,
    ) -> Self {
        SparseTilesFillAdapter {
            stream,
            sc: StateContainer {
                current_idx: tile_grid_bounds.min_index(),
                current_time: TimeInterval::default(),
                global_geo_transform,
                grid_bounds: tile_grid_bounds,
                next_tile: None,
                no_data_grid: EmptyGrid2D::new(tile_shape, no_data_value),
                state: State::Initial,
            },
        }
    }

    pub fn new_like_subquery(
        stream: S,
        query_rect_to_answer: RasterQueryRectangle,
        tiling_spec: TilingSpecification,
        no_data_value: T,
    ) -> Self {
        debug_assert!(query_rect_to_answer.spatial_resolution.y > 0.);

        let tiling_strat = tiling_spec.strategy(
            query_rect_to_answer.spatial_resolution.x,
            -query_rect_to_answer.spatial_resolution.y,
        );

        let grid_bounds = tiling_strat.tile_grid_box(query_rect_to_answer.spatial_partition());
        Self::new(
            stream,
            grid_bounds,
            tiling_strat.geo_transform,
            tiling_spec.tile_size_in_pixels,
            no_data_value,
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
            State::Initial => {
                // poll for a first (input) tile
                let res = match ready!(this.stream.as_mut().poll_next(cx)) {
                    // this is a the first tile ever
                    Some(Ok(tile)) => {
                        // to be in this state we must await a first tile in a grid run
                        debug_assert!(this.sc.current_idx == min_idx);
                        // there must not be a grid stored
                        debug_assert!(!this.sc.is_any_tile_stored());

                        // in any case the tiles time is the first time interval /  instant we can produce
                        this.sc.current_time = tile.time;

                        if this.sc.current_idx == tile.tile_position {
                            // it is the the first in the grid AND therefore the first one to return.
                            // this.sc.current_time = tile.time;
                            this.sc.state = State::PollingForNextTile;
                            Some(Ok(tile))
                        } else {
                            // this.sc.current_time = tile.time;
                            this.sc.next_tile = Some(tile);
                            this.sc.state = State::FillAndProduceNextTile;
                            Some(Ok(this.sc.current_no_data_tile())) // it is important to generate the no data tile here since we need the time
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
                        Some(Ok(this.sc.current_no_data_tile()))
                    }
                };
                // move the current_idx. There is no need to do time progress here. Either a new tile triggers that or it is never needed for an empty source.
                this.sc.current_idx = wrapped_next_idx;
                Poll::Ready(res)
            }
            State::PollingForNextTile => {
                let res = match ready!(this.stream.as_mut().poll_next(cx)) {
                    // CHECK BASIC ASSERTIONS

                    // The start of the recieved TimeInterval MUST NOT BE before the start of the current TimeInterval.
                    Some(Ok(tile)) => {
                        debug_assert!(!this.sc.is_any_tile_stored());
                        dbg!(tile.tile_position, tile.time);

                        // A received tile MUST not have a time start before the current time.
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
                        // A received TimeInterval with start EQUAL to the current TimeInterval MUST NOT differ in the end
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

                        let next_tile = if this.sc.time_equals_current_state(tile.time) {
                            // TILE IN CURRENT GRID RUN / TIME INTERVAL
                            debug_assert!(tile.time.start() == this.sc.current_time.start());

                            if this.sc.grid_idx_is_the_next_to_produce(tile.tile_position) {
                                debug_assert!(this.sc.current_idx == tile.tile_position);
                                dbg!("0___n");
                                // Time and idx are EQUAL to the current ones (to produce).
                                // Therefore, it is the one to produce in this call.

                                this.sc.current_time = tile.time; // time is equal so dont to update it
                                this.sc.state = State::PollingForNextTile;
                                tile
                            } else {
                                debug_assert!(this.sc.current_idx != tile.tile_position);
                                dbg!("0___g");
                                // TimeInterval is EQUAL to the current ones (to produce).
                                // GridIdx is NOT EQUAL the current one (to produce).
                                // Therefore, return empty tile and go to FillAndProduceNextTile.
                                this.sc.next_tile = Some(tile);
                                this.sc.state = State::FillAndProduceNextTile;
                                this.sc.current_no_data_tile()
                            }
                        } else if this.sc.time_directly_following_current_state(tile.time) {
                            debug_assert!(
                                tile.time.start() == this.sc.current_time.end()
                                    || (this.sc.current_time.is_instant()
                                        && this.sc.current_time.end() + 1 == tile.time.start())
                            );
                            // TILE IN NEXT GRID RUN / TIME INTERVAL
                            if this.sc.current_idx_is_first_in_grid_run() {
                                debug_assert!(this.sc.current_idx == this.sc.min_index());
                                if this.sc.grid_idx_is_the_next_to_produce(tile.tile_position) {
                                    debug_assert!(this.sc.current_idx == tile.tile_position);
                                    dbg!("1_f_n");
                                    // We are at the start of a new grid run => The tile we received is the first in an new TimeInstant.
                                    // The tile GridIdx is equal to the GridIdx (to produce).
                                    // AND the current TimeInterval is a TimeInstant.
                                    // AND the tiles start TimeInstant is the direct follower of the current TimeInstant (end + 1 = start).
                                    // Therefore, it is the one to produce in this call.

                                    this.sc.current_time = tile.time;
                                    this.sc.state = State::PollingForNextTile;
                                    tile
                                } else {
                                    debug_assert!(this.sc.current_idx != tile.tile_position);
                                    dbg!("1_f_g");
                                    // The tile GridIdx is NOT EQUAL to the GridIdx (to produce) => Tile is in a new TimeInterval but not the current AND first one.
                                    // AND the tiles TimeInterval starts where the current/previous TimeInterval ends => use tile.time as current_time.
                                    this.sc.current_time = tile.time;
                                    this.sc.next_tile = Some(tile);
                                    this.sc.state = State::FillAndProduceNextTile;
                                    this.sc.current_no_data_tile()
                                }
                            } else {
                                dbg!("1_i_g");
                                this.sc.next_tile = Some(tile);
                                this.sc.state = State::FillAndProduceNextTile;
                                this.sc.current_no_data_tile()
                            }
                        } else {
                            // GAP BETWEEN CURRENT AND NEXT GRID RUN / TIME INTERVAL
                            debug_assert!(this.sc.current_time.end() < tile.time.start()); // should be impossible

                            if this.sc.current_idx_is_first_in_grid_run() {
                                dbg!("2_f_g");

                                this.sc.current_time = TimeInterval::new(
                                    this.sc.current_time.end(),
                                    tile.time.start(),
                                )?;
                                this.sc.next_tile = Some(tile);
                                this.sc.state = State::FillAndProduceNextTile;
                                this.sc.current_no_data_tile()
                            } else {
                                dbg!("2_i_g");

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
                        if this.sc.current_idx == min_idx {
                            // there was a tile and it flipped the next index to the first one. => we are done.
                            this.sc.state = State::Ended;
                            None
                        } else if this.sc.current_idx == this.sc.max_index() {
                            // this is the last tile
                            this.sc.state = State::Ended;
                            Some(Ok(this.sc.current_no_data_tile()))
                        } else {
                            // there was a tile and it was not the last one. => fill to end.
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
                debug_assert!(this.sc.current_time == next_tile.time);
                debug_assert!(this.sc.current_idx == next_tile.tile_position);

                this.sc.current_time = next_tile.time;
                this.sc.current_idx = wrapped_next_idx;
                this.sc.state = State::PollingForNextTile;

                Poll::Ready(Some(Ok(next_tile)))
            }
            State::FillAndProduceNextTile => {
                let stored_tile_time = this
                    .sc
                    .next_tile
                    .as_ref()
                    .map(|t| t.time)
                    .expect("next_tile must be set in NextTile state");
                dbg!("State::FillAndProduceNextTile A", stored_tile_time);
                let (next_idx, next_time) = match this.sc.maybe_next_idx() {
                    // the next GridIdx is in the current TimeInterval
                    Some(idx) => {
                        dbg!("0");
                        (idx, this.sc.current_time)
                    }
                    // the next GridIdx is in the next TimeInstant which is the one of the next tile
                    None => {
                        if this.sc.current_time.is_instant()
                            && this.sc.current_time.end() + 1 == stored_tile_time.start()
                        {
                            dbg!("1");
                            (this.sc.min_index(), stored_tile_time)
                        } else if this.sc.current_time.end() == stored_tile_time.start() {
                            dbg!("2");
                            (this.sc.min_index(), stored_tile_time)
                        } else {
                            // the next GridIdx is in the next TimeInterval which is BEFORE the one of the next tile
                            dbg!("3");
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
                dbg!("State::FillAndProduceNextTile B", next_time);

                debug_assert!(next_time.start() >= this.sc.current_time.start());

                let no_data_tile = this.sc.current_no_data_tile();

                this.sc.current_time = next_time;
                this.sc.current_idx = next_idx;

                Poll::Ready(Some(Ok(no_data_tile)))
            }
            // this is  the last tile to produce ever
            State::FillToEnd if this.sc.current_idx == this.sc.max_index() => {
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
            // GAP
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], no_data_value)
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
        let no_data_value = 0;

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            global_geo_transform,
            tile_shape,
            no_data_value,
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
        let no_data_value = 0;

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            global_geo_transform,
            tile_shape,
            no_data_value,
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
        let no_data_value = Some(0);
        let data = vec![
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], no_data_value)
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
        let no_data_value = 0;

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            global_geo_transform,
            tile_shape,
            no_data_value,
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
            // GAP
            // GAP
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
            // GAP
            // GAP
        ];

        let result_data = data.into_iter().map(Ok);

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();
        let no_data_value = 0;

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            global_geo_transform,
            tile_shape,
            no_data_value,
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
        let no_data_value = Some(0);
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], no_data_value)
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
        let no_data_value = 0;

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            global_geo_transform,
            tile_shape,
            no_data_value,
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
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
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
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], no_data_value)
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
        let no_data_value = 0;

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            global_geo_transform,
            tile_shape,
            no_data_value,
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
        let no_data_value = Some(0);
        let data = vec![
            RasterTile2D {
                time: TimeInterval::default(),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            // GAP
            // GAP
            RasterTile2D {
                time: TimeInterval::default(),
                tile_position: [0, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10], no_data_value)
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
        let no_data_value = 0;

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            global_geo_transform,
            tile_shape,
            no_data_value,
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
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], Some(0))
                    .unwrap()
                    .into(),
                properties: Default::default(),
            }),
            Err(crate::error::Error::NoSpatialBoundsAvailable),
        ];

        let result_data = data.into_iter();

        let in_stream = stream::iter(result_data);
        let grid_bounding_box = GridBoundingBox2D::new([-1, 0], [0, 1]).unwrap();
        let global_geo_transform = GeoTransform::test_default();
        let tile_shape = [2, 2].into();
        let no_data_value = 0;

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            global_geo_transform,
            tile_shape,
            no_data_value,
        );

        let tiles: Vec<Result<RasterTile2D<i32>>> = adapter.collect().await;

        assert_eq!(tiles.len(), 2);
        assert!(tiles[0].is_ok());
        assert!(tiles[1].is_err());
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_timeinterval_gap() {
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
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [0, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], no_data_value)
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
        let no_data_value = 0;

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            global_geo_transform,
            tile_shape,
            no_data_value,
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
        let no_data_value = Some(0);
        let data = vec![
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
                time: TimeInterval::new_unchecked(10, 15),
                tile_position: [0, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], no_data_value)
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
        let no_data_value = 0;

        let adapter = SparseTilesFillAdapter::new(
            in_stream,
            grid_bounding_box,
            global_geo_transform,
            tile_shape,
            no_data_value,
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
