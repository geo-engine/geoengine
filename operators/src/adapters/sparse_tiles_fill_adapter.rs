use std::{pin::Pin, task::Poll};

use futures::{ready, Stream};
use geoengine_datatypes::{
    primitives::TimeInterval,
    raster::{
        EmptyGrid2D, GeoTransform, GridBoundingBox2D, GridBounds, GridIdx2D, GridShape2D, GridStep,
        Pixel, RasterTile2D,
    },
    util::Result,
};
use pin_project::pin_project;

#[derive(Debug, PartialEq, Clone, Copy)]
enum StateInner {
    Initial,
    BetweenTiles,
    NextTile,
    FillToEnd,
    Ended,
}

#[derive(Debug, PartialEq, Clone)]
struct StateOuter<T> {
    current_idx: GridIdx2D,
    current_time: TimeInterval,
    next_tile: Option<RasterTile2D<T>>,
    no_data_grid: EmptyGrid2D<T>,
    grid_bounds: GridBoundingBox2D,
    global_geo_transform: GeoTransform,
    state: StateInner,
}

impl<T> StateOuter<T>
where
    T: Pixel,
{
    fn select_next_tile(&self) -> bool {
        if let Some(t) = &self.next_tile {
            t.tile_position == self.current_idx && t.time == self.current_time
        } else {
            false
        }
    }

    pub fn poll(&self) -> bool {
        matches!(self.state, StateInner::Initial | StateInner::BetweenTiles)
    }

    pub fn ended(&self) -> bool {
        matches!(self.state, StateInner::Ended)
    }

    pub fn emergency_break(&mut self) {
        self.state = StateInner::Ended;
    }

    pub fn please_end_it(&mut self) {
        let min_idx = self.grid_bounds.min_index();
        match self.state {
            // there was never a tile. Fill with default time interval.
            StateInner::Initial => {
                self.state = StateInner::FillToEnd;
                debug_assert!(self.current_idx == min_idx);
                self.current_idx = min_idx;
                self.current_time = TimeInterval::default();
            }
            // there was a tile and it flipped the next index to the first one. => we are done.
            StateInner::BetweenTiles if self.current_idx == min_idx => {
                self.state = StateInner::Ended
            }
            // there was a tile and it was not the last one. => fill to end.
            StateInner::BetweenTiles => self.state = StateInner::FillToEnd,
            _ => panic!("invalid state transition!"),
        }
    }

    pub fn new_tile(&mut self, tile: RasterTile2D<T>) {
        match self.state {
            // the tile is the first one. Fill and produce the tile.
            StateInner::Initial => {
                self.state = StateInner::NextTile;
                debug_assert!(self.current_idx == self.grid_bounds.min_index());
                self.current_time = tile.time;
                debug_assert!(self.next_tile.is_none());
                self.next_tile = Some(tile);
            }
            // there was a previous tile. Fill the gap and return the tile.
            StateInner::BetweenTiles => {
                self.state = StateInner::NextTile;
                debug_assert!(self.next_tile.is_none());
                debug_assert!(tile.time.start() >= self.current_time.start());
                self.next_tile = Some(tile);
            }
            _ => panic!("invlid state transition!"),
        }
    }

    pub fn next_tile(&mut self) -> RasterTile2D<T> {
        let maybe_next_idx = self.grid_bounds.fwd_idx_unchecked(self.current_idx, 1);
        match self.state {
            // the next tile required is the one stored
            StateInner::NextTile if self.select_next_tile() => {
                // take the tile (replace in state with NONE)
                let next_tile = self.next_tile.take().expect("checked by case");
                // calculate the next idx
                let next_tile_idx = maybe_next_idx.unwrap_or_else(|| self.grid_bounds.min_index());

                debug_assert!(self.current_time == next_tile.time);
                debug_assert!(self.current_idx == next_tile.tile_position);
                self.current_idx = next_tile_idx;
                self.state = StateInner::BetweenTiles;

                next_tile
            }
            StateInner::NextTile => {
                let next_time = self
                    .next_tile
                    .as_ref()
                    .map(|t| t.time)
                    .expect("next_tile must be set in NextTile state");

                let no_data_tile = RasterTile2D::new(
                    self.current_time,
                    self.current_idx,
                    self.global_geo_transform,
                    self.no_data_grid.into(),
                );

                let (next_idx, next_time) = match maybe_next_idx {
                    Some(idx) => (idx, self.current_time),
                    None => (self.grid_bounds.min_index(), next_time),
                };

                self.current_idx = next_idx;
                self.current_time = next_time;
                self.state = StateInner::NextTile;

                no_data_tile
            }
            // produce tiles until all are done.
            StateInner::FillToEnd if self.current_idx == self.grid_bounds.max_index() => {
                let no_data_tile = RasterTile2D::new(
                    self.current_time,
                    self.current_idx,
                    self.global_geo_transform,
                    self.no_data_grid.into(),
                );

                self.state = StateInner::Ended;
                no_data_tile
            }
            StateInner::FillToEnd => {
                let no_data_tile = RasterTile2D::new(
                    self.current_time,
                    self.current_idx,
                    self.global_geo_transform,
                    self.no_data_grid.into(),
                );

                self.state = StateInner::FillToEnd;
                self.current_idx = maybe_next_idx.expect("last index checked in prev. case");
                no_data_tile
            }
            _ => panic!("invlid state transition!"),
        }
    }
}

/*
pub enum State<T> {
    Initial,
    Polling {
        current_idx: GridIdx2D,
        current_time: TimeInterval,
    },
    NextTile {
        current_idx: GridIdx2D,
        current_time: TimeInterval,
        next_tile: RasterTile2D<T>,
    },
    FillToEnd {
        current_idx: GridIdx2D,
        current_time: TimeInterval,
        no_data_grid: EmptyGrid2D<T>,
    },
    Ended,
}

impl<T> State<T>
where
    T: Pixel,
{
    pub fn poll(&self) -> bool {
        match *self {
            State::Initial => true,
            State::Polling {
                current_idx: _,
                current_time: _,
            } => true,
            _ => false,
        }
    }

    pub fn ended(&self) -> bool {
        match self {
            State::Ended => true,
            _ => false,
        }
    }

    pub fn please_end_it(
        self,
        no_data_grid: EmptyGrid2D<T>,
        grid_bounds: &GridBoundingBox2D,
    ) -> Self {
        let min_idx = grid_bounds.min_index();
        match self {
            // there was never a tile. Fill with default time interval.
            State::Initial => State::FillToEnd {
                no_data_grid,
                current_idx: min_idx,
                current_time: TimeInterval::default(),
            },
            // there was a tile and it flipped the next index to the first one. => we are done.
            State::Polling {
                current_idx,
                current_time: _,
            } if current_idx == grid_bounds.min_index() => State::Ended,
            // there was a tile and it was not the last one. => fill to end.
            State::Polling {
                current_idx,
                current_time,
            } => State::FillToEnd {
                no_data_grid,
                current_idx,
                current_time,
            },
            _ => panic!("invalid state transition!"),
        }
    }

    pub fn new_tile(self, tile: RasterTile2D<T>, grid_bounds: &GridBoundingBox2D) -> Self {
        match self {
            // the tile is the first one. Fill and produce the tile.
            State::Initial => State::NextTile {
                current_idx: grid_bounds.min_index(),
                current_time: tile.time,
                next_tile: tile,
            },
            // there was a previous tile. Fill the gap and return the tile.
            State::Polling {
                current_idx,
                current_time,
            } => State::NextTile {
                current_idx,
                current_time,
                next_tile: tile,
            },
            _ => panic!("invlid state transition!"),
        }
    }

    pub fn next_tile_and_state(
        self,
        grid_bounds: &GridBoundingBox2D,
        global_geo_transform: GeoTransform,
    ) -> (RasterTile2D<T>, Self) {
        match self {
            // the next tile required is the one stored
            State::NextTile {
                current_idx,
                current_time,
                next_tile,
            } if current_idx == next_tile.tile_position => {
                let next_tile_idx = dbg!(grid_bounds.fwd_idx_unchecked(current_idx, 1))
                    .unwrap_or(grid_bounds.min_index());

                debug_assert!(current_time == next_tile.time);

                (
                    next_tile,
                    State::Polling {
                        current_idx: next_tile_idx,
                        current_time,
                    },
                )
            }
            State::NextTile {
                current_idx,
                current_time,
                next_tile,
            } => {
                let no_data_tile = RasterTile2D::new(
                    current_time,
                    current_idx,
                    next_tile.global_geo_transform,
                    EmptyGrid2D::new(
                        next_tile.grid_array.grid_shape(),
                        next_tile.no_data_value().unwrap(),
                    )
                    .into(),
                );

                let (next_idx, next_time) = match dbg!(grid_bounds.fwd_idx_unchecked(current_idx, 1))
                {
                    Some(idx) => (idx, current_time),
                    None => (grid_bounds.min_index(), next_tile.time),
                };
                (
                    no_data_tile,
                    State::NextTile {
                        current_idx: next_idx,
                        current_time: next_time,
                        next_tile,
                    },
                )
            }
            // produce tiles until all are done.
            State::FillToEnd {
                current_idx,
                current_time,
                no_data_grid,
            } => {
                let no_data_tile = RasterTile2D::new(
                    current_time,
                    current_idx,
                    global_geo_transform,
                    no_data_grid.into(),
                );

                // if there is a next idx then keep this state
                if let Some(next_tile_idx) = dbg!(grid_bounds.fwd_idx_unchecked(current_idx, 1)) {
                    return (
                        no_data_tile,
                        State::FillToEnd {
                            current_idx: next_tile_idx,
                            current_time,
                            no_data_grid,
                        },
                    );
                }

                // there is no next tile. End here.
                (no_data_tile, State::Ended)
            }
            _ => panic!("invlid state transition!"),
        }
    }
}
*/

#[pin_project]
pub struct SparseTilesFillAdapter<T, S> {
    #[pin]
    stream: S,

    state: StateOuter<T>,
}

impl<T, S> SparseTilesFillAdapter<T, S>
where
    T: Pixel,
{
    pub fn new(
        stream: S,
        tile_grid_bounds: GridBoundingBox2D,
        global_geo_transform: GeoTransform,
        tile_shape: GridShape2D,
        no_data_value: T,
    ) -> Self {
        let state = StateOuter {
            current_idx: tile_grid_bounds.min_index(),
            current_time: TimeInterval::default(),
            global_geo_transform,
            grid_bounds: tile_grid_bounds,
            next_tile: None,
            no_data_grid: EmptyGrid2D::new(tile_shape, no_data_value),
            state: StateInner::Initial,
        };

        SparseTilesFillAdapter { stream, state }
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
        if self.state.ended() {
            return Poll::Ready(None);
        }

        let this = self.project();

        dbg!(&this.state);

        if this.state.poll() {
            match ready!(this.stream.poll_next(cx)) {
                // there is a new tile. Store it to use it in the next steps.
                Some(Ok(r)) => {
                    this.state.new_tile(r);
                }
                // an error ouccured, stop producing anything and return the error.
                Some(Err(e)) => {
                    this.state.emergency_break();
                    return Poll::Ready(Some(Err(e)));
                }
                // the source is empty (now). Remember that.
                None => {
                    this.state.please_end_it();
                }
            }
        }

        dbg!(&this.state);

        if !this.state.ended() {
            let next_tile = this.state.next_tile();
            return Poll::Ready(Some(Ok(next_tile)));
        }

        Poll::Ready(None)
    }
}

/*

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
        if self.done_filling {
            return Poll::Ready(None);
        }

        let this = self.project();

        // if there is no known tile and the source is not done, query the source for the next tile.
        if this.next_known_tile.is_none() && (!*this.source_done) {
            match ready!(this.stream.poll_next(cx)) {
                // there is a new tile. Store it to use it in the next steps.
                Some(Ok(r)) => {
                    let old = this.next_known_tile.replace(r);
                    debug_assert!(old.is_none());
                }
                // an error ouccured, stop producing anything and return the error.
                Some(Err(e)) => {
                    *this.done_filling = true; // trigger the fuse
                    return Poll::Ready(Some(Err(e)));
                }
                // the source is empty (now). Remember that.
                None => {
                    *this.source_done = true;
                }
            }
        }

        // when we arrive here, there is either a new tile OR the input stream ended.

        // the current tile idx is the one to produce
        let current_tile_idx = dbg!(*this.current_tile_idx);

        // find out if we need to fill up to a tile or if we need to fill to the end
        let (next_known_tile_idx, current_time_to_use) =
            match (&this.next_known_tile, *this.current_time) {
                (None, None) => {
                    // there is no tile and no tile was seen. Fill until grid idx max with time default.
                    debug_assert!(*this.source_done); // source ended
                    (this.grid_bounding_box.max_index(), TimeInterval::default())
                }
                (None, Some(current_time)) => {
                    // there is no tile but at least one was produces before. Fill until grid idx max with the time of the previous tile.
                    debug_assert!(*this.source_done);
                    (this.grid_bounding_box.max_index(), current_time)
                }
                (Some(next_known_tile), None) => {
                    // got a first tile. Fill until this tile idx is reached, then return the tile. Should happen only once in each time interval.
                    debug_assert!(!*this.source_done);
                    (next_known_tile.tile_position, next_known_tile.time)
                }
                (Some(next_known_tile), Some(current_time))
                    if current_time == next_known_tile.time =>
                {
                    // A new tile for the same time interval was produced. Fill until the current tile idx is reached.
                    debug_assert!(!*this.source_done);
                    (next_known_tile.tile_position, next_known_tile.time)
                }
                (Some(next_known_tile), Some(current_time)) => {
                    // A new tile was produced but the current time slot needs filling. Fill to max tile idx in the current time interval.
                    debug_assert!(!*this.source_done);
                    debug_assert!(current_time != next_known_tile.time);
                    debug_assert!(current_time.start() <= next_known_tile.time.start());
                    (this.grid_bounding_box.max_index(), current_time)
                }
            };

        dbg!(next_known_tile_idx);
        dbg!(current_time_to_use);

        // get the next tile idx. If it is None then make time progress.
        let next_tile_idx = dbg!(this
            .grid_bounding_box
            .fwd_idx_unchecked(current_tile_idx, 1));

        if let Some(next_tile_idx) = next_tile_idx {
            *this.current_tile_idx = next_tile_idx;
        } else {
            // this must only happen if tile idx max was reached
            debug_assert!(current_tile_idx == this.grid_bounding_box.max_index());
            *this.current_tile_idx = this.grid_bounding_box.min_index();

            *this.current_time = None; // this.next_known_tile.as_ref().map(|t| t.time);
        };

        // produce no data tiles if the current tile idx is not the next known tile idx
        if current_tile_idx != next_known_tile_idx {
            // return a no data tile for the current start idx.
            return Poll::Ready(Some(Ok(no_data_tile(
                current_tile_idx,
                current_time_to_use,
                *this.global_geo_transform,
                *this.tile_shape,
                *this.no_data_value,
            ))));
        }

        // the tile to create is the stored one OR the last tile.
        match this.next_known_tile.take() {
            None => {
                *this.done_filling = true;
                return Poll::Ready(Some(Ok(no_data_tile(
                    current_tile_idx,
                    current_time_to_use,
                    *this.global_geo_transform,
                    *this.tile_shape,
                    *this.no_data_value,
                ))));
            }
            Some(next_known_tile) => {
                debug_assert!(next_known_tile.tile_position == current_tile_idx);
                debug_assert!(next_known_tile.time == current_time_to_use);
                *this.current_time = Some(next_known_tile.time);
                Poll::Ready(Some(Ok(next_known_tile)))
            }
        }
    }
}

*/

#[cfg(test)]
mod tests {
    use futures::{stream, StreamExt};
    use geoengine_datatypes::{primitives::TimeInterval, raster::Grid, util::test::TestDefault};

    use super::*;

    #[tokio::test]
    async fn test_gaps() {
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
        ];

        let result_data = data.into_iter().map(|d| Ok(d));

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

        assert_eq!(tile_time_positions, expected_positions)
    }

    #[tokio::test]
    async fn test_empty() {
        let data = vec![];

        let result_data = data.into_iter().map(|d| Ok(d));

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

        assert_eq!(tile_time_positions, expected_positions)
    }
}
