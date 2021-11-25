use std::{pin::Pin, task::Poll};

use futures::{ready, Stream};
use geoengine_datatypes::{
    primitives::{SpatialPartitioned, TimeInterval},
    raster::{
        EmptyGrid2D, GeoTransform, GridBoundingBox2D, GridBounds, GridIdx2D, GridShape2D, GridStep,
        Pixel, RasterTile2D, TilingSpecification,
    },
};
use pin_project::pin_project;

use crate::util::Result;

use crate::engine::RasterQueryRectangle;

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
                    // this is a the first tile ever and the first in the grid. Return it!
                    Some(Ok(tile)) if this.sc.current_idx == tile.tile_position => {
                        debug_assert!(this.sc.current_idx == min_idx);
                        debug_assert!(!this.sc.is_any_tile_stored());
                        this.sc.current_time = tile.time;
                        this.sc.state = State::PollingForNextTile;
                        Some(Ok(tile))
                    }
                    // This is the first tile but not for the first idx. Store it to use it later.
                    Some(Ok(tile)) => {
                        debug_assert!(this.sc.current_idx == min_idx);
                        debug_assert!(!this.sc.is_any_tile_stored());

                        this.sc.current_time = tile.time;
                        this.sc.next_tile = Some(tile);
                        this.sc.state = State::FillAndProduceNextTile;
                        Some(Ok(this.sc.current_no_data_tile())) // it is important to generate the no data tile here since we need the time
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
                    // This is a new tile and it is the one to produce in this call. Time and idx are correct.
                    Some(Ok(tile))
                        if this.sc.current_idx == tile.tile_position
                            && this.sc.current_time == tile.time =>
                    {
                        debug_assert!(!this.sc.is_any_tile_stored());
                        debug_assert!(tile.time.start() >= this.sc.current_time.start());

                        this.sc.current_time = tile.time;
                        this.sc.state = State::PollingForNextTile;
                        Some(Ok(tile))
                    }
                    // this is a new tile,  we are at the start of a new grid run and this is the first tile
                    Some(Ok(tile))
                        if this.sc.current_idx == tile.tile_position
                            && this.sc.current_idx == min_idx =>
                    {
                        debug_assert!(!this.sc.is_any_tile_stored());
                        debug_assert!(tile.time.start() >= this.sc.current_time.start());

                        this.sc.current_time = tile.time;
                        this.sc.state = State::PollingForNextTile;
                        Some(Ok(tile))
                    }
                    // this is a new tile and we are at the start of a new grid run but it is not the tile to produce
                    Some(Ok(tile)) if this.sc.current_idx == min_idx => {
                        debug_assert!(!this.sc.is_any_tile_stored());
                        debug_assert!(tile.time.start() >= this.sc.current_time.start());

                        this.sc.current_time = tile.time;
                        this.sc.next_tile = Some(tile);
                        this.sc.state = State::FillAndProduceNextTile;
                        Some(Ok(this.sc.current_no_data_tile()))
                    }
                    // there is a new tile. Store it to use it in the next steps.
                    Some(Ok(tile)) => {
                        debug_assert!(!this.sc.is_any_tile_stored());
                        debug_assert!(tile.time.start() >= this.sc.current_time.start());

                        this.sc.next_tile = Some(tile);
                        this.sc.state = State::FillAndProduceNextTile;
                        Some(Ok(this.sc.current_no_data_tile()))
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
                let next_time = this
                    .sc
                    .next_tile
                    .as_ref()
                    .map(|t| t.time)
                    .expect("next_tile must be set in NextTile state");

                let (next_idx, next_time) = match this.sc.maybe_next_idx() {
                    Some(idx) => (idx, this.sc.current_time),
                    None => (this.sc.min_index(), next_time),
                };

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
}
