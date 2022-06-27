use rayon::{
    iter::{IndexedParallelIterator, ParallelIterator},
    slice::{ParallelSlice, ParallelSliceMut},
};

use crate::raster::{
    Grid, GridIdx, GridIdx2D, GridOrEmpty, GridOrEmpty2D, GridSize, GridSpaceToLinearSpace,
    MaskedGrid, MaskedGrid2D, RasterTile2D,
};

pub trait MapIndexedElements<In, Out, Index, F: Fn(Index, In) -> Out> {
    type Output;
    fn map_index_elements(self, map_fn: F) -> Self::Output;
}

pub trait MapIndexedElementsParallel<In, Out, Index, F: Fn(Index, In) -> Out> {
    type Output;
    fn map_index_elements_parallel(self, map_fn: F) -> Self::Output;
}

impl<G, A, In, Out, F> MapIndexedElements<In, Out, GridIdx<A>, F> for Grid<G, In>
where
    G: GridSpaceToLinearSpace<IndexArray = A> + Clone,
    A: AsRef<[isize]>,
    F: Fn(GridIdx<A>, In) -> Out,
    In: Clone,
    Out: Default + Clone,
{
    type Output = Grid<G, Out>;

    fn map_index_elements(self, map_fn: F) -> Self::Output {
        let grid_shape = self.shape.clone();

        let grid_idx_map_fn = |lin_idx, old_value_option| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx, old_value_option)
        };
        self.map_index_elements(grid_idx_map_fn)
    }
}

impl<G, In, Out, F> MapIndexedElements<In, Out, usize, F> for Grid<G, In>
where
    G: GridSize,
    F: Fn(usize, In) -> Out,
    In: Clone,
    Out: Default + Clone,
{
    type Output = Grid<G, Out>;

    fn map_index_elements(self, map_fn: F) -> Self::Output {
        let Grid { shape, data } = self;

        let out_data: Vec<Out> = data
            .into_iter()
            .enumerate()
            .map(|(lin_idx, i)| map_fn(lin_idx, i))
            .collect();

        Grid::new(shape, out_data).expect("Grid creation with shape failed before.")
    }
}

impl<G, Index, In, Out, F> MapIndexedElements<In, Out, Index, F> for MaskedGrid<G, In>
where
    G: GridSize + PartialEq + Clone,
    F: Fn(Index, In) -> Out,
    In: Clone,
    Out: Default + Clone,
    Grid<G, In>: MapIndexedElements<In, Out, Index, F, Output = Grid<G, Out>>,
{
    type Output = MaskedGrid<G, Out>;

    fn map_index_elements(self, map_fn: F) -> Self::Output {
        let MaskedGrid {
            inner_grid,
            validity_mask,
        } = self;
        let out_grid = inner_grid.map_index_elements(map_fn);

        MaskedGrid::new(out_grid, validity_mask).expect("Createion failed before.")
    }
}

impl<G, A, In, Out, F> MapIndexedElements<Option<In>, Option<Out>, GridIdx<A>, F>
    for MaskedGrid<G, In>
where
    G: GridSize + GridSpaceToLinearSpace<IndexArray = A> + Clone + PartialEq,
    A: AsRef<[isize]>,
    F: Fn(GridIdx<A>, Option<In>) -> Option<Out>,
    In: Clone,
    Out: Default + Clone,
{
    type Output = MaskedGrid<G, Out>;

    fn map_index_elements(self, map_fn: F) -> Self::Output {
        let grid_shape = self.shape().clone();

        let grid_idx_map_fn = |lin_idx, old_value_option| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx, old_value_option)
        };
        self.map_index_elements(grid_idx_map_fn)
    }
}

impl<G, In, Out, F> MapIndexedElements<Option<In>, Option<Out>, usize, F> for MaskedGrid<G, In>
where
    G: GridSize + Clone + PartialEq,
    F: Fn(usize, Option<In>) -> Option<Out>,
    In: Clone,
    Out: Default + Clone,
{
    type Output = MaskedGrid<G, Out>;

    fn map_index_elements(self, map_fn: F) -> Self::Output {
        let MaskedGrid {
            inner_grid: data,
            mut validity_mask,
        } = self;
        debug_assert!(data.data.len() == validity_mask.data.len());
        debug_assert!(data.shape == validity_mask.shape);

        let out_data: Vec<Out> = data
            .data
            .into_iter()
            .zip(validity_mask.data.iter_mut())
            .enumerate()
            .map(|(lin_idx, (i, m))| {
                let in_masked_value = if *m { Some(i) } else { None };

                let out_value_option = map_fn(lin_idx, in_masked_value);

                *m = out_value_option.is_some();

                out_value_option.unwrap_or_default()
            })
            .collect();

        MaskedGrid::new(
            Grid::new(data.shape, out_data).expect("Grid createion failed before"),
            validity_mask,
        )
        .expect("Grid createion failed before")
    }
}

impl<G, TIn, TOut, FIn, FOut, Index, F> MapIndexedElements<FIn, FOut, Index, F>
    for GridOrEmpty<G, TIn>
where
    G: GridSize,
    F: Fn(Index, FIn) -> FOut,
    FIn: Clone + 'static,
    FOut: Default + Clone + 'static,
    TOut: 'static + Clone,
    TIn: 'static,
    MaskedGrid<G, TIn>: MapIndexedElements<FIn, FOut, Index, F, Output = MaskedGrid<G, TOut>>,
    GridOrEmpty<G, TOut>: From<MaskedGrid<G, TOut>>,
{
    type Output = GridOrEmpty<G, TOut>;

    fn map_index_elements(self, map_fn: F) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => GridOrEmpty::from(g.map_index_elements(map_fn)),
            GridOrEmpty::Empty(e) => e.convert_dtype::<TOut>().into(),
        }
    }
}

impl<In, Out, Index, F> MapIndexedElements<Option<In>, Option<Out>, Index, F> for RasterTile2D<In>
where
    F: Fn(Index, Option<In>) -> Option<Out>,
    In: Clone + 'static,
    Out: Default + Clone + 'static,
    MaskedGrid2D<In>:
        MapIndexedElements<Option<In>, Option<Out>, Index, F, Output = MaskedGrid2D<Out>>,
    GridOrEmpty2D<Out>: From<MaskedGrid2D<Out>>,
{
    type Output = RasterTile2D<Out>;

    fn map_index_elements(self, map_fn: F) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.map_index_elements(map_fn),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
        }
    }
}

impl<In, Out, F> MapIndexedElementsParallel<Option<In>, Option<Out>, GridIdx2D, F>
    for MaskedGrid2D<In>
where
    F: Fn(GridIdx2D, Option<In>) -> Option<Out> + Send + Sync,
    In: Copy + Clone + Sync,
    Out: Default + Clone + Send,
{
    type Output = MaskedGrid2D<Out>;

    fn map_index_elements_parallel(self, map_fn: F) -> Self::Output {
        let MaskedGrid {
            inner_grid: data,
            mut validity_mask,
        } = self;
        debug_assert!(data.data.len() == validity_mask.data.len());
        debug_assert!(data.shape == validity_mask.shape);

        let x_axis_size = data.shape.axis_size_x();
        let y_axis_size = data.shape.axis_size_y();

        let mut out_data = vec![Out::default(); data.data.len()];

        let parallelism = rayon::current_num_threads();
        let rows_per_task = num::integer::div_ceil(y_axis_size, parallelism);

        let chunk_size = x_axis_size * rows_per_task;

        out_data
            .par_chunks_mut(chunk_size)
            .zip(validity_mask.data.par_chunks_mut(chunk_size))
            .zip(data.data.par_chunks(chunk_size))
            .enumerate()
            .for_each(|(y_f, ((out_rows_slice, mask_row_slice), in_raw_slice))| {
                let chunk_lin_start = y_f * chunk_size;

                out_rows_slice
                    .iter_mut()
                    .zip(mask_row_slice.iter_mut())
                    .zip(in_raw_slice)
                    .enumerate()
                    .for_each(|(elem_x_idx, ((out, mask), i))| {
                        let lin_idx = chunk_lin_start + elem_x_idx;
                        let grid_idx = data.shape.grid_idx_unchecked(lin_idx);

                        let in_masked_value = if *mask { Some(*i) } else { None };

                        let out_value_option = map_fn(grid_idx, in_masked_value);

                        *mask = out_value_option.is_some();

                        if let Some(out_value) = out_value_option {
                            *out = out_value;
                        }
                    });
            });

        MaskedGrid::new(
            Grid::new(data.shape, out_data).expect("Grid createion failed before"),
            validity_mask,
        )
        .expect("Grid createion failed before")
    }
}

impl<In, Out, Index, F> MapIndexedElementsParallel<Option<In>, Option<Out>, Index, F>
    for GridOrEmpty2D<In>
where
    F: Fn(Index, Option<In>) -> Option<Out> + Send + Sync,
    In: Default + Copy + Clone + Sync + 'static,
    Out: Default + Clone + Send + 'static,
    MaskedGrid2D<In>:
        MapIndexedElementsParallel<Option<In>, Option<Out>, Index, F, Output = MaskedGrid2D<Out>>,
    GridOrEmpty2D<Out>: From<MaskedGrid2D<Out>>,
{
    type Output = GridOrEmpty2D<Out>;

    fn map_index_elements_parallel(self, map_fn: F) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => g.map_index_elements_parallel(map_fn).into(),
            GridOrEmpty::Empty(e) => {
                MaskedGrid::from(e)
                    .map_index_elements_parallel(map_fn)
                    .into() // TODO: this need some more thoughts. Currently it will materialize all empty grids. Propably check if any mask is true after ?
            }
        }
    }
}

impl<In, Out, Index, F> MapIndexedElementsParallel<Option<In>, Option<Out>, Index, F>
    for RasterTile2D<In>
where
    F: Fn(Index, Option<In>) -> Option<Out> + Send + Sync,
    In: Default + Copy + Clone + Sync + 'static,
    Out: Default + Clone + Send + 'static,
    MaskedGrid2D<In>:
        MapIndexedElementsParallel<Option<In>, Option<Out>, Index, F, Output = MaskedGrid2D<Out>>,
    GridOrEmpty2D<Out>: From<MaskedGrid2D<Out>>,
{
    type Output = RasterTile2D<Out>;

    fn map_index_elements_parallel(self, map_fn: F) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.map_index_elements_parallel(map_fn),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
        }
    }
}

// Impl for lin_idx

impl<In, Out, F> MapIndexedElementsParallel<Option<In>, Option<Out>, usize, F> for MaskedGrid2D<In>
where
    F: Fn(usize, Option<In>) -> Option<Out> + Send + Sync,
    In: Copy + Clone + Sync,
    Out: Default + Clone + Send,
{
    type Output = MaskedGrid2D<Out>;

    fn map_index_elements_parallel(self, map_fn: F) -> Self::Output {
        let MaskedGrid {
            inner_grid: data,
            mut validity_mask,
        } = self;
        debug_assert!(data.data.len() == validity_mask.data.len());
        debug_assert!(data.shape == validity_mask.shape);

        let x_axis_size = data.shape.axis_size_x();
        let y_axis_size = data.shape.axis_size_y();

        let mut out_data = vec![Out::default(); data.data.len()];

        let parallelism = rayon::current_num_threads();
        let rows_per_task = num::integer::div_ceil(y_axis_size, parallelism);

        let chunk_size = x_axis_size * rows_per_task;

        out_data
            .par_chunks_mut(chunk_size)
            .zip(validity_mask.data.par_chunks_mut(chunk_size))
            .zip(data.data.par_chunks(chunk_size))
            .enumerate()
            .for_each(|(y_f, ((out_rows_slice, mask_row_slice), in_raw_slice))| {
                let chunk_lin_start = y_f * chunk_size;

                out_rows_slice
                    .iter_mut()
                    .zip(mask_row_slice.iter_mut())
                    .zip(in_raw_slice)
                    .enumerate()
                    .for_each(|(elem_x_idx, ((out, mask), i))| {
                        let lin_idx = chunk_lin_start + elem_x_idx;

                        let in_masked_value = if *mask { Some(*i) } else { None };

                        let out_value_option = map_fn(lin_idx, in_masked_value);

                        *mask = out_value_option.is_some();

                        if let Some(out_value) = out_value_option {
                            *out = out_value;
                        }
                    });
            });

        MaskedGrid::new(
            Grid::new(data.shape, out_data).expect("Grid createion failed before"),
            validity_mask,
        )
        .expect("Grid createion failed before")
    }
}
