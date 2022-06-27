use rayon::iter::{IndexedParallelIterator, IntoParallelRefMutIterator, ParallelIterator};

use crate::raster::{
    Grid, GridIdx, GridOrEmpty, GridSize, GridSpaceToLinearSpace, MaskedGrid, MaskedGrid2D,
    RasterTile2D,
};

/// This trait models mutable updates on elements using a closure.
pub trait UpdateIndexedElements<Index, FT, F: Fn(Index, FT) -> FT> {
    /// Apply the map fn to all elements and overwrite the old value with the result of the closure.
    /// Use `usize` for enumerated pixels or `GridIdx` for n-diemnsional element locations as `Index` type in the `map_fn`.
    fn update_indexed_elements(&mut self, map_fn: F);
}

/// This trait models mutable updates on elements using a closure.
/// It is equal to `UpdateIndexedElements` but uses a thread pool to do the operation in parallel.
pub trait UpdateIndexedElementsParallel<Index, FT, F: Fn(Index, FT) -> FT> {
    /// Apply the `map_fn` to all elements and overwrite the old value with the result of the closure parallel.
    /// Use `usize` for enumerated pixels or `GridIdx` for n-diemnsional element locations as `Index` type in the `map_fn`.
    fn update_indexed_elements_parallel(&mut self, map_fn: F);
}

impl<G, A, T, F> UpdateIndexedElements<GridIdx<A>, T, F> for Grid<G, T>
where
    G: GridSpaceToLinearSpace<IndexArray = A> + Clone,
    F: Fn(GridIdx<A>, T) -> T,
    A: AsRef<[isize]>,
    T: Copy,
{
    fn update_indexed_elements(&mut self, map_fn: F) {
        let grid_shape = self.shape.clone();

        let grid_idx_map_fn = |lin_idx, old_value_option| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx, old_value_option)
        };

        self.update_indexed_elements(grid_idx_map_fn);
    }
}

impl<G, T, F> UpdateIndexedElements<usize, T, F> for Grid<G, T>
where
    G: GridSize,
    F: Fn(usize, T) -> T,
    T: Copy,
{
    fn update_indexed_elements(&mut self, map_fn: F) {
        self.data
            .iter_mut()
            .enumerate()
            .for_each(|(lin_idx, element_value)| {
                let out_value = map_fn(lin_idx, *element_value);
                *element_value = out_value;
            });
    }
}

impl<G, A, T, F> UpdateIndexedElements<GridIdx<A>, Option<T>, F> for MaskedGrid<G, T>
where
    G: GridSpaceToLinearSpace<IndexArray = A> + PartialEq + Clone,
    F: Fn(GridIdx<A>, Option<T>) -> Option<T>,
    A: AsRef<[isize]>,
    T: Copy,
{
    fn update_indexed_elements(&mut self, map_fn: F) {
        let grid_shape = self.inner_grid.shape.clone();

        let grid_idx_map_fn = |lin_idx, old_value_option| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx, old_value_option)
        };

        self.update_indexed_elements(grid_idx_map_fn);
    }
}

impl<G, A, T, F> UpdateIndexedElements<GridIdx<A>, T, F> for MaskedGrid<G, T>
where
    G: GridSpaceToLinearSpace<IndexArray = A> + PartialEq + Clone,
    F: Fn(GridIdx<A>, T) -> T,
    A: AsRef<[isize]>,
    T: Copy,
    Grid<G, T>: UpdateIndexedElements<GridIdx<A>, T, F>,
{
    fn update_indexed_elements(&mut self, map_fn: F) {
        self.inner_grid.update_indexed_elements(map_fn)
    }
}

impl<G, T, F> UpdateIndexedElements<usize, Option<T>, F> for MaskedGrid<G, T>
where
    F: Fn(usize, Option<T>) -> Option<T>,
    T: Copy,
{
    fn update_indexed_elements(&mut self, map_fn: F) {
        self.inner_grid
            .data
            .iter_mut()
            .zip(self.validity_mask.data.iter_mut())
            .enumerate()
            .for_each(|(lin_idx, (element_value, is_valid))| {
                let in_masked_value = if *is_valid {
                    Some(*element_value)
                } else {
                    None
                };

                let out_value_option = map_fn(lin_idx, in_masked_value);

                *is_valid = out_value_option.is_some();

                if let Some(out_value) = out_value_option {
                    *element_value = out_value;
                }
            });
    }
}

impl<G, T, F> UpdateIndexedElements<usize, T, F> for MaskedGrid<G, T>
where
    F: Fn(usize, T) -> T,
    T: Copy,
    Grid<G, T>: UpdateIndexedElements<usize, T, F>,
{
    fn update_indexed_elements(&mut self, map_fn: F) {
        self.inner_grid.update_indexed_elements(map_fn)
    }
}

impl<T, TF, Index, F, G> UpdateIndexedElements<Index, TF, F> for GridOrEmpty<G, T>
where
    TF: Copy,
    G: GridSize + Clone + PartialEq,
    F: Fn(Index, TF) -> TF,
    MaskedGrid<G, T>: UpdateIndexedElements<Index, TF, F>,
{
    fn update_indexed_elements(&mut self, map_fn: F) {
        match self {
            GridOrEmpty::Grid(grid) => grid.update_indexed_elements(map_fn),
            GridOrEmpty::Empty(_) => {}
        }
    }
}

impl<T, TF, Index, F> UpdateIndexedElements<Index, TF, F> for RasterTile2D<T>
where
    TF: Copy,
    F: Fn(Index, TF) -> TF,
    MaskedGrid2D<T>: UpdateIndexedElements<Index, TF, F>,
{
    fn update_indexed_elements(&mut self, map_fn: F) {
        self.grid_array.update_indexed_elements(map_fn);
    }
}

impl<G, T, F> UpdateIndexedElementsParallel<usize, T, F> for Grid<G, T>
where
    G: GridSize,
    F: Fn(usize, T) -> T + Sync,
    T: 'static + Copy + Send + Sync,
{
    fn update_indexed_elements_parallel(&mut self, map_fn: F) {
        let axis_size_x = self.shape.axis_size_x();

        self.data
            .par_iter_mut()
            .with_min_len(axis_size_x)
            .enumerate()
            .for_each(|(lin_idx, element_value)| {
                let out_value = map_fn(lin_idx, *element_value);
                *element_value = out_value;
            });
    }
}

impl<G, A, T, F> UpdateIndexedElementsParallel<GridIdx<A>, T, F> for Grid<G, T>
where
    G: GridSpaceToLinearSpace<IndexArray = A> + Clone + Sync,
    F: Fn(GridIdx<A>, T) -> T + Sync,
    A: AsRef<[isize]>,
    T: 'static + Copy + Send + Sync,
{
    fn update_indexed_elements_parallel(&mut self, map_fn: F) {
        let grid_shape = self.shape.clone();

        // TODO: benchmark if this is more expensive then the variant with slices.
        let grid_idx_map_fn = |lin_idx, old_value_option| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx, old_value_option)
        };

        self.update_indexed_elements_parallel(grid_idx_map_fn);
    }
}

impl<G, T, F> UpdateIndexedElementsParallel<usize, Option<T>, F> for MaskedGrid<G, T>
where
    G: GridSize,
    F: Fn(usize, Option<T>) -> Option<T> + Sync,
    T: 'static + Copy + Send + Sync,
{
    fn update_indexed_elements_parallel(&mut self, map_fn: F) {
        let axis_size_x = self.inner_grid.shape.axis_size_x();

        self.inner_grid
            .data
            .par_iter_mut()
            .with_min_len(axis_size_x)
            .zip(
                self.validity_mask
                    .data
                    .par_iter_mut()
                    .with_min_len(axis_size_x),
            )
            .enumerate()
            .for_each(|(lin_idx, (element_value, element_valid))| {
                let in_value_option = if *element_valid {
                    Some(*element_value)
                } else {
                    None
                };

                let out_value_option = map_fn(lin_idx, in_value_option);

                *element_valid = out_value_option.is_some();

                if let Some(out_value) = out_value_option {
                    *element_value = out_value;
                }
            });
    }
}

impl<G, A, T, F> UpdateIndexedElementsParallel<GridIdx<A>, Option<T>, F> for MaskedGrid<G, T>
where
    G: GridSpaceToLinearSpace<IndexArray = A> + Clone + Sync,
    F: Fn(GridIdx<A>, Option<T>) -> Option<T> + Sync,
    A: AsRef<[isize]>,
    T: 'static + Copy + Send + Sync,
{
    fn update_indexed_elements_parallel(&mut self, map_fn: F) {
        let grid_shape = self.inner_grid.shape.clone();

        // TODO: benchmark if this is more expensive then the variant with slices.
        let grid_idx_map_fn = |lin_idx, old_value_option| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx, old_value_option)
        };

        self.update_indexed_elements_parallel(grid_idx_map_fn);
    }
}

impl<T, TF, Index, F, G> UpdateIndexedElementsParallel<Index, TF, F> for GridOrEmpty<G, T>
where
    TF: Copy,
    G: GridSize + Clone + PartialEq,
    F: Fn(Index, TF) -> TF,
    MaskedGrid<G, T>: UpdateIndexedElementsParallel<Index, TF, F>,
{
    fn update_indexed_elements_parallel(&mut self, map_fn: F) {
        match self {
            GridOrEmpty::Grid(grid) => grid.update_indexed_elements_parallel(map_fn),
            GridOrEmpty::Empty(_) => {}
        }
    }
}

impl<T, TF, Index, F> UpdateIndexedElementsParallel<Index, TF, F> for RasterTile2D<T>
where
    TF: Copy,
    F: Fn(Index, TF) -> TF,
    MaskedGrid2D<T>: UpdateIndexedElementsParallel<Index, TF, F>,
{
    fn update_indexed_elements_parallel(&mut self, map_fn: F) {
        self.grid_array.update_indexed_elements_parallel(map_fn);
    }
}
