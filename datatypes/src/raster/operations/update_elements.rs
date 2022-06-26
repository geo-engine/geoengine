use crate::raster::{Grid, GridOrEmpty, GridSize, MaskedGrid, MaskedGrid2D, RasterTile2D};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefMutIterator, ParallelIterator};

pub trait UpdateElements<FT, F: Fn(FT) -> FT> {
    /// Apply the map fn to all elements and overwrite the old value with the result of the closure.
    fn update_elements(&mut self, map_fn: F);
}

pub trait UpdateElementsParallel<FT, F: Fn(FT) -> FT> {
    /// Apply the `map_fn` to all elements and overwrite the old value with the result of the closure parallel.
    fn update_elements_parallel(&mut self, map_fn: F);
}

impl<T, F, G> UpdateElements<T, F> for Grid<G, T>
where
    T: 'static + Copy,
    G: GridSize + Clone,
    F: Fn(T) -> T,
{
    fn update_elements(&mut self, map_fn: F) {
        self.data.iter_mut().for_each(|t| *t = map_fn(*t));
    }
}

impl<T, F, G> UpdateElements<T, F> for MaskedGrid<G, T>
where
    T: 'static,
    G: GridSize + Clone,
    F: Fn(T) -> T,
    Grid<G, T>: UpdateElements<T, F>,
{
    fn update_elements(&mut self, map_fn: F) {
        self.inner_grid.update_elements(map_fn);
    }
}

impl<T, F, G> UpdateElements<Option<T>, F> for MaskedGrid<G, T>
where
    T: 'static + Copy,
    G: GridSize + Clone,
    F: Fn(Option<T>) -> Option<T>,
{
    fn update_elements(&mut self, map_fn: F) {
        self.inner_grid
            .data
            .iter_mut()
            .zip(self.validity_mask.data.iter_mut())
            .for_each(|(o, m)| {
                let in_value = if *m { Some(*o) } else { None };

                let new_out_value = map_fn(in_value);
                *m = new_out_value.is_some();

                if let Some(out_value) = new_out_value {
                    *o = out_value;
                }
            });
    }
}

impl<T, TF, F, G> UpdateElements<TF, F> for GridOrEmpty<G, T>
where
    T: 'static + Copy,
    G: GridSize + Clone,
    F: Fn(TF) -> TF,
    MaskedGrid<G, T>: UpdateElements<TF, F>,
{
    fn update_elements(&mut self, map_fn: F) {
        match self {
            GridOrEmpty::Grid(grid) => grid.update_elements(map_fn),
            GridOrEmpty::Empty(_) => {}
        }
    }
}

impl<T, TF, F> UpdateElements<TF, F> for RasterTile2D<T>
where
    T: 'static + Copy,
    F: Fn(TF) -> TF,
    MaskedGrid2D<T>: UpdateElements<TF, F>,
{
    fn update_elements(&mut self, map_fn: F) {
        self.grid_array.update_elements(map_fn);
    }
}

impl<T, F, G> UpdateElementsParallel<T, F> for Grid<G, T>
where
    T: 'static + Send + Copy,
    G: GridSize + Clone,
    F: Fn(T) -> T + Send + Sync,
{
    fn update_elements_parallel(&mut self, map_fn: F) {
        self.data
            .par_iter_mut()
            .with_min_len(self.shape.axis_size_x())
            .for_each(|t| *t = map_fn(*t));
    }
}

impl<T, F, G> UpdateElementsParallel<T, F> for MaskedGrid<G, T>
where
    T: 'static + Send,
    G: GridSize + Clone,
    F: Fn(T) -> T + Send + Sync,
    Grid<G, T>: UpdateElementsParallel<T, F>,
{
    fn update_elements_parallel(&mut self, map_fn: F) {
        self.inner_grid.update_elements_parallel(map_fn);
    }
}

impl<T, F, G> UpdateElementsParallel<Option<T>, F> for MaskedGrid<G, T>
where
    T: 'static + Send + Copy,
    G: GridSize + Clone,
    F: Fn(Option<T>) -> Option<T> + Send + Sync,
{
    fn update_elements_parallel(&mut self, map_fn: F) {
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
            .for_each(|(i, m)| {
                let in_value = if *m { Some(*i) } else { None };

                let out_value = map_fn(in_value);
                *m = out_value.is_some();

                if let Some(o) = out_value {
                    *i = o;
                };
            });
    }
}

impl<T, TF, F, G> UpdateElementsParallel<TF, F> for GridOrEmpty<G, T>
where
    T: 'static + Send + Copy,
    G: GridSize + Clone,
    F: Fn(TF) -> TF + Send + Sync,
    MaskedGrid<G, T>: UpdateElementsParallel<TF, F>,
{
    fn update_elements_parallel(&mut self, map_fn: F) {
        match self {
            GridOrEmpty::Grid(grid) => grid.update_elements_parallel(map_fn),
            GridOrEmpty::Empty(_) => {}
        }
    }
}

impl<T, TF, F> UpdateElementsParallel<TF, F> for RasterTile2D<T>
where
    T: 'static + Send + Copy,
    F: Fn(TF) -> TF + Send + Sync,
    MaskedGrid2D<T>: UpdateElementsParallel<TF, F>,
{
    fn update_elements_parallel(&mut self, map_fn: F) {
        self.grid_array.update_elements_parallel(map_fn);
    }
}
