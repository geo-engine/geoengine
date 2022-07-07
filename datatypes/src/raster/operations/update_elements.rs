use crate::raster::{Grid, GridOrEmpty, GridOrEmpty2D, GridSize, MaskedGrid, RasterTile2D};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefMutIterator, ParallelIterator};

/// This trait models mutable updates on elements using a provided update function that maps each element to a new value.
///
/// Most usefull implementations are on: `Grid`, `MaskedGrid`, `GridOrEmpty` and `RasterTile2D`.
///
/// On `Grid` elements are mapped as `|element: T| { element + 1 }` with `F: Fn(T) -> T`
///
/// On `MaskedGrid` elements are mapped ignoring _no data_ as `|element: T| { element + 1 }` with `F: Fn(T) -> T` or handling _no data_ as `|element: Option<T>| { element.map(|e| e+ 1) }` with `F: Fn(Option<T>) -> Option<T>`.
pub trait UpdateElements<FT, F: Fn(FT) -> FT> {
    /// Apply the map fn to all elements and overwrite the old value with the result of the closure.
    fn update_elements(&mut self, map_fn: F);
}

/// This trait is equal to `UpdateElements` but uses a thread pool to do the operation in parallel.
/// Most usefull implementations are on: `Grid`, `MaskedGrid`, `GridOrEmpty` and `RasterTile2D`.
///
/// On `Grid` elements are mapped as `|element: T| { element + 1 }` with `F: Fn(T) -> T`
///
/// On `MaskedGrid` elements are mapped ignoring _no data_ as `|element: T| { element + 1 }` with `F: Fn(T) -> T` or handling _no data_ as `|element: Option<T>| { element.map(|e| e+ 1) }` with `F: Fn(Option<T>) -> Option<T>`.
pub trait UpdateElementsParallel<FT, F: Fn(FT) -> FT> {
    /// Apply the `map_fn` to all elements and overwrite the old value with the result of the closure parallel.
    fn update_elements_parallel(&mut self, map_fn: F);
}

// Implementation for Grid using usize as index: F: Fn(T) -> T.
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

// Implementation for MaskedGrid to enable update of the inner data with F: Fn(T) -> T.
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

// Implementation for MaskedGrid using usize as index: F: Fn(Option<T>) -> Option<T>.
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

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(T) -> T
impl<T, F, G> UpdateElements<T, F> for GridOrEmpty<G, T>
where
    T: 'static + Copy,
    G: GridSize + Clone,
    F: Fn(T) -> T,
    MaskedGrid<G, T>: UpdateElements<T, F>,
{
    fn update_elements(&mut self, map_fn: F) {
        match self {
            GridOrEmpty::Grid(grid) => grid.update_elements(map_fn),
            GridOrEmpty::Empty(_) => {}
        }
    }
}

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(Option<T>) -> Option<T>,
impl<T, F, G> UpdateElements<Option<T>, F> for GridOrEmpty<G, T>
where
    T: 'static + Copy + Default,
    G: GridSize + Clone + PartialEq,
    F: Fn(Option<T>) -> Option<T>,
    MaskedGrid<G, T>: UpdateElements<Option<T>, F>,
{
    fn update_elements(&mut self, map_fn: F) {
        if self.is_empty() {
            // if None maps to a value we can be sure that the whole grid will turn to that value.
            if let Some(fill_value) = map_fn(None as Option<T>) {
                *self =
                    GridOrEmpty::Grid(MaskedGrid::new_filled(self.shape_ref().clone(), fill_value));
            }
            return;
        }

        if let GridOrEmpty::Grid(grid) = self {
            grid.update_elements(map_fn);
        };
    }
}

// Implementation for RasterTile2D.
// Works with:
//    F: Fn(Option<T>) -> Option<T>,
//    F: Fn(T) -> T
impl<T, TF, F> UpdateElements<TF, F> for RasterTile2D<T>
where
    T: 'static + Copy,
    F: Fn(TF) -> TF,
    GridOrEmpty2D<T>: UpdateElements<TF, F>,
{
    fn update_elements(&mut self, map_fn: F) {
        self.grid_array.update_elements(map_fn);
    }
}

// Implementation for Grid using usize as index: F: Fn(T) -> T.
impl<T, F, G> UpdateElementsParallel<T, F> for Grid<G, T>
where
    T: 'static + Send + Copy,
    G: GridSize + Clone,
    F: Fn(T) -> T + Send + Sync,
{
    fn update_elements_parallel(&mut self, map_fn: F) {
        
        let num_elements_per_thread = num::integer::div_ceil(self.shape.number_of_elements(), rayon::current_num_threads());

        self.data
            .par_iter_mut()
            .with_min_len(num_elements_per_thread)
            .for_each(|t| *t = map_fn(*t));
    }
}

// Implementation for MaskedGrid to enable update of the inner data with F: Fn(T) -> T.
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

// Implementation for MaskedGrid using usize as index: F: Fn(Option<T>) -> Option<T>.
impl<T, F, G> UpdateElementsParallel<Option<T>, F> for MaskedGrid<G, T>
where
    T: 'static + Send + Copy,
    G: GridSize + Clone + PartialEq,
    F: Fn(Option<T>) -> Option<T> + Send + Sync,
{
    fn update_elements_parallel(&mut self, map_fn: F) {
        let num_elements_per_thread = num::integer::div_ceil(self.shape().number_of_elements(), rayon::current_num_threads());

        self.inner_grid
            .data
            .par_iter_mut()
            .with_min_len(num_elements_per_thread)
            .zip(
                self.validity_mask
                    .data
                    .par_iter_mut()
                    .with_min_len(num_elements_per_thread),
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

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(T) -> T
impl<T, F, G> UpdateElementsParallel<T, F> for GridOrEmpty<G, T>
where
    T: 'static + Send + Copy,
    G: GridSize + Clone,
    F: Fn(T) -> T + Send + Sync,
    MaskedGrid<G, T>: UpdateElementsParallel<T, F>,
{
    fn update_elements_parallel(&mut self, map_fn: F) {
        match self {
            GridOrEmpty::Grid(grid) => grid.update_elements_parallel(map_fn),
            GridOrEmpty::Empty(_) => {}
        }
    }
}

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(Option<T>) -> Option<T>,
impl<T, F, G> UpdateElementsParallel<Option<T>, F> for GridOrEmpty<G, T>
where
    T: 'static + Send + Copy + Default,
    G: GridSize + Clone + PartialEq,
    F: Fn(Option<T>) -> Option<T> + Send + Sync,
    MaskedGrid<G, T>: UpdateElementsParallel<Option<T>, F>,
{
    fn update_elements_parallel(&mut self, map_fn: F) {
        if self.is_empty() {
            // if None maps to a value we can be sure that the whole grid will turn to that value.
            if let Some(fill_value) = map_fn(None as Option<T>) {
                *self =
                    GridOrEmpty::Grid(MaskedGrid::new_filled(self.shape_ref().clone(), fill_value));
            }
            return;
        }

        if let GridOrEmpty::Grid(grid) = self {
            grid.update_elements_parallel(map_fn);
        };
    }
}

// Implementation for RasterTile2D.
// Works with:
//    F: Fn(Option<T>) -> Option<T>,
//    F: Fn(T) -> T
impl<T, TF, F> UpdateElementsParallel<TF, F> for RasterTile2D<T>
where
    T: 'static + Send + Copy,
    F: Fn(TF) -> TF + Send + Sync,
    GridOrEmpty2D<T>: UpdateElementsParallel<TF, F>,
{
    fn update_elements_parallel(&mut self, map_fn: F) {
        self.grid_array.update_elements_parallel(map_fn);
    }
}
