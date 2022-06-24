use rayon::iter::{IndexedParallelIterator, IntoParallelRefMutIterator, ParallelIterator};

use crate::raster::{GridOrEmpty, GridSize, MaskedGrid, RasterTile2D};

pub trait UpdateMaskedElements<T, F: Fn(Option<T>) -> Option<T>> {
    fn update_masked_elements(&mut self, map_fn: F);
}

pub trait UpdateMaskedElementsParallel<T, F: Fn(Option<T>) -> Option<T>> {
    fn update_masked_elements_parallel(&mut self, map_fn: F);
}

impl<T, F, G> UpdateMaskedElements<T, F> for MaskedGrid<G, T>
where
    T: Copy,
    G: GridSize + Clone + PartialEq,
    F: Fn(Option<T>) -> Option<T>,
{
    fn update_masked_elements(&mut self, map_fn: F) {
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

impl<T, F, G> UpdateMaskedElements<T, F> for GridOrEmpty<G, T>
where
    T: Copy,
    G: GridSize + Clone + PartialEq,
    F: Fn(Option<T>) -> Option<T>,
    MaskedGrid<G, T>: UpdateMaskedElements<T, F>,
{
    fn update_masked_elements(&mut self, map_fn: F) {
        match self {
            GridOrEmpty::Grid(grid) => grid.update_masked_elements(map_fn),
            GridOrEmpty::Empty(_) => {}
        }
    }
}

impl<T, F> UpdateMaskedElements<T, F> for RasterTile2D<T>
where
    T: Copy,
    F: Fn(Option<T>) -> Option<T>,
{
    fn update_masked_elements(&mut self, map_fn: F) {
        self.grid_array.update_masked_elements(map_fn);
    }
}

impl<T, F, G> UpdateMaskedElementsParallel<T, F> for MaskedGrid<G, T>
where
    T: 'static + Copy + Send + Sync,
    G: GridSize + Clone + Send + Sync,
    F: Fn(Option<T>) -> Option<T> + Sync + Send,
{
    fn update_masked_elements_parallel(&mut self, map_fn: F) {
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

impl<T, F, G> UpdateMaskedElementsParallel<T, F> for GridOrEmpty<G, T>
where
    T: 'static + Copy + Send + Sync,
    G: GridSize + Clone + Send + Sync,
    F: Fn(Option<T>) -> Option<T> + Sync + Send,
    MaskedGrid<G, T>: UpdateMaskedElementsParallel<T, F>,
{
    fn update_masked_elements_parallel(&mut self, map_fn: F) {
        match self {
            GridOrEmpty::Grid(grid) => grid.update_masked_elements_parallel(map_fn),
            GridOrEmpty::Empty(_) => {}
        }
    }
}

impl<T, F> UpdateMaskedElementsParallel<T, F> for RasterTile2D<T>
where
    T: 'static + Copy + Send + Sync,
    F: Fn(Option<T>) -> Option<T> + Sync + Send,
{
    fn update_masked_elements_parallel(&mut self, map_fn: F) {
        self.grid_array.update_masked_elements_parallel(map_fn);
    }
}
