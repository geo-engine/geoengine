use crate::raster::{Grid, GridOrEmpty, GridSize, MaskedGrid, RasterTile2D};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefMutIterator, ParallelIterator};

pub trait UpdateElements<T, F: Fn(&mut T) -> ()> {
    fn update_elements(&mut self, map_fn: F);
}

pub trait UpdateElementsParallel<T, F: Fn(&mut T) -> ()> {
    fn update_elements_parallel(&mut self, map_fn: F);
}

impl<T, F, G> UpdateElements<T, F> for Grid<G, T>
where
    T: 'static,
    G: GridSize + Clone,
    F: Fn(&mut T),
{
    fn update_elements(&mut self, map_fn: F) {
        self.data.iter_mut().for_each(map_fn);
    }
}

impl<T, F, G> UpdateElements<T, F> for MaskedGrid<G, T>
where
    T: 'static,
    G: GridSize + Clone,
    F: Fn(&mut T),
    Grid<G, T>: UpdateElements<T, F>,
{
    fn update_elements(&mut self, map_fn: F) {
        self.inner_grid.update_elements(map_fn);
    }
}

impl<T, F, G> UpdateElements<T, F> for GridOrEmpty<G, T>
where
    T: 'static,
    G: GridSize + Clone,
    F: Fn(&mut T),
    Grid<G, T>: UpdateElements<T, F>,
{
    fn update_elements(&mut self, map_fn: F) {
        match self {
            GridOrEmpty::Grid(grid) => grid.update_elements(map_fn),
            GridOrEmpty::Empty(_) => {}
        }
    }
}

impl<T, F> UpdateElements<T, F> for RasterTile2D<T>
where
    T: 'static,
    F: Fn(&mut T),
{
    fn update_elements(&mut self, map_fn: F) {
        self.grid_array.update_elements(map_fn);
    }
}

impl<T, F, G> UpdateElementsParallel<T, F> for Grid<G, T>
where
    T: 'static + Send,
    G: GridSize + Clone,
    F: Fn(&mut T) + Send + Sync,
{
    fn update_elements_parallel(&mut self, map_fn: F) {
        self.data
            .par_iter_mut()
            .with_min_len(self.shape.axis_size_x())
            .for_each(map_fn);
    }
}

impl<T, F, G> UpdateElementsParallel<T, F> for MaskedGrid<G, T>
where
    T: 'static + Send,
    G: GridSize + Clone,
    F: Fn(&mut T) + Send + Sync,
    Grid<G, T>: UpdateElementsParallel<T, F>,
{
    fn update_elements_parallel(&mut self, map_fn: F) {
        self.inner_grid.update_elements_parallel(map_fn);
    }
}

impl<T, F, G> UpdateElementsParallel<T, F> for GridOrEmpty<G, T>
where
    T: 'static + Send,
    G: GridSize + Clone,
    F: Fn(&mut T) + Send + Sync,
    Grid<G, T>: UpdateElementsParallel<T, F>,
{
    fn update_elements_parallel(&mut self, map_fn: F) {
        match self {
            GridOrEmpty::Grid(grid) => grid.update_elements_parallel(map_fn),
            GridOrEmpty::Empty(_) => {}
        }
    }
}

impl<T, F> UpdateElementsParallel<T, F> for RasterTile2D<T>
where
    T: 'static + Send,
    F: Fn(&mut T) + Send + Sync,
{
    fn update_elements_parallel(&mut self, map_fn: F) {
        self.grid_array.update_elements_parallel(map_fn);
    }
}
