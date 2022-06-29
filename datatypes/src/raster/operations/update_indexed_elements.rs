use rayon::iter::{
    IndexedParallelIterator, IntoParallelRefIterator, IntoParallelRefMutIterator, ParallelIterator,
};

use crate::raster::{
    EmptyGrid, Grid, GridIdx, GridOrEmpty, GridOrEmpty2D, GridSize, GridSpaceToLinearSpace,
    MapIndexedElements, MapIndexedElementsParallel, MaskedGrid, RasterTile2D,
};

/// This trait models mutable updates on elements using a provided update function that maps each element to a new value.
///
/// The trait is implemented in a way that `Index` as well as the type `FT` that the map function uses are generic.
/// The generic `Index` allows to either use enumerated elements `Index = usize` or n-dimensinal grid coordinates `Index = GridIdx`.
///
/// Most usefull implementations are on: `Grid`, `MaskedGrid`, `GridOrEmpty` and `RasterTile2D`.
///
/// On `Grid` elements are mapped as `|element: T| { element + 1 }` with `F: Fn(T, Index) -> T`
///
/// On `MaskedGrid` elements are mapped ignoring _no data_ as `|element: T| { element + 1 }` with `F: Fn(T, Index) -> T` or handling _no data_ as `|element: Option<T>| { element.mao(|e| e+ 1) }` with `F: Fn(Option<T>, Index) -> Option<T>`.
pub trait UpdateIndexedElements<Index, FT, F: Fn(Index, FT) -> FT> {
    /// Apply the map fn to all elements and overwrite the old value with the result of the closure.
    /// Use `usize` for enumerated pixels or `GridIdx` for n-dimnsional element locations as `Index` type in the `map_fn`.
    fn update_indexed_elements(&mut self, map_fn: F);
}

/// This trait is equal to `UpdateIndexedElements` but uses a thread pool to do the operation in parallel.
/// It models mutable updates on elements using a provided update function that maps each element to a new value.
///
/// The trait is implemented in a way that `Index` as well as the type `FT` that the map function uses are generic.
/// The generic `Index` allows to either use enumerated elements `Index = usize` or n-dimensinal grid coordinates `Index = GridIdx`.
///
/// Most usefull implementations are on: `Grid`, `MaskedGrid`, `GridOrEmpty` and `RasterTile2D`.
///
/// On `Grid` elements are mapped as `|element: T| { element + 1 }` with `F: Fn(T, Index) -> T`
///
/// On `MaskedGrid` elements are mapped ignoring _no data_ as `|element: T| { element + 1 }` with `F: Fn(T, Index) -> T` or handling _no data_ as `|element: Option<T>| { element.mao(|e| e+ 1) }` with `F: Fn(Option<T>, Index) -> Option<T>`.
pub trait UpdateIndexedElementsParallel<Index, FT, F: Fn(Index, FT) -> FT> {
    /// Apply the `map_fn` to all elements and overwrite the old value with the result of the closure parallel.
    /// Use `usize` for enumerated pixels or `GridIdx` for n-diemnsional element locations as `Index` type in the `map_fn`.
    fn update_indexed_elements_parallel(&mut self, map_fn: F);
}

// Implementation for Grid using usize as index: F: Fn(usize, T) -> T.
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

// Implementation for Grid using GridIdx as index: F: Fn(GridIdx, T) -> T.
// Delegates to implementation for Grid with F: Fn(usize, T) -> T.
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

// Implementation for MaskedGrid to enable update of the inner data with F: Fn(T, Index) -> T.
impl<G, Index, T, F> UpdateIndexedElements<Index, T, F> for MaskedGrid<G, T>
where
    F: Fn(Index, T) -> T,
    T: Copy,
    Grid<G, T>: UpdateIndexedElements<Index, T, F>,
{
    fn update_indexed_elements(&mut self, map_fn: F) {
        self.inner_grid.update_indexed_elements(map_fn);
    }
}

// Implementation for MaskedGrid using usize as index: F: Fn(usize, Option<T>) -> Option<T>.
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

// Implementation for MaskedGrid using GridIdx as index: F: Fn(GridIdx, Option<T>) -> Option<T>.
// Delegates to implementation for MaskedGrid with F: Fn(usize, Option<T>) -> Option<T>.
impl<G, A, T, F> UpdateIndexedElements<GridIdx<A>, Option<T>, F> for MaskedGrid<G, T>
where
    G: GridSpaceToLinearSpace<IndexArray = A> + Clone + PartialEq,
    F: Fn(GridIdx<A>, Option<T>) -> Option<T>,
    A: AsRef<[isize]>,
    T: Copy,
{
    fn update_indexed_elements(&mut self, map_fn: F) {
        let grid_shape = self.shape().clone();

        let grid_idx_map_fn = |lin_idx, old_value_option| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx, old_value_option)
        };

        self.update_indexed_elements(grid_idx_map_fn);
    }
}

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(GridIdx, T) -> T
//    F: Fn(usize, T) -> T
impl<T, Index, F, G> UpdateIndexedElements<Index, T, F> for GridOrEmpty<G, T>
where
    T: Copy,
    G: GridSize + Clone + PartialEq,
    F: Fn(Index, T) -> T,
    MaskedGrid<G, T>: UpdateIndexedElements<Index, T, F>,
{
    fn update_indexed_elements(&mut self, map_fn: F) {
        match self {
            GridOrEmpty::Grid(grid) => grid.update_indexed_elements(map_fn),
            GridOrEmpty::Empty(_) => {}
        }
    }
}

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(GridIdx, Option<T>) -> Option<T>,
//    F: Fn(usize, Option<T>) -> Option<T>,
impl<T, Index, F, G> UpdateIndexedElements<Index, Option<T>, F> for GridOrEmpty<G, T>
where
    T: Copy,
    G: GridSize + Clone + PartialEq,
    F: Fn(Index, Option<T>) -> Option<T>,
    MaskedGrid<G, T>: UpdateIndexedElements<Index, Option<T>, F>,
    EmptyGrid<G, T>: MapIndexedElements<Option<T>, Option<T>, Index, F, Output = MaskedGrid<G, T>>,
{
    fn update_indexed_elements(&mut self, map_fn: F) {
        match self {
            GridOrEmpty::Grid(grid) => grid.update_indexed_elements(map_fn),
            GridOrEmpty::Empty(e) => {
                // we need to map all the empty pixels. If any is valid set the mapped grid as self.
                let mapped_grid = e.clone().map_indexed_elements(map_fn);
                if mapped_grid.mask_ref().data.iter().any(|m| *m) {
                    *self = GridOrEmpty::Grid(mapped_grid);
                }
            }
        }
    }
}

// Implementation for RasterTile2D.
// Works with:
//    F: Fn(Option<T>, GridIdx2D) -> Option<T>,
//    F: Fn(T, GridIdx2D) -> T
//    F: Fn(usize, Option<T>) -> Option<T>,
//    F: Fn(usize, T) -> T
impl<T, TF, Index, F> UpdateIndexedElements<Index, TF, F> for RasterTile2D<T>
where
    TF: Copy,
    F: Fn(Index, TF) -> TF,
    GridOrEmpty2D<T>: UpdateIndexedElements<Index, TF, F>,
{
    fn update_indexed_elements(&mut self, map_fn: F) {
        self.grid_array.update_indexed_elements(map_fn);
    }
}

// Implementation for Grid using usize as index: F: Fn(usize, T) -> T.
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

// Implementation for Grid using GridIdx as index: F: Fn(GridIdx, T) -> T.
// Delegates to implementation for Grid with F: Fn(usize, T) -> T.
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

// Implementation for MaskedGrid to enable update of the inner data with F: Fn(T, Index) -> T.
impl<G, Index, T, F> UpdateIndexedElementsParallel<Index, T, F> for MaskedGrid<G, T>
where
    F: Fn(Index, T) -> T,
    T: Copy,
    Grid<G, T>: UpdateIndexedElementsParallel<Index, T, F>,
{
    fn update_indexed_elements_parallel(&mut self, map_fn: F) {
        self.inner_grid.update_indexed_elements_parallel(map_fn);
    }
}

// Implementation for MaskedGrid using usize as index: F: Fn(usize, Option<T>) -> Option<T>.
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

// Implementation for MaskedGrid using GridIdx as index: F: Fn(GridIdx, Option<T>) -> Option<T>.
// Delegates to implementation for MaskedGrid with F: Fn(usize, Option<T>) -> Option<T>.
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

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(GridIdx, T) -> T
//    F: Fn(usize, T) -> T
impl<T, Index, F, G> UpdateIndexedElementsParallel<Index, T, F> for GridOrEmpty<G, T>
where
    T: Copy,
    G: GridSize + Clone + PartialEq,
    F: Fn(Index, T) -> T,
    MaskedGrid<G, T>: UpdateIndexedElementsParallel<Index, T, F>,
{
    fn update_indexed_elements_parallel(&mut self, map_fn: F) {
        match self {
            GridOrEmpty::Grid(grid) => grid.update_indexed_elements_parallel(map_fn),
            GridOrEmpty::Empty(_) => {}
        }
    }
}

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(GridIdx, Option<T>) -> Option<T>,
//    F: Fn(usize, Option<T>) -> Option<T>,
impl<T, Index, F, G> UpdateIndexedElementsParallel<Index, Option<T>, F> for GridOrEmpty<G, T>
where
    T: Copy,
    G: GridSize + Clone + PartialEq,
    F: Fn(Index, Option<T>) -> Option<T>,
    MaskedGrid<G, T>: UpdateIndexedElementsParallel<Index, Option<T>, F>,
    EmptyGrid<G, T>:
        MapIndexedElementsParallel<Option<T>, Option<T>, Index, F, Output = MaskedGrid<G, T>>,
{
    fn update_indexed_elements_parallel(&mut self, map_fn: F) {
        match self {
            GridOrEmpty::Grid(grid) => grid.update_indexed_elements_parallel(map_fn),
            GridOrEmpty::Empty(e) => {
                // we need to map all the empty pixels. If any is valid set the mapped grid as self.
                let mapped_grid = e.clone().map_indexed_elements_parallel(map_fn);
                if mapped_grid
                    .mask_ref()
                    .data
                    .par_iter()
                    .with_min_len(mapped_grid.shape().axis_size_x())
                    .any(|m| *m)
                {
                    *self = GridOrEmpty::Grid(mapped_grid);
                }
            }
        }
    }
}

// Implementation for RasterTile2D.
// Works with:
//    F: Fn(Option<T>, GridIdx2D) -> Option<T>,
//    F: Fn(T, GridIdx2D) -> T
//    F: Fn(usize, Option<T>) -> Option<T>,
//    F: Fn(usize, T) -> T
impl<T, TF, Index, F> UpdateIndexedElementsParallel<Index, TF, F> for RasterTile2D<T>
where
    TF: Copy,
    F: Fn(Index, TF) -> TF,
    GridOrEmpty2D<T>: UpdateIndexedElementsParallel<Index, TF, F>,
{
    fn update_indexed_elements_parallel(&mut self, map_fn: F) {
        self.grid_array.update_indexed_elements_parallel(map_fn);
    }
}

#[cfg(test)]
mod tests {
    use crate::raster::{EmptyGrid2D, Grid2D, GridIdx2D};

    use super::*;

    #[test]
    fn update_indexed_elements_inner_linear_idx() {
        let dim = [2, 2];
        let data: Vec<i32> = vec![1, 2, 3, 4];

        let mut r1 = GridOrEmpty::Grid(MaskedGrid::from(Grid2D::new(dim.into(), data).unwrap()));
        r1.update_indexed_elements(|idx: usize, p: i32| p + idx as i32);

        let expected = [Some(1), Some(3), Some(5), Some(7)];

        match r1 {
            GridOrEmpty::Grid(g) => {
                let res_options: Vec<Option<i32>> = g.masked_element_deref_iterator().collect();
                assert_eq!(res_options, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn update_indexed_elements_no_data_linear_idx() {
        let dim = [2, 2];
        let data: Vec<i32> = vec![1, 2, 3, 4];
        let validity_mask: Vec<bool> = vec![true, false, true, false];

        let mut r1 = GridOrEmpty::Grid(
            MaskedGrid::new(
                Grid2D::new(dim.into(), data).unwrap(),
                Grid2D::new(dim.into(), validity_mask).unwrap(),
            )
            .unwrap(),
        );
        r1.update_indexed_elements(
            |idx: usize, p: Option<i32>| {
                if p.is_some() {
                    None
                } else {
                    Some(idx as i32)
                }
            },
        );

        let expected = [None, Some(1), None, Some(3)];

        match r1 {
            GridOrEmpty::Grid(g) => {
                let res_options: Vec<Option<i32>> = g.masked_element_deref_iterator().collect();
                assert_eq!(res_options, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn update_indexed_elements_no_data_grid_idx() {
        let dim = [4, 4];
        let data: Vec<i32> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let validity_mask: Vec<bool> = vec![
            true, false, true, false, true, false, true, false, true, false, true, false, true,
            false, true, false,
        ];

        let mut r1 = GridOrEmpty::Grid(
            MaskedGrid::new(
                Grid2D::new(dim.into(), data).unwrap(),
                Grid2D::new(dim.into(), validity_mask).unwrap(),
            )
            .unwrap(),
        );
        r1.update_indexed_elements(|idx: GridIdx2D, p: Option<i32>| {
            let GridIdx([y, x]) = idx;
            if p.is_some() {
                None
            } else {
                Some((y * 10 + x) as i32)
            }
        });

        let expected = [
            None,
            Some(1),
            None,
            Some(3),
            None,
            Some(11),
            None,
            Some(13),
            None,
            Some(21),
            None,
            Some(23),
            None,
            Some(31),
            None,
            Some(33),
        ];

        match r1 {
            GridOrEmpty::Grid(g) => {
                let res_options: Vec<Option<i32>> = g.masked_element_deref_iterator().collect();
                assert_eq!(res_options, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn update_indexed_elements_empty() {
        let dim = [2, 2];

        let mut r2 = GridOrEmpty::Empty::<_, i32>(EmptyGrid2D::new(dim.into()));
        r2.update_indexed_elements(|idx: usize, p: i32| p + idx as i32);

        match r2 {
            GridOrEmpty::Grid(_) => {
                panic!("Expected empty grid")
            }
            GridOrEmpty::Empty(e) => {
                assert_eq!(e.shape, dim.into());
            }
        }
    }
    #[test]
    fn update_indexed_elements_parallel_inner_linear_idx() {
        let dim = [2, 2];
        let data: Vec<i32> = vec![1, 2, 3, 4];

        let mut r1 = GridOrEmpty::Grid(MaskedGrid::from(Grid2D::new(dim.into(), data).unwrap()));
        r1.update_indexed_elements_parallel(|idx: usize, p: i32| p + idx as i32);

        let expected = [Some(1), Some(3), Some(5), Some(7)];

        match r1 {
            GridOrEmpty::Grid(g) => {
                let res_options: Vec<Option<i32>> = g.masked_element_deref_iterator().collect();
                assert_eq!(res_options, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn update_indexed_elements_parallel_no_data_linear_idx() {
        let dim = [2, 2];
        let data: Vec<i32> = vec![1, 2, 3, 4];
        let validity_mask: Vec<bool> = vec![true, false, true, false];

        let mut r1 = GridOrEmpty::Grid(
            MaskedGrid::new(
                Grid2D::new(dim.into(), data).unwrap(),
                Grid2D::new(dim.into(), validity_mask).unwrap(),
            )
            .unwrap(),
        );
        r1.update_indexed_elements_parallel(
            |idx: usize, p: Option<i32>| {
                if p.is_some() {
                    None
                } else {
                    Some(idx as i32)
                }
            },
        );

        let expected = [None, Some(1), None, Some(3)];

        match r1 {
            GridOrEmpty::Grid(g) => {
                let res_options: Vec<Option<i32>> = g.masked_element_deref_iterator().collect();
                assert_eq!(res_options, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn update_indexed_elements_parallel_no_data_grid_idx() {
        let dim = [4, 4];
        let data: Vec<i32> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let validity_mask: Vec<bool> = vec![
            true, false, true, false, true, false, true, false, true, false, true, false, true,
            false, true, false,
        ];

        let mut r1 = GridOrEmpty::Grid(
            MaskedGrid::new(
                Grid2D::new(dim.into(), data).unwrap(),
                Grid2D::new(dim.into(), validity_mask).unwrap(),
            )
            .unwrap(),
        );
        r1.update_indexed_elements_parallel(|idx: GridIdx2D, p: Option<i32>| {
            let GridIdx([y, x]) = idx;
            if p.is_some() {
                None
            } else {
                Some((y * 10 + x) as i32)
            }
        });

        let expected = [
            None,
            Some(1),
            None,
            Some(3),
            None,
            Some(11),
            None,
            Some(13),
            None,
            Some(21),
            None,
            Some(23),
            None,
            Some(31),
            None,
            Some(33),
        ];

        match r1 {
            GridOrEmpty::Grid(g) => {
                let res_options: Vec<Option<i32>> = g.masked_element_deref_iterator().collect();
                assert_eq!(res_options, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn update_indexed_elements_parallel_empty() {
        let dim = [2, 2];

        let mut r2 = GridOrEmpty::Empty::<_, i32>(EmptyGrid2D::new(dim.into()));
        r2.update_indexed_elements_parallel(|idx: usize, p: i32| p + idx as i32);

        match r2 {
            GridOrEmpty::Grid(_) => {
                panic!("Expected empty grid")
            }
            GridOrEmpty::Empty(e) => {
                assert_eq!(e.shape, dim.into());
            }
        }
    }
}
