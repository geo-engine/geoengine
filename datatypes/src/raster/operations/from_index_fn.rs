use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

use crate::raster::{
    EmptyGrid, Grid, GridIdx, GridOrEmpty, GridSize, GridSpaceToLinearSpace, MaskedGrid,
};

const MIN_ELEMENTS_PER_THREAD: usize = 16 * 512;

/// This trait provides a creation operation for a `Grid` of type `T` . This is done using a provided function that maps each elements index to a new value.
///
/// The trait is implemented in a way that `Index` as well as the type `T` used by the map function are generic.
/// The generic `Index` allows to either use enumerated elements `Index = usize` or n-dimensinal grid coordinates `Index = GridIdx`.
///
/// Most useful implementations are on: `Grid`, `MaskedGrid`, `GridOrEmpty` and `RasterTile2D`.
///
/// On `Grid` elements are generated from `F: Fn(Index) -> T`
///
/// On `MaskedGrid` elements are mapped ignoring _no data_  by `F: Fn(Index) -> T` or handling _no data_ with `F: Fn(Index) -> Option<Out>`.
pub trait FromIndexFn<G, T, Index, F: Fn(Index) -> T> {
    /// Create a new instance from a shape. The `map_fn` produces a new element from its position.
    fn from_index_fn(grid_shape: &G, map_fn: F) -> Self;
}

/// This trait is equal to `MapIndexedElements` but uses a thread pool to do the operation in parallel.
/// This trait provides a creation operation for a `Grid` of type `T` . This is done using a provided function that maps each elements index to a new value.
///
/// The trait is implemented in a way that `Index` as well as the type `T` used by the map function are generic.
/// The generic `Index` allows to either use enumerated elements `Index = usize` or n-dimensinal grid coordinates `Index = GridIdx`.
///
/// Most useful implementations are on: `Grid`, `MaskedGrid`, `GridOrEmpty` and `RasterTile2D`.
///
/// On `Grid` elements are generated from `F: Fn(Index) -> T`
///
/// On `MaskedGrid` elements are mapped ignoring _no data_  by `F: Fn(Index) -> T` or handling _no data_ with `F: Fn(Index) -> Option<Out>`.
pub trait FromIndexFnParallel<G, T, Index, F: Fn(Index) -> T> {
    /// Create a new instance from a shape. The `map_fn` produces a new element from its position. Uses a `ThreadPool` for parallel operations.
    fn from_index_fn_parallel(grid_shape: &G, map_fn: F) -> Self;
}

// Implementation for Grid using usize as index: F: Fn(usize) -> Out.
impl<G, T, F> FromIndexFn<G, T, usize, F> for Grid<G, T>
where
    G: GridSize + Clone,
    F: Fn(usize) -> T,
    T: Default + Clone,
{
    fn from_index_fn(g: &G, map_fn: F) -> Grid<G, T> {
        let out_data: Vec<T> = (0..g.number_of_elements()).map(map_fn).collect();

        Grid::new(g.clone(), out_data)
            .expect("Grid creation should work because the shape is valid.")
    }
}

// Implementation for Grid using GridIdx as index: F: Fn(GridIdx) -> Out.
// Delegates to implementation for Grid with F: Fn(usize) -> Out.
impl<G, A, T, F> FromIndexFn<G, T, GridIdx<A>, F> for Grid<G, T>
where
    G: GridSpaceToLinearSpace<IndexArray = A> + Clone,
    A: AsRef<[isize]>,
    F: Fn(GridIdx<A>) -> T,
    T: Default + Clone,
{
    fn from_index_fn(grid_shape: &G, map_fn: F) -> Self {
        let grid_idx_map_fn = |lin_idx| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx)
        };
        Self::from_index_fn(grid_shape, grid_idx_map_fn)
    }
}

// Implementation for MaskedGrid using usize as index: F: Fn(usize) -> Out.
impl<G, I, T, F> FromIndexFn<G, T, I, F> for MaskedGrid<G, T>
where
    G: GridSize + Clone + PartialEq,
    F: Fn(I) -> T,
    T: Default + Clone,
    Grid<G, T>: FromIndexFn<G, T, I, F>,
{
    fn from_index_fn(g: &G, map_fn: F) -> MaskedGrid<G, T> {
        let out_grid = Grid::from_index_fn(g, map_fn);

        MaskedGrid::new_with_data(out_grid)
    }
}

// Implementation for MaskedGrid using usize as index: F: Fn(usize) -> Option<Out>.
impl<G, T, F> FromIndexFn<G, Option<T>, usize, F> for MaskedGrid<G, T>
where
    G: GridSize + Clone + PartialEq,
    F: Fn(usize) -> Option<T>,
    T: Default + Clone,
{
    fn from_index_fn(g: &G, map_fn: F) -> MaskedGrid<G, T> {
        let (out_data, out_validity): (Vec<T>, Vec<bool>) = (0..g.number_of_elements())
            .map(|i| {
                let res_option = map_fn(i);
                if let Some(res) = res_option {
                    (res, true)
                } else {
                    (T::default(), false)
                }
            })
            .unzip();

        let inner_grid = Grid::new(g.clone(), out_data)
            .expect("Grid creation should work because the shape is valid.");
        let validity_mask = Grid::new(g.clone(), out_validity)
            .expect("Grid creation should work because the shape is valid.");

        MaskedGrid {
            inner_grid,
            validity_mask,
        }
    }
}

// Implementation for MaskedGrid using GridIdx as index: F: Fn(GridIdx) -> Option<Out>.
// Delegates to implementation for MaskedGrid with F: Fn(usize) -> Option<Out>.
impl<G, A, T, F> FromIndexFn<G, Option<T>, GridIdx<A>, F> for MaskedGrid<G, T>
where
    G: GridSpaceToLinearSpace<IndexArray = A> + Clone + PartialEq,
    A: AsRef<[isize]>,
    F: Fn(GridIdx<A>) -> Option<T>,
    T: Default + Clone,
{
    fn from_index_fn(grid_shape: &G, map_fn: F) -> Self {
        let grid_idx_map_fn = |lin_idx| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx)
        };
        Self::from_index_fn(grid_shape, grid_idx_map_fn)
    }
}

// Implementation for GridOrEmpty. NOTE: This implementation will check if any mask pixel is set to true and return an EmptyGrid otherwise.
// Delegates MaskedGrid creation to the matching implementation for MaskedGrid.
impl<G, I, T, F, FT> FromIndexFn<G, FT, I, F> for GridOrEmpty<G, T>
where
    G: GridSize + Clone,
    F: Fn(I) -> FT,
    MaskedGrid<G, T>: FromIndexFn<G, FT, I, F>,
{
    fn from_index_fn(grid_shape: &G, map_fn: F) -> Self {
        let masked_grid = MaskedGrid::from_index_fn(grid_shape, map_fn);

        let is_valid = masked_grid.validity_mask.data.iter().any(|&m| m);

        if !is_valid {
            return GridOrEmpty::Empty(EmptyGrid::new(grid_shape.clone()));
        }

        GridOrEmpty::Grid(masked_grid)
    }
}

// parallel implementations

// Implementation for Grid using usize as index: F: Fn(usize) -> Out.
impl<G, T, F> FromIndexFnParallel<G, T, usize, F> for Grid<G, T>
where
    G: GridSize + Clone,
    F: Fn(usize) -> T + Send + Sync,
    T: Default + Clone + Send + Sync,
{
    fn from_index_fn_parallel(grid_shape: &G, map_fn: F) -> Self {
        let num_elements_per_thread = num::integer::div_ceil(
            grid_shape.number_of_elements(),
            rayon::current_num_threads(),
        )
        .max(MIN_ELEMENTS_PER_THREAD);

        let mut out_data = Vec::with_capacity(grid_shape.number_of_elements());

        (0..grid_shape.number_of_elements())
            .into_par_iter()
            .with_min_len(num_elements_per_thread)
            .map(map_fn)
            .collect_into_vec(out_data.as_mut());

        Grid::new(grid_shape.clone(), out_data)
            .expect("Grid creation should work because the shape is valid.")
    }
}

// Implementation for Grid using GridIdx as index: F: Fn(GridIdx) -> Out.
// Delegates to implementation for Grid with F: Fn(usize) -> Out.
impl<G, A, T, F> FromIndexFnParallel<G, T, GridIdx<A>, F> for Grid<G, T>
where
    G: GridSpaceToLinearSpace<IndexArray = A> + Clone + Send + Sync,
    A: AsRef<[isize]>,
    F: Fn(GridIdx<A>) -> T + Send + Sync,
    T: Default + Clone + Send + Sync,
{
    fn from_index_fn_parallel(grid_shape: &G, map_fn: F) -> Self {
        let grid_idx_map_fn = |lin_idx| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx)
        };
        Self::from_index_fn_parallel(grid_shape, grid_idx_map_fn)
    }
}

// Implementation for MaskedGrid using usize as index: F: Fn(usize) -> Out.
impl<G, I, T, F> FromIndexFnParallel<G, T, I, F> for MaskedGrid<G, T>
where
    G: GridSize + Clone + PartialEq,
    F: Fn(I) -> T + Send + Sync,
    T: Default + Clone + Send + Sync,
    Grid<G, T>: FromIndexFnParallel<G, T, I, F>,
{
    fn from_index_fn_parallel(g: &G, map_fn: F) -> MaskedGrid<G, T> {
        let out_grid = Grid::from_index_fn_parallel(g, map_fn);

        MaskedGrid::new_with_data(out_grid)
    }
}

// Implementation for MaskedGrid using usize as index: F: Fn(usize) -> Option<Out>.
impl<G, T, F> FromIndexFnParallel<G, Option<T>, usize, F> for MaskedGrid<G, T>
where
    G: GridSize + Clone + PartialEq,
    F: Fn(usize) -> Option<T> + Send + Sync,
    T: Default + Clone + Send + Sync,
{
    fn from_index_fn_parallel(grid_shape: &G, map_fn: F) -> MaskedGrid<G, T> {
        let num_elements_per_thread = num::integer::div_ceil(
            grid_shape.number_of_elements(),
            rayon::current_num_threads(),
        )
        .max(MIN_ELEMENTS_PER_THREAD);

        let mut out_data = Vec::with_capacity(grid_shape.number_of_elements());
        let mut out_validity = Vec::with_capacity(grid_shape.number_of_elements());

        (0..grid_shape.number_of_elements())
            .into_par_iter()
            .with_min_len(num_elements_per_thread)
            .map(|i| {
                let res_option = map_fn(i);
                if let Some(res) = res_option {
                    (res, true)
                } else {
                    (T::default(), false)
                }
            })
            .unzip_into_vecs(out_data.as_mut(), out_validity.as_mut());

        let inner_grid =
            Grid::new(grid_shape.clone(), out_data).expect("grid shape should be valid");
        let validity_mask =
            Grid::new(grid_shape.clone(), out_validity).expect("grid shape should be valid");

        MaskedGrid {
            inner_grid,
            validity_mask,
        }
    }
}

// Implementation for MaskedGrid using GridIdx as index: F: Fn(GridIdx) -> Option<Out>.
// Delegates to implementation for MaskedGrid with F: Fn(usize) -> Option<Out>.
impl<G, A, T, F> FromIndexFnParallel<G, Option<T>, GridIdx<A>, F> for MaskedGrid<G, T>
where
    G: GridSpaceToLinearSpace<IndexArray = A> + Clone + PartialEq + Send + Sync,
    A: AsRef<[isize]>,
    F: Fn(GridIdx<A>) -> Option<T> + Send + Sync,
    T: Default + Clone + Send + Sync,
{
    fn from_index_fn_parallel(grid_shape: &G, map_fn: F) -> Self {
        let grid_idx_map_fn = |lin_idx| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx)
        };
        Self::from_index_fn_parallel(grid_shape, grid_idx_map_fn)
    }
}

// Implementation for GridOrEmpty. NOTE: This implementation will check if any mask pixel is set to true and return an EmptyGrid otherwise.
// Delegates MaskedGrid creation to the matching implementation for MaskedGrid.
impl<G, I, T, F, FT> FromIndexFnParallel<G, FT, I, F> for GridOrEmpty<G, T>
where
    G: GridSize + Clone + Send + Sync,
    F: Fn(I) -> FT + Send + Sync,
    MaskedGrid<G, T>: FromIndexFnParallel<G, FT, I, F>,
{
    fn from_index_fn_parallel(grid_shape: &G, map_fn: F) -> Self {
        let masked_grid = MaskedGrid::from_index_fn_parallel(grid_shape, map_fn);

        let is_valid = masked_grid.validity_mask.data.iter().any(|&m| m); // TODO: benchmark if a parallel iterator can be faster here!

        if !is_valid {
            return GridOrEmpty::Empty(EmptyGrid::new(grid_shape.clone()));
        }

        GridOrEmpty::Grid(masked_grid)
    }
}

#[cfg(test)]
mod tests {
    use crate::raster::{GridIdx2D, GridOrEmpty2D, GridShape};

    use super::*;

    #[test]
    fn grid_from_2d_linear_index() {
        let grid_shape = GridShape::from([2, 4]);
        let grid = Grid::from_index_fn(&grid_shape, |i: usize| i);
        assert_eq!(grid.shape, GridShape::from([2, 4]));
        assert_eq!(grid.data, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn grid_from_2d_linear_index_parallel() {
        let grid_shape = GridShape::from([2, 4]);
        let grid = Grid::from_index_fn_parallel(&grid_shape, |i: usize| i);
        assert_eq!(grid.shape, GridShape::from([2, 4]));
        assert_eq!(grid.data, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn grid_from_2d_grid_index() {
        let grid_shape = GridShape::from([2, 4]);
        let grid = Grid::from_index_fn(&grid_shape, |GridIdx([y, x]): GridIdx2D| y * 10 + x);
        assert_eq!(grid.shape, GridShape::from([2, 4]));
        assert_eq!(grid.data, vec![0, 1, 2, 3, 10, 11, 12, 13]);
    }

    #[test]
    fn grid_from_2d_grid_index_parallel() {
        let grid_shape = GridShape::from([2, 4]);
        let grid =
            Grid::from_index_fn_parallel(&grid_shape, |GridIdx([y, x]): GridIdx2D| y * 10 + x);
        assert_eq!(grid.shape, GridShape::from([2, 4]));
        assert_eq!(grid.data, vec![0, 1, 2, 3, 10, 11, 12, 13]);
    }

    #[test]
    fn masked_grid_from_linear_index() {
        let grid_shape = GridShape::from([2, 4]);
        let masked_grid = MaskedGrid::from_index_fn(&grid_shape, |i: usize| i);
        assert_eq!(masked_grid.shape(), &GridShape::from([2, 4]));
        let res_values: Vec<Option<usize>> = masked_grid.masked_element_deref_iterator().collect();
        assert_eq!(
            res_values,
            vec![
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(6),
                Some(7)
            ]
        );
    }

    #[test]
    fn masked_grid_from_linear_index_parallel() {
        let grid_shape = GridShape::from([2, 4]);
        let masked_grid = MaskedGrid::from_index_fn_parallel(&grid_shape, |i: usize| i);
        assert_eq!(masked_grid.shape(), &GridShape::from([2, 4]));
        let res_values: Vec<Option<usize>> = masked_grid.masked_element_deref_iterator().collect();
        assert_eq!(
            res_values,
            vec![
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(6),
                Some(7)
            ]
        );
    }

    #[test]
    fn masked_grid_from_grid_index() {
        let grid_shape = GridShape::from([2, 4]);
        let masked_grid =
            MaskedGrid::from_index_fn(&grid_shape, |GridIdx([y, x]): GridIdx2D| y * 10 + x);
        assert_eq!(masked_grid.shape(), &GridShape::from([2, 4]));
        let res_values: Vec<Option<isize>> = masked_grid.masked_element_deref_iterator().collect();
        assert_eq!(
            res_values,
            vec![
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(10),
                Some(11),
                Some(12),
                Some(13)
            ]
        );
    }

    #[test]
    fn masked_grid_from_grid_index_parallel() {
        let grid_shape = GridShape::from([2, 4]);
        let masked_grid =
            MaskedGrid::from_index_fn_parallel(&grid_shape, |GridIdx([y, x]): GridIdx2D| {
                y * 10 + x
            });
        assert_eq!(masked_grid.shape(), &GridShape::from([2, 4]));
        let res_values: Vec<Option<isize>> = masked_grid.masked_element_deref_iterator().collect();
        assert_eq!(
            res_values,
            vec![
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(10),
                Some(11),
                Some(12),
                Some(13)
            ]
        );
    }

    #[test]
    fn masked_grid_from_linear_index_option() {
        let grid_shape = GridShape::from([2, 4]);
        let masked_grid =
            MaskedGrid::from_index_fn(
                &grid_shape,
                |i: usize| if i % 2 == 0 { Some(i) } else { None },
            );
        assert_eq!(masked_grid.shape(), &GridShape::from([2, 4]));
        let res_values: Vec<Option<usize>> = masked_grid.masked_element_deref_iterator().collect();
        assert_eq!(
            res_values,
            vec![Some(0), None, Some(2), None, Some(4), None, Some(6), None]
        );
    }

    #[test]
    fn masked_grid_from_linear_index_parallel_option() {
        let grid_shape = GridShape::from([2, 4]);
        let masked_grid = MaskedGrid::from_index_fn_parallel(&grid_shape, |i: usize| {
            if i % 2 == 0 {
                Some(i)
            } else {
                None
            }
        });
        assert_eq!(masked_grid.shape(), &GridShape::from([2, 4]));
        let res_values: Vec<Option<usize>> = masked_grid.masked_element_deref_iterator().collect();
        assert_eq!(
            res_values,
            vec![Some(0), None, Some(2), None, Some(4), None, Some(6), None]
        );
    }

    #[test]
    fn masked_grid_from_grid_index_option() {
        let grid_shape = GridShape::from([2, 4]);
        let masked_grid = MaskedGrid::from_index_fn(&grid_shape, |GridIdx([y, x]): GridIdx2D| {
            let r = y * 10 + x;
            if r % 2 == 0 {
                Some(r)
            } else {
                None
            }
        });
        assert_eq!(masked_grid.shape(), &GridShape::from([2, 4]));
        let res_values: Vec<Option<isize>> = masked_grid.masked_element_deref_iterator().collect();
        assert_eq!(
            res_values,
            vec![Some(0), None, Some(2), None, Some(10), None, Some(12), None]
        );
    }

    #[test]
    fn masked_grid_from_grid_index_parallel_option() {
        let grid_shape = GridShape::from([2, 4]);
        let masked_grid =
            MaskedGrid::from_index_fn_parallel(&grid_shape, |GridIdx([y, x]): GridIdx2D| {
                let r = y * 10 + x;
                if r % 2 == 0 {
                    Some(r)
                } else {
                    None
                }
            });
        assert_eq!(masked_grid.shape(), &GridShape::from([2, 4]));
        let res_values: Vec<Option<isize>> = masked_grid.masked_element_deref_iterator().collect();
        assert_eq!(
            res_values,
            vec![Some(0), None, Some(2), None, Some(10), None, Some(12), None]
        );
    }

    #[test]
    fn grid_or_empty_from_grid_index_option() {
        let grid_shape = GridShape::from([2, 4]);
        let grid_or_empty =
            GridOrEmpty::from_index_fn(&grid_shape, |GridIdx([y, x]): GridIdx2D| {
                let r = y * 10 + x;
                if r % 2 == 0 {
                    Some(r)
                } else {
                    None
                }
            });
        assert!(grid_or_empty.is_grid());

        let masked_grid = match grid_or_empty {
            GridOrEmpty::Grid(masked_grid) => masked_grid,
            GridOrEmpty::Empty(_) => panic!(),
        };

        assert_eq!(masked_grid.shape(), &GridShape::from([2, 4]));
        let res_values: Vec<Option<isize>> = masked_grid.masked_element_deref_iterator().collect();
        assert_eq!(
            res_values,
            vec![Some(0), None, Some(2), None, Some(10), None, Some(12), None]
        );
    }

    #[test]
    fn grid_or_empty_from_grid_index_parallel_option() {
        let grid_shape = GridShape::from([2, 4]);
        let grid_or_empty =
            GridOrEmpty::from_index_fn_parallel(&grid_shape, |GridIdx([y, x]): GridIdx2D| {
                let r = y * 10 + x;
                if r % 2 == 0 {
                    Some(r)
                } else {
                    None
                }
            });

        assert!(grid_or_empty.is_grid());

        let masked_grid = match grid_or_empty {
            GridOrEmpty::Grid(masked_grid) => masked_grid,
            GridOrEmpty::Empty(_) => panic!(),
        };

        assert_eq!(masked_grid.shape(), &GridShape::from([2, 4]));
        let res_values: Vec<Option<isize>> = masked_grid.masked_element_deref_iterator().collect();
        assert_eq!(
            res_values,
            vec![Some(0), None, Some(2), None, Some(10), None, Some(12), None]
        );
    }

    #[test]
    fn grid_or_empty_from_grid_index_option_empty() {
        let grid_shape = GridShape::from([2, 4]);
        let grid_or_empty =
            GridOrEmpty2D::<isize>::from_index_fn(&grid_shape, |GridIdx([y, x]): GridIdx2D| {
                let r = y * 10 + x;
                if r > 100 {
                    Some(r)
                } else {
                    None
                }
            });
        assert!(grid_or_empty.is_empty());

        let empty_grid = match grid_or_empty {
            GridOrEmpty::Grid(_) => panic!(),
            GridOrEmpty::Empty(empty_grid) => empty_grid,
        };

        assert_eq!(empty_grid.shape, GridShape::from([2, 4]));
    }

    #[test]
    fn grid_or_empty_from_grid_index_parallel_option_empty() {
        let grid_shape = GridShape::from([2, 4]);
        let grid_or_empty = GridOrEmpty2D::<isize>::from_index_fn_parallel(
            &grid_shape,
            |GridIdx([y, x]): GridIdx2D| {
                let r = y * 10 + x;
                if r > 100 {
                    Some(r)
                } else {
                    None
                }
            },
        );

        assert!(grid_or_empty.is_empty());

        let empty_grid = match grid_or_empty {
            GridOrEmpty::Grid(_) => panic!(),
            GridOrEmpty::Empty(empty_grid) => empty_grid,
        };

        assert_eq!(empty_grid.shape, GridShape::from([2, 4]));
    }
}
