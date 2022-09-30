use crate::raster::{
    EmptyGrid, Grid, GridIdx, GridIndexAccess, GridOrEmpty, GridOrEmpty2D, GridSize,
    GridSpaceToLinearSpace, MaskedGrid, MaskedGrid2D, RasterTile2D,
};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

const MIN_ELEMENTS_PER_THREAD: usize = 16 * 512;

/// This trait models a map operation from a `Grid` of type `In` into a `Grid` of Type `Out`. This is done using a provided function that maps each element to a new value.
///
/// The trait is implemented in a way that `Index` as well as the types `In` and `Out` used by the map function are generic.
/// The generic `Index` allows to either use enumerated elements `Index = usize` or n-dimensinal grid coordinates `Index = GridIdx`.
///
/// Most useful implementations are on: `Grid`, `MaskedGrid`, `GridOrEmpty` and `RasterTile2D`.
///
/// On `Grid` elements are mapped, e.g., as `|element: In| { element + 1 }` with `F: Fn(In, Index) -> Out`
///
/// On `MaskedGrid` you can map either only valid data (excluding no data), e.g., `|element: In| { element + 1 }` with `Fn(In, Index) -> Out` or handling no data as `|element: Option<In>| { element.map(|e| e + 1) }` with `F: Fn(Option<In>, Index) -> Option<Out>`.
pub trait MapIndexedElements<In, Out, Index, F: Fn(Index, In) -> Out> {
    type Output;
    /// Create a new instance from the current one. The `map_fn` transforms all elements to a new value.
    fn map_indexed_elements(self, map_fn: F) -> Self::Output;
}

/// This trait is equal to `MapIndexedElements` but uses a thread pool to do the operation in parallel.
/// This trait models a map operation from a `Grid` of type `In` into a `Grid` of Type `Out`. This is done using a provided function that maps each element to a new value.
///
/// The trait is implemented in a way that `Index` as well as the types `In` and `Out` used by the map function are generic.
/// The generic `Index` allows to either use enumerated elements `Index = usize` or n-dimensinal grid coordinates `Index = GridIdx`.
///
/// Most useful implementations are on: `Grid`, `MaskedGrid`, `GridOrEmpty` and `RasterTile2D`.
///
/// On `Grid` elements are mapped, e.g., as `|element: In| { element + 1 }` with `F: Fn(In, Index) -> Out`
///
/// On `MaskedGrid` you can map either only valid data (excluding no data), e.g., `|element: In| { element + 1 }` with `Fn(In, Index) -> Out` or handling no data as `|element: Option<In>| { element.map(|e| e + 1) }` with `F: Fn(Option<In>, Index) -> Option<Out>`.
pub trait MapIndexedElementsParallel<In, Out, Index, F: Fn(Index, In) -> Out> {
    type Output;
    /// Create a new instance from the current one. The `map_fn` transforms all elements to a new value. Use a `ThreadPool` for parallel map operations.
    fn map_indexed_elements_parallel(self, map_fn: F) -> Self::Output;
}

// Implementation for Grid using usize as index: F: Fn(usize, In) -> Out.
impl<G, In, Out, F> MapIndexedElements<In, Out, usize, F> for Grid<G, In>
where
    G: GridSize,
    F: Fn(usize, In) -> Out,
    In: Clone,
    Out: Default + Clone,
{
    type Output = Grid<G, Out>;

    fn map_indexed_elements(self, map_fn: F) -> Self::Output {
        let Grid { shape, data } = self;

        let out_data: Vec<Out> = data
            .into_iter()
            .enumerate()
            .map(|(lin_idx, i)| map_fn(lin_idx, i))
            .collect();

        Grid::new(shape, out_data).expect("Grid should have been created.")
    }
}

// Implementation for Grid using GridIdx as index: F: Fn(GridIdx, In) -> Out.
// Delegates to implementation for Grid with F: Fn(usize, In) -> Out.
impl<G, A, In, Out, F> MapIndexedElements<In, Out, GridIdx<A>, F> for Grid<G, In>
where
    G: GridSpaceToLinearSpace<IndexArray = A> + Clone,
    A: AsRef<[isize]>,
    F: Fn(GridIdx<A>, In) -> Out,
    In: Clone,
    Out: Default + Clone,
{
    type Output = Grid<G, Out>;

    fn map_indexed_elements(self, map_fn: F) -> Self::Output {
        let grid_shape = self.shape.clone();

        let grid_idx_map_fn = |lin_idx, old_value_option| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx, old_value_option)
        };
        self.map_indexed_elements(grid_idx_map_fn)
    }
}

// Implementation for MaskedGrid to map only the inner data with F: Fn(In, Index) -> Out.
impl<G, Index, In, Out, F> MapIndexedElements<In, Out, Index, F> for MaskedGrid<G, In>
where
    G: GridSize + PartialEq + Clone,
    F: Fn(Index, In) -> Out,
    In: Clone,
    Out: Default + Clone,
    Grid<G, In>: MapIndexedElements<In, Out, Index, F, Output = Grid<G, Out>>,
{
    type Output = MaskedGrid<G, Out>;

    fn map_indexed_elements(self, map_fn: F) -> Self::Output {
        let MaskedGrid {
            inner_grid,
            validity_mask,
        } = self;
        let out_grid = inner_grid.map_indexed_elements(map_fn);

        MaskedGrid::new(out_grid, validity_mask).expect("Masked grid should have been created.")
    }
}

// Implementation for MaskedGrid using usize as index: F: Fn(usize, Option<In>) -> Option<Out>.
impl<G, In, Out, F> MapIndexedElements<Option<In>, Option<Out>, usize, F> for MaskedGrid<G, In>
where
    G: GridSize + Clone + PartialEq,
    F: Fn(usize, Option<In>) -> Option<Out>,
    In: Clone,
    Out: Default + Clone,
    Grid<G, In>: GridIndexAccess<In, usize>,
{
    type Output = MaskedGrid<G, Out>;

    fn map_indexed_elements(self, map_fn: F) -> Self::Output {
        let MaskedGrid {
            inner_grid: data,
            mut validity_mask,
        } = self;
        debug_assert!(data.data.len() == validity_mask.data.len());
        debug_assert!(data.shape == validity_mask.shape);

        let out_data: Vec<Out> = validity_mask
            .data
            .iter_mut()
            .enumerate()
            .map(|(lin_idx, m)| {
                let in_masked_value = if *m {
                    let i = data.get_at_grid_index_unchecked(lin_idx);
                    Some(i)
                } else {
                    None
                };

                let out_value_option = map_fn(lin_idx, in_masked_value);

                *m = out_value_option.is_some();

                out_value_option.unwrap_or_default()
            })
            .collect();

        MaskedGrid::new(
            Grid::new(data.shape, out_data)
                .expect("the shape of the grid and the data size should match"),
            validity_mask,
        )
        .expect("the shape of the grid and the data size should match")
    }
}

// Implementation for MaskedGrid using GridIdx as index: F: Fn(GridIdx, Option<In>) -> Option<Out>.
// Delegates to implementation for MaskedGrid with F: Fn(usize, Option<In>) -> Option<Out>.
impl<G, A, In, Out, F> MapIndexedElements<Option<In>, Option<Out>, GridIdx<A>, F>
    for MaskedGrid<G, In>
where
    G: GridSize + GridSpaceToLinearSpace<IndexArray = A> + Clone + PartialEq,
    A: AsRef<[isize]>,
    F: Fn(GridIdx<A>, Option<In>) -> Option<Out>,
    In: Clone,
    Out: Default + Clone,
    Grid<G, In>: GridIndexAccess<In, usize>,
{
    type Output = MaskedGrid<G, Out>;

    fn map_indexed_elements(self, map_fn: F) -> Self::Output {
        let grid_shape = self.shape().clone();

        let grid_idx_map_fn = |lin_idx, old_value_option| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx, old_value_option)
        };
        self.map_indexed_elements(grid_idx_map_fn)
    }
}

// Implementation for EmptyGrid using usize as index: F: Fn(usize, Option<In>) -> Option<Out>.
impl<G, In, Out, F> MapIndexedElements<Option<In>, Option<Out>, usize, F> for EmptyGrid<G, In>
where
    G: GridSize + Clone + PartialEq,
    F: Fn(usize, Option<In>) -> Option<Out>,
    In: Clone,
    Out: Default + Clone,
{
    type Output = MaskedGrid<G, Out>;

    fn map_indexed_elements(self, map_fn: F) -> Self::Output {
        let (out_data, validity_mask) = (0..self.shape.number_of_elements())
            .into_iter()
            .map(|lin_idx| {
                let in_masked_value = None as Option<In>;

                let out_value_option = map_fn(lin_idx, in_masked_value);
                let is_valid = out_value_option.is_some();

                (out_value_option.unwrap_or_default(), is_valid)
            })
            .unzip();

        MaskedGrid::new(
            Grid::new(self.shape.clone(), out_data)
                .expect("the shape of the grid and the data size should match"),
            Grid::new(self.shape, validity_mask)
                .expect("the shape of the grid and the data size should match"),
        )
        .expect("the shape of the grid and the data size should match")
    }
}

// Implementation for EmptyGrid using GridIdx as index: F: Fn(GridIdx, Option<In>) -> Option<Out>.
// Delegates to implementation for EmptyGrid with F: Fn(usize, Option<In>) -> Option<Out>.
impl<G, A, In, Out, F> MapIndexedElements<Option<In>, Option<Out>, GridIdx<A>, F>
    for EmptyGrid<G, In>
where
    G: GridSize + GridSpaceToLinearSpace<IndexArray = A> + Clone + PartialEq,
    A: AsRef<[isize]>,
    F: Fn(GridIdx<A>, Option<In>) -> Option<Out>,
    In: Clone,
    Out: Default + Clone,
{
    type Output = MaskedGrid<G, Out>;

    fn map_indexed_elements(self, map_fn: F) -> Self::Output {
        let grid_shape = self.shape.clone();

        let grid_idx_map_fn = |lin_idx, old_value_option| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx, old_value_option)
        };
        self.map_indexed_elements(grid_idx_map_fn)
    }
}

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(GridIdx, In) -> Out
//    F: Fn(usize, In) -> Out
impl<G, In, Out, Index, F> MapIndexedElements<In, Out, Index, F> for GridOrEmpty<G, In>
where
    G: GridSize,
    F: Fn(Index, In) -> Out,
    In: Clone + 'static,
    Out: Default + Clone + 'static,
    MaskedGrid<G, In>: MapIndexedElements<In, Out, Index, F, Output = MaskedGrid<G, Out>>,
    GridOrEmpty<G, Out>: From<MaskedGrid<G, Out>>,
{
    type Output = GridOrEmpty<G, Out>;

    fn map_indexed_elements(self, map_fn: F) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => GridOrEmpty::from(g.map_indexed_elements(map_fn)),
            GridOrEmpty::Empty(e) => e.convert_dtype::<Out>().into(),
        }
    }
}

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(GridIdx, Option<In>) -> Option<Out>,
//    F: Fn(usize, Option<In>) -> Option<Out>,
impl<G, In, Out, Index, F> MapIndexedElements<Option<In>, Option<Out>, Index, F>
    for GridOrEmpty<G, In>
where
    G: GridSize + Clone + PartialEq,
    F: Fn(Index, Option<In>) -> Option<Out>,
    In: Clone + 'static + Default,
    Out: Default + Clone + 'static,
    MaskedGrid<G, In>:
        MapIndexedElements<Option<In>, Option<Out>, Index, F, Output = MaskedGrid<G, Out>>,
    EmptyGrid<G, In>:
        MapIndexedElements<Option<In>, Option<Out>, Index, F, Output = MaskedGrid<G, Out>>,
    GridOrEmpty<G, Out>: From<MaskedGrid<G, Out>>,
{
    type Output = GridOrEmpty<G, Out>;

    fn map_indexed_elements(self, map_fn: F) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => GridOrEmpty::from(g.map_indexed_elements(map_fn)),
            GridOrEmpty::Empty(e) => {
                // we have to map all the empty pixels. However, if the validity mask is empty we can return an empty grid.
                let mapped = e.map_indexed_elements(map_fn);
                if mapped.mask_ref().data.iter().any(|m| *m) {
                    return mapped.into();
                }
                EmptyGrid::new(mapped.inner_grid.shape).into()
            }
        }
    }
}

// Implementation for RasterTile2D.
// Works with:
//    F: Fn(GridIdx2D, Option<In>) -> Option<Out>,
//    F: Fn(GridIdx2D, In) -> Out
//    F: Fn(usize, Option<In>) -> Option<Out>,
//    F: Fn(usize, In) -> Out
impl<TIn, TOut, FIn, FOut, Index, F> MapIndexedElements<FIn, FOut, Index, F> for RasterTile2D<TIn>
where
    F: Fn(Index, FIn) -> FOut,
    FIn: Clone + 'static,
    TIn: Clone + 'static,
    TOut: Default + Clone + 'static,
    GridOrEmpty2D<TIn>: MapIndexedElements<FIn, FOut, Index, F, Output = GridOrEmpty2D<TOut>>,
{
    type Output = RasterTile2D<TOut>;

    fn map_indexed_elements(self, map_fn: F) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.map_indexed_elements(map_fn),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
        }
    }
}

// Implementation for Grid using usize as index: F: Fn(usize, In) -> Out.
impl<G, In, Out, F> MapIndexedElementsParallel<In, Out, usize, F> for Grid<G, In>
where
    G: GridSize,
    F: Fn(usize, In) -> Out + Send + Sync,
    In: 'static + Sized + Send + Sync,
    Out: Default + Sized + Send + Sync + Clone,
    Vec<In>: Sized,
{
    type Output = Grid<G, Out>;

    fn map_indexed_elements_parallel(self, map_fn: F) -> Self::Output {
        let Grid { shape, data } = self;
        let num_elements_per_thread =
            num::integer::div_ceil(shape.number_of_elements(), rayon::current_num_threads())
                .max(MIN_ELEMENTS_PER_THREAD);

        let mut out_data = Vec::with_capacity(shape.number_of_elements());

        data.into_par_iter()
            .with_min_len(num_elements_per_thread)
            .enumerate()
            .map(|(lin_idx, i)| map_fn(lin_idx, i))
            .collect_into_vec(out_data.as_mut());

        Grid::new(shape, out_data).expect("Grid should have been created.")
    }
}

// Implementation for Grid using GridIdx as index: F: Fn(GridIdx, In) -> Out.
// Delegates to implementation for Grid with F: Fn(usize, In) -> Out.
impl<G, A, In, Out, F> MapIndexedElementsParallel<In, Out, GridIdx<A>, F> for Grid<G, In>
where
    G: GridSpaceToLinearSpace<IndexArray = A> + Clone + GridSize + Send + Sync,
    A: AsRef<[isize]>,
    F: Fn(GridIdx<A>, In) -> Out + Send + Sync,
    In: 'static + Sized + Send + Sync,
    Out: Default + Sized + Send + Sync + Clone,
{
    type Output = Grid<G, Out>;

    fn map_indexed_elements_parallel(self, map_fn: F) -> Self::Output {
        let grid_shape = self.shape.clone();

        let grid_idx_map_fn = |lin_idx: usize, old_value_option: In| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx, old_value_option)
        };
        self.map_indexed_elements_parallel(grid_idx_map_fn)
    }
}

// Implementation for MaskedGrid to map only the inner data with F: Fn(In, Index) -> Out.
impl<G, Index, In, Out, F> MapIndexedElementsParallel<In, Out, Index, F> for MaskedGrid<G, In>
where
    G: GridSize + PartialEq + Clone,
    F: Fn(Index, In) -> Out,
    In: Clone,
    Out: Default + Clone,
    Grid<G, In>: MapIndexedElementsParallel<In, Out, Index, F, Output = Grid<G, Out>>,
{
    type Output = MaskedGrid<G, Out>;

    fn map_indexed_elements_parallel(self, map_fn: F) -> Self::Output {
        let MaskedGrid {
            inner_grid,
            validity_mask,
        } = self;
        let out_grid = inner_grid.map_indexed_elements_parallel(map_fn);

        MaskedGrid::new(out_grid, validity_mask).expect("Masked grid should have been created.")
    }
}

// Implementation for MaskedGrid using usize as index: F: Fn(usize, Option<In>) -> Option<Out>.
impl<G, In, Out, F> MapIndexedElementsParallel<Option<In>, Option<Out>, usize, F>
    for MaskedGrid<G, In>
where
    G: GridSize + PartialEq + Clone + Send + Sync,
    F: Fn(usize, Option<In>) -> Option<Out> + Send + Sync,
    In: Copy + Clone + Sync,
    Out: Default + Clone + Send,
{
    type Output = MaskedGrid<G, Out>;

    fn map_indexed_elements_parallel(self, map_fn: F) -> Self::Output {
        let MaskedGrid {
            inner_grid: data,
            validity_mask,
        } = self;
        debug_assert!(data.data.len() == validity_mask.data.len());
        debug_assert!(data.shape == validity_mask.shape);

        let num_elements_per_thread = num::integer::div_ceil(
            data.shape.number_of_elements(),
            rayon::current_num_threads(),
        )
        .max(MIN_ELEMENTS_PER_THREAD);

        let mut out_data = Vec::with_capacity(data.shape.number_of_elements());
        let mut out_validity = Vec::with_capacity(data.shape.number_of_elements());

        data.data
            .into_par_iter()
            .with_min_len(num_elements_per_thread)
            .zip(
                validity_mask
                    .data
                    .into_par_iter()
                    .with_min_len(num_elements_per_thread),
            )
            .enumerate()
            .map(|(lin_idx, (in_pixel, mask))| {
                let in_masked_value = if mask { Some(*in_pixel) } else { None };

                let out_value_option = map_fn(lin_idx, in_masked_value);

                if let Some(out_value) = out_value_option {
                    (out_value, true)
                } else {
                    (Out::default(), false)
                }
            })
            .unzip_into_vecs(out_data.as_mut(), out_validity.as_mut());

        MaskedGrid::new(
            Grid::new(data.shape, out_data)
                .expect("the shape of the grid and the data size should match"),
            Grid::new(validity_mask.shape, out_validity)
                .expect("the shape of the grid and the data size should match"),
        )
        .expect("the shape of the grid and the data size should match")
    }
}

// Implementation for MaskedGrid using GridIdx as index: F: Fn(GridIdx, Option<In>) -> Option<Out>.
// Delegates to implementation for MaskedGrid with F: Fn(usize, Option<In>) -> Option<Out>.
impl<G, A, In, Out, F> MapIndexedElementsParallel<Option<In>, Option<Out>, GridIdx<A>, F>
    for MaskedGrid<G, In>
where
    G: GridSize + GridSpaceToLinearSpace<IndexArray = A> + PartialEq + Clone + Send + Sync,
    A: AsRef<[isize]>,
    F: Fn(GridIdx<A>, Option<In>) -> Option<Out> + Send + Sync,
    In: Copy + Clone + Sync,
    Out: Default + Clone + Send,
{
    type Output = MaskedGrid<G, Out>;

    fn map_indexed_elements_parallel(self, map_fn: F) -> Self::Output {
        let grid_shape = self.shape().clone();

        let grid_idx_map_fn = |lin_idx, old_value_option| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx, old_value_option)
        };
        self.map_indexed_elements_parallel(grid_idx_map_fn)
    }
}

// Implementation for EmptyGrid using usize as index: F: Fn(usize, Option<In>) -> Option<Out>.
impl<G, In, Out, F> MapIndexedElementsParallel<Option<In>, Option<Out>, usize, F>
    for EmptyGrid<G, In>
where
    G: GridSize + Clone + PartialEq,
    F: Fn(usize, Option<In>) -> Option<Out> + Send + Sync,
    In: Clone,
    Out: Default + Clone + Send + Sync,
{
    type Output = MaskedGrid<G, Out>;

    fn map_indexed_elements_parallel(self, map_fn: F) -> Self::Output {
        let num_elements_per_thread = num::integer::div_ceil(
            self.shape.number_of_elements(),
            rayon::current_num_threads(),
        )
        .max(MIN_ELEMENTS_PER_THREAD);

        let mut out_data = Vec::with_capacity(self.shape.number_of_elements());
        let mut out_validity = Vec::with_capacity(self.shape.number_of_elements());

        (0..self.shape.number_of_elements())
            .into_par_iter()
            .with_min_len(num_elements_per_thread)
            .map(|lin_idx| {
                let in_masked_value = None as Option<In>;

                let out_value_option = map_fn(lin_idx, in_masked_value);
                let is_valid = out_value_option.is_some();

                (out_value_option.unwrap_or_default(), is_valid)
            })
            .unzip_into_vecs(&mut out_data, &mut out_validity);

        MaskedGrid::new(
            Grid::new(self.shape.clone(), out_data)
                .expect("the shape of the grid and the data size should match"),
            Grid::new(self.shape, out_validity)
                .expect("the shape of the grid and the data size should match"),
        )
        .expect("the shape of the grid and the data size should match")
    }
}

// Implementation for EmptyGrid using GridIdx as index: F: Fn(GridIdx, Option<In>) -> Option<Out>.
// Delegates to implementation for EmptyGrid with F: Fn(usize, Option<In>) -> Option<Out>.
impl<G, A, In, Out, F> MapIndexedElementsParallel<Option<In>, Option<Out>, GridIdx<A>, F>
    for EmptyGrid<G, In>
where
    G: GridSize + GridSpaceToLinearSpace<IndexArray = A> + Clone + PartialEq + Send + Sync,
    A: AsRef<[isize]>,
    F: Fn(GridIdx<A>, Option<In>) -> Option<Out> + Send + Sync,
    In: Clone,
    Out: Default + Clone + Send + Sync,
{
    type Output = MaskedGrid<G, Out>;

    fn map_indexed_elements_parallel(self, map_fn: F) -> Self::Output {
        let grid_shape = self.shape.clone();

        let grid_idx_map_fn = |lin_idx, old_value_option| {
            let grid_idx = grid_shape.grid_idx_unchecked(lin_idx);
            map_fn(grid_idx, old_value_option)
        };
        self.map_indexed_elements_parallel(grid_idx_map_fn)
    }
}

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(GridIdx, In) -> Out
//    F: Fn(usize, In) -> Out
impl<G, In, Out, Index, F> MapIndexedElementsParallel<In, Out, Index, F> for GridOrEmpty<G, In>
where
    G: GridSize,
    F: Fn(Index, In) -> Out + Send + Sync,
    In: Default + Copy + Clone + Sync + 'static,
    Out: Default + Clone + Send + 'static,
    MaskedGrid<G, In>: MapIndexedElementsParallel<In, Out, Index, F, Output = MaskedGrid<G, Out>>,
    GridOrEmpty<G, Out>: From<MaskedGrid<G, Out>>,
{
    type Output = GridOrEmpty<G, Out>;

    fn map_indexed_elements_parallel(self, map_fn: F) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => g.map_indexed_elements_parallel(map_fn).into(),
            GridOrEmpty::Empty(e) => e.convert_dtype::<Out>().into(),
        }
    }
}

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(GridIdx, Option<In>) -> Option<Out>,
//    F: Fn(usize, Option<In>) -> Option<Out>,
impl<G, In, Out, Index, F> MapIndexedElementsParallel<Option<In>, Option<Out>, Index, F>
    for GridOrEmpty<G, In>
where
    G: GridSize + PartialEq + Send + Sync + Clone,
    F: Fn(Index, Option<In>) -> Option<Out> + Send + Sync,
    In: Default + Copy + Clone + Sync + 'static,
    Out: Default + Clone + Send + 'static,
    MaskedGrid<G, In>:
        MapIndexedElementsParallel<Option<In>, Option<Out>, Index, F, Output = MaskedGrid<G, Out>>,
    EmptyGrid<G, In>:
        MapIndexedElementsParallel<Option<In>, Option<Out>, Index, F, Output = MaskedGrid<G, Out>>,
    GridOrEmpty<G, Out>: From<MaskedGrid2D<Out>>,
{
    type Output = GridOrEmpty<G, Out>;

    fn map_indexed_elements_parallel(self, map_fn: F) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => g.map_indexed_elements_parallel(map_fn).into(),
            GridOrEmpty::Empty(e) => {
                // we have to map all the empty pixels. However, if the validity mask is empty we can return an empty grid.
                let mapped = e.map_indexed_elements_parallel(map_fn);
                if mapped
                    .mask_ref()
                    .data
                    .iter() // TODO: benchmark if a parallel iterator is faster here.
                    .any(|m| *m)
                {
                    return mapped.into();
                }
                EmptyGrid::new(mapped.inner_grid.shape).into()
            }
        }
    }
}

// Implementation for RasterTile2D.
// Works with:
//    F: Fn(GridIdx2D, Option<In>) -> Option<Out>,
//    F: Fn(GridIdx2D, In) -> Out
//    F: Fn(usize, Option<In>) -> Option<Out>,
//    F: Fn(usize, In) -> Out
impl<TIn, TOut, FIn, FOut, Index, F> MapIndexedElementsParallel<FIn, FOut, Index, F>
    for RasterTile2D<TIn>
where
    F: Fn(Index, FIn) -> FOut + Send + Sync,
    FIn: Default + Copy + Clone + Sync + 'static,
    FOut: Default + Clone + Send + 'static,
    TIn: Default + Copy + Clone + Sync + 'static,
    TOut: Default + Clone + Send + 'static,
    GridOrEmpty2D<TIn>:
        MapIndexedElementsParallel<FIn, FOut, Index, F, Output = GridOrEmpty2D<TOut>>,
{
    type Output = RasterTile2D<TOut>;

    fn map_indexed_elements_parallel(self, map_fn: F) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.map_indexed_elements_parallel(map_fn),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::raster::{EmptyGrid2D, Grid2D, GridIdx2D};

    use super::*;

    #[test]
    fn map_indexed_elements_inner_linear_idx() {
        let dim = [2, 2];
        let data: Vec<i32> = vec![1, 2, 3, 4];

        let r1 = GridOrEmpty::Grid(MaskedGrid::from(Grid2D::new(dim.into(), data).unwrap()));
        let r2 = r1.map_indexed_elements(|idx: usize, p: i32| p + idx as i32);

        let expected = [Some(1), Some(3), Some(5), Some(7)];

        match r2 {
            GridOrEmpty::Grid(g) => {
                let res_options: Vec<Option<i32>> = g.masked_element_deref_iterator().collect();
                assert_eq!(res_options, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn map_indexed_elements_no_data_linear_idx() {
        let dim = [2, 2];
        let data: Vec<i32> = vec![1, 2, 3, 4];
        let validity_mask: Vec<bool> = vec![true, false, true, false];

        let r1 = GridOrEmpty::Grid(
            MaskedGrid::new(
                Grid2D::new(dim.into(), data).unwrap(),
                Grid2D::new(dim.into(), validity_mask).unwrap(),
            )
            .unwrap(),
        );
        let r2 = r1.map_indexed_elements(
            |idx: usize, p: Option<i32>| {
                if p.is_some() {
                    None
                } else {
                    Some(idx as i32)
                }
            },
        );

        let expected = [None, Some(1), None, Some(3)];

        match r2 {
            GridOrEmpty::Grid(g) => {
                let res_options: Vec<Option<i32>> = g.masked_element_deref_iterator().collect();
                assert_eq!(res_options, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn map_indexed_elements_no_data_grid_idx() {
        let dim = [4, 4];
        let data: Vec<i32> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let validity_mask: Vec<bool> = vec![
            true, false, true, false, true, false, true, false, true, false, true, false, true,
            false, true, false,
        ];

        let r1 = GridOrEmpty::Grid(
            MaskedGrid::new(
                Grid2D::new(dim.into(), data).unwrap(),
                Grid2D::new(dim.into(), validity_mask).unwrap(),
            )
            .unwrap(),
        );
        let r2 = r1.map_indexed_elements(|idx: GridIdx2D, p: Option<i32>| {
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

        match r2 {
            GridOrEmpty::Grid(g) => {
                let res_options: Vec<Option<i32>> = g.masked_element_deref_iterator().collect();
                assert_eq!(res_options, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn map_indexed_elements_empty() {
        let dim = [2, 2];

        let r2 = GridOrEmpty::Empty::<_, i32>(EmptyGrid2D::new(dim.into()));
        let r3 = r2.map_indexed_elements(|idx: usize, p: i32| p + idx as i32);

        match r3 {
            GridOrEmpty::Grid(_) => {
                panic!("Expected empty grid")
            }
            GridOrEmpty::Empty(e) => {
                assert_eq!(e.shape, dim.into());
            }
        }
    }
    #[test]
    fn map_indexed_elements_parallel_inner_linear_idx() {
        let dim = [2, 2];
        let data: Vec<i32> = vec![1, 2, 3, 4];

        let r1 = GridOrEmpty::Grid(MaskedGrid::from(Grid2D::new(dim.into(), data).unwrap()));
        let r2 = r1.map_indexed_elements_parallel(|idx: usize, p: i32| p + idx as i32);

        let expected = [Some(1), Some(3), Some(5), Some(7)];

        match r2 {
            GridOrEmpty::Grid(g) => {
                let res_options: Vec<Option<i32>> = g.masked_element_deref_iterator().collect();
                assert_eq!(res_options, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn map_indexed_elements_parallel_no_data_linear_idx() {
        let dim = [2, 2];
        let data: Vec<i32> = vec![1, 2, 3, 4];
        let validity_mask: Vec<bool> = vec![true, false, true, false];

        let r1 = GridOrEmpty::Grid(
            MaskedGrid::new(
                Grid2D::new(dim.into(), data).unwrap(),
                Grid2D::new(dim.into(), validity_mask).unwrap(),
            )
            .unwrap(),
        );
        let r2 =
            r1.map_indexed_elements_parallel(
                |idx: usize, p: Option<i32>| {
                    if p.is_some() {
                        None
                    } else {
                        Some(idx as i32)
                    }
                },
            );

        let expected = [None, Some(1), None, Some(3)];

        match r2 {
            GridOrEmpty::Grid(g) => {
                let res_options: Vec<Option<i32>> = g.masked_element_deref_iterator().collect();
                assert_eq!(res_options, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn map_indexed_elements_parallel_no_data_grid_idx() {
        let dim = [4, 4];
        let data: Vec<i32> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let validity_mask: Vec<bool> = vec![
            true, false, true, false, true, false, true, false, true, false, true, false, true,
            false, true, false,
        ];

        let r1 = GridOrEmpty::Grid(
            MaskedGrid::new(
                Grid2D::new(dim.into(), data).unwrap(),
                Grid2D::new(dim.into(), validity_mask).unwrap(),
            )
            .unwrap(),
        );
        let r2 = r1.map_indexed_elements_parallel(|idx: GridIdx2D, p: Option<i32>| {
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

        match r2 {
            GridOrEmpty::Grid(g) => {
                let res_options: Vec<Option<i32>> = g.masked_element_deref_iterator().collect();
                assert_eq!(res_options, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn map_indexed_elements_parallel_empty() {
        let dim = [2, 2];

        let r2 = GridOrEmpty::Empty::<_, i32>(EmptyGrid2D::new(dim.into()));
        let r3 = r2.map_indexed_elements_parallel(|idx: usize, p: i32| p + idx as i32);

        match r3 {
            GridOrEmpty::Grid(_) => {
                panic!("Expected empty grid")
            }
            GridOrEmpty::Empty(e) => {
                assert_eq!(e.shape, dim.into());
            }
        }
    }
}
