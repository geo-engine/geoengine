use crate::raster::{Grid, GridOrEmpty, GridOrEmpty2D, GridSize, MaskedGrid, RasterTile2D};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

const MIN_ELEMENTS_PER_THREAD: usize = 16 * 512;

/// This trait models a map operation from a `Grid` of type `In` into a `Grid` of Type `Out`. This is done using a provided function that maps each element to a new value.
///
/// Most useful implementations are on: `Grid`, `MaskedGrid`, `GridOrEmpty` and `RasterTile2D`.
///
/// On `Grid` elements are mapped as `|element: In| { element + 1 }` with `F: Fn(In) -> Out`
///
/// On `MaskedGrid` elements are mapped ignoring _no data_ as `|element: In| { element + 1 }` with `F: Fn(In) -> Out` or handling _no data_ as `|element: Option<In>| { element.map(|e| e+ 1) }` with `F: Fn(Option<In>) -> Option<Out>`.
pub trait MapElements<In, Out, F: Fn(In) -> Out> {
    type Output;
    /// Create a new instance from the current one. The `map_fn` transforms all elements to a new value.
    fn map_elements(self, map_fn: F) -> Self::Output;
}

/// This trait is equal to `MapElements` but uses a thread pool to do the operation in parallel.
/// This trait models a map operation from a `Grid` of type `In` into a `Grid` of Type `Out`. This is done using a provided function that maps each element to a new value.
///
/// Most usefull implementations are on: `Grid`, `MaskedGrid`, `GridOrEmpty` and `RasterTile2D`.
///
/// On `Grid` elements are mapped as `|element: In| { element + 1 }` with `F: Fn(In) -> Out`
///
/// On `MaskedGrid` elements are mapped ignoring _no data_ as `|element: In| { element + 1 }` with `F: Fn(In) -> Out` or handling _no data_ as `|element: Option<In>| { element.map(|e| e+ 1) }` with `F: Fn(Option<In>) -> Option<Out>`.

pub trait MapElementsParallel<In, Out, F: Fn(In) -> Out> {
    type Output;
    /// Create a new instance from the current one. The `map_fn` transforms all elements to a new value. Use a `ThreadPool` for parallel map operations.
    fn map_elements_parallel(self, map_fn: F) -> Self::Output;
}

// Implementation for Grid: F: Fn(In) -> Out.
impl<In, Out, F, G> MapElements<In, Out, F> for Grid<G, In>
where
    In: 'static,
    Out: 'static,
    G: GridSize + Clone,
    F: Fn(In) -> Out,
{
    type Output = Grid<G, Out>;
    fn map_elements(self, map_fn: F) -> Self::Output {
        let shape = self.shape;
        let data = self.data.into_iter().map(map_fn).collect();

        Grid { shape, data }
    }
}

// Implementation for MaskedGrid to enable update of the inner data with F: Fn(T) -> T.
impl<In, Out, F, G> MapElements<In, Out, F> for MaskedGrid<G, In>
where
    In: 'static + Clone,
    Out: 'static + Clone,
    G: GridSize + Clone + PartialEq,
    F: Fn(In) -> Out,
    Grid<G, In>: MapElements<In, Out, F, Output = Grid<G, Out>>,
{
    type Output = MaskedGrid<G, Out>;

    fn map_elements(self, map_fn: F) -> Self::Output {
        MaskedGrid::new(self.inner_grid.map_elements(map_fn), self.validity_mask)
            .expect("Creation failed for prev valid dimensions")
    }
}

// Implementation for MaskedGrid: F: Fn(Option<T>) -> Option<T>.
impl<In, Out, F, G> MapElements<Option<In>, Option<Out>, F> for MaskedGrid<G, In>
where
    In: 'static + Clone,
    Out: 'static + Clone + Default,
    G: GridSize + Clone + PartialEq,
    F: Fn(Option<In>) -> Option<Out>,
{
    type Output = MaskedGrid<G, Out>;

    fn map_elements(self, map_fn: F) -> Self::Output {
        let MaskedGrid {
            inner_grid: data,
            mut validity_mask,
        } = self;

        let mut new_data = Grid::new_filled(data.shape.clone(), Out::default());

        new_data
            .data
            .iter_mut()
            .zip(validity_mask.data.iter_mut())
            .zip(data.data.into_iter())
            .for_each(|((o, m), i)| {
                let in_value = if *m { Some(i) } else { None };

                let new_out_value = map_fn(in_value);
                *m = new_out_value.is_some();

                if let Some(out_value) = new_out_value {
                    *o = out_value;
                }
            });

        MaskedGrid::new(new_data, validity_mask)
            .expect("Creation of grid with dimension failed before")
    }
}

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(T) -> T
impl<In, Out, F, G> MapElements<In, Out, F> for GridOrEmpty<G, In>
where
    In: 'static + Copy,
    Out: 'static + Copy,
    G: GridSize + Clone + PartialEq,
    F: Fn(In) -> Out,
    MaskedGrid<G, In>: MapElements<In, Out, F, Output = MaskedGrid<G, Out>>,
{
    type Output = GridOrEmpty<G, Out>;

    fn map_elements(self, map_fn: F) -> Self::Output {
        match self {
            GridOrEmpty::Grid(grid) => GridOrEmpty::Grid(grid.map_elements(map_fn)),
            GridOrEmpty::Empty(empty) => GridOrEmpty::Empty(empty.convert_dtype()),
        }
    }
}

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(Option<T>) -> Option<T>,
impl<In, Out, F, G> MapElements<Option<In>, Option<Out>, F> for GridOrEmpty<G, In>
where
    In: 'static,
    Out: 'static + Clone,
    G: GridSize + Clone + PartialEq,
    F: Fn(Option<In>) -> Option<Out>,
    MaskedGrid<G, In>: MapElements<Option<In>, Option<Out>, F, Output = MaskedGrid<G, Out>>,
{
    type Output = GridOrEmpty<G, Out>;

    fn map_elements(self, map_fn: F) -> Self::Output {
        match self {
            GridOrEmpty::Grid(grid) => GridOrEmpty::Grid(grid.map_elements(map_fn)),
            GridOrEmpty::Empty(empty) => {
                // if None maps to a value we can be sure that the whole grid will turn to that value.
                if let Some(fill_value) = map_fn(None as Option<In>) {
                    return GridOrEmpty::Grid(MaskedGrid::new_filled(empty.shape, fill_value));
                }
                GridOrEmpty::Empty(empty.convert_dtype())
            }
        }
    }
}

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(Option<T>) -> Option<T>,
//    F: Fn(T) -> T
impl<FIn, FOut, TIn, TOut, F> MapElements<FIn, FOut, F> for RasterTile2D<TIn>
where
    FIn: 'static + Copy,
    FOut: 'static + Copy,
    TIn: 'static,
    TOut: 'static,
    F: Fn(FIn) -> FOut,
    GridOrEmpty2D<TIn>: MapElements<FIn, FOut, F, Output = GridOrEmpty2D<TOut>>,
{
    type Output = RasterTile2D<TOut>;

    fn map_elements(self, map_fn: F) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.map_elements(map_fn),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
        }
    }
}

// Implementation for Grid: F: Fn(In) -> Out.
impl<In, Out, F, G> MapElementsParallel<In, Out, F> for Grid<G, In>
where
    In: 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync,
    G: GridSize + Clone + Send + Sync,
    F: Fn(In) -> Out + Sync + Send,
{
    type Output = Grid<G, Out>;
    fn map_elements_parallel(self, map_fn: F) -> Self::Output {
        let shape = self.shape.clone();
        let num_elements_per_thread = num::integer::div_ceil(
            self.shape.number_of_elements(),
            rayon::current_num_threads(),
        )
        .max(MIN_ELEMENTS_PER_THREAD);

        let mut data = Vec::with_capacity(self.shape.number_of_elements());

        self.data
            .into_par_iter()
            .with_min_len(num_elements_per_thread)
            .map(map_fn)
            .collect_into_vec(data.as_mut());
        Grid { shape, data }
    }
}

// Implementation for MaskedGrid to enable update of the inner data with F: Fn(T) -> T.
impl<In, Out, F, G> MapElementsParallel<In, Out, F> for MaskedGrid<G, In>
where
    In: 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync,
    G: GridSize + Clone + Send + Sync + PartialEq,
    F: Fn(In) -> Out + Sync + Send,
{
    type Output = MaskedGrid<G, Out>;
    fn map_elements_parallel(self, map_fn: F) -> Self::Output {
        let MaskedGrid {
            inner_grid: data,
            validity_mask,
        } = self;
        let new_data = data.map_elements_parallel(map_fn);
        MaskedGrid::new(new_data, validity_mask).expect("Grid creation failed before")
    }
}

// Implementation for MaskedGrid: F: Fn(Option<T>) -> Option<T>.
impl<In, Out, F, G> MapElementsParallel<Option<In>, Option<Out>, F> for MaskedGrid<G, In>
where
    In: 'static + Copy + Send + Sync,
    Out: 'static + Copy + Send + Sync + Default,
    G: GridSize + Clone + Send + Sync + PartialEq,
    F: Fn(Option<In>) -> Option<Out> + Sync + Send,
{
    type Output = MaskedGrid<G, Out>;
    fn map_elements_parallel(self, map_fn: F) -> Self::Output {
        let MaskedGrid {
            inner_grid: data,
            validity_mask,
        } = self;

        let shape = data.shape.clone();
        let num_elements_per_thread =
            num::integer::div_ceil(shape.number_of_elements(), rayon::current_num_threads())
                .max(MIN_ELEMENTS_PER_THREAD);

        let mut out_data = Vec::with_capacity(shape.number_of_elements());
        let mut out_validity = Vec::with_capacity(shape.number_of_elements());

        data.data
            .into_par_iter()
            .with_min_len(num_elements_per_thread)
            .zip(
                validity_mask
                    .data
                    .into_par_iter()
                    .with_min_len(num_elements_per_thread),
            )
            .map(|(i, m)| {
                let in_value = if m { Some(i) } else { None };

                if let Some(o) = map_fn(in_value) {
                    (o, m)
                } else {
                    (Out::default(), false)
                }
            })
            .unzip_into_vecs(out_data.as_mut(), out_validity.as_mut());

        MaskedGrid::new(
            Grid::new(shape.clone(), out_data).expect("Grid creation failed before"),
            Grid::new(shape, out_validity).expect("Grid creation failed before"),
        )
        .expect("Grid creation failed before")
    }
}

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(T) -> T
impl<In, Out, F, G> MapElementsParallel<In, Out, F> for GridOrEmpty<G, In>
where
    In: 'static,
    Out: 'static,
    G: GridSize,
    F: Fn(In) -> Out + Send + Sync,
    MaskedGrid<G, In>: MapElementsParallel<In, Out, F, Output = MaskedGrid<G, Out>>,
{
    type Output = GridOrEmpty<G, Out>;

    fn map_elements_parallel(self, map_fn: F) -> Self::Output {
        match self {
            GridOrEmpty::Grid(grid) => GridOrEmpty::Grid(grid.map_elements_parallel(map_fn)),
            GridOrEmpty::Empty(empty) => GridOrEmpty::Empty(empty.convert_dtype()),
        }
    }
}

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(Option<T>) -> Option<T>,
impl<In, Out, F, G> MapElementsParallel<Option<In>, Option<Out>, F> for GridOrEmpty<G, In>
where
    In: 'static,
    Out: 'static + Clone,
    G: GridSize + PartialEq + Clone,
    F: Fn(Option<In>) -> Option<Out> + Send + Sync,
    MaskedGrid<G, In>: MapElementsParallel<Option<In>, Option<Out>, F, Output = MaskedGrid<G, Out>>,
{
    type Output = GridOrEmpty<G, Out>;

    fn map_elements_parallel(self, map_fn: F) -> Self::Output {
        match self {
            GridOrEmpty::Grid(grid) => GridOrEmpty::Grid(grid.map_elements_parallel(map_fn)),
            GridOrEmpty::Empty(empty) => {
                // if None maps to a value we can be sure that the whole grid will turn to that value.
                if let Some(fill_value) = map_fn(None as Option<In>) {
                    return GridOrEmpty::Grid(MaskedGrid::new_filled(empty.shape, fill_value));
                }
                GridOrEmpty::Empty(empty.convert_dtype())
            }
        }
    }
}

// Implementation for GridOrEmpty.
// Works with:
//    F: Fn(Option<T>) -> Option<T>,
//    F: Fn(T) -> T
impl<FIn, FOut, TIn, TOut, F> MapElementsParallel<FIn, FOut, F> for RasterTile2D<TIn>
where
    TIn: 'static,
    TOut: 'static,
    F: Fn(FIn) -> FOut + Send + Sync,
    GridOrEmpty2D<TIn>: MapElementsParallel<FIn, FOut, F, Output = GridOrEmpty2D<TOut>>,
{
    type Output = RasterTile2D<TOut>;

    fn map_elements_parallel(self, map_fn: F) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.map_elements_parallel(map_fn),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        primitives::TimeInterval,
        raster::{EmptyGrid2D, GeoTransform, Grid2D},
        util::test::TestDefault,
    };

    use super::*;

    #[test]
    fn map_grid() {
        let dim = [2, 2];
        let data = vec![1, 2, 3, 4];

        let r1 = Grid2D::new(dim.into(), data).unwrap();
        let scaled_r1 = r1.map_elements(|p| p * 2 + 1);

        let expected = [3, 5, 7, 9];
        assert_eq!(scaled_r1.data, expected);
    }

    #[test]
    fn map_grid_or_empty() {
        let dim = [2, 2];
        let data = vec![1, 2, 3, 4];

        let r1 = GridOrEmpty::Grid(MaskedGrid::from(Grid2D::new(dim.into(), data).unwrap()));
        let scaled_r1 = r1.map_elements(|p: i32| p * 2 + 1);

        let expected = [3, 5, 7, 9];

        match scaled_r1 {
            GridOrEmpty::Grid(g) => {
                assert_eq!(g.inner_grid.data, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }

        let r2 = GridOrEmpty::Empty::<_, u8>(EmptyGrid2D::new(dim.into()));
        let scaled_r2 = r2.map_elements(|p: u8| p - 10);

        match scaled_r2 {
            GridOrEmpty::Grid(_) => {
                panic!("Expected empty grid")
            }
            GridOrEmpty::Empty(e) => {
                assert_eq!(e.shape, dim.into());
            }
        }
    }

    #[test]
    fn map_grid_parallel() {
        let dim = [2, 2];
        let data = vec![7, 7, 8, 8];

        let r1 = Grid2D::new(dim.into(), data).unwrap();
        let scaled_r1 = r1.map_elements_parallel(|p: i32| p * 2 + 1);

        let expected = [15, 15, 17, 17];
        assert_eq!(scaled_r1.data, expected);
    }

    #[test]
    fn map_grid_or_empty_parallel() {
        let dim = [2, 2];
        let data = vec![7, 7, 8, 8];

        let r1 = GridOrEmpty::Grid(MaskedGrid::from(Grid2D::new(dim.into(), data).unwrap()));
        let scaled_r1 = r1.map_elements_parallel(|p: i32| p * 2 + 1);

        let expected = [15, 15, 17, 17];

        match scaled_r1 {
            GridOrEmpty::Grid(g) => {
                assert_eq!(g.inner_grid.data, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }

        let r2 = GridOrEmpty::Empty::<_, u8>(EmptyGrid2D::new(dim.into()));
        let scaled_r2 = r2.map_elements_parallel(|p: u8| p - 10);

        match scaled_r2 {
            GridOrEmpty::Grid(_) => {
                panic!("Expected empty grid")
            }
            GridOrEmpty::Empty(e) => {
                assert_eq!(e.shape, dim.into());
            }
        }
    }

    #[test]
    fn map_raster_tile_parallel() {
        let dim = [2, 2];
        let data = vec![7, 7, 8, 8];
        let geo = GeoTransform::test_default();

        let r1 = GridOrEmpty::Grid(MaskedGrid::from(Grid2D::new(dim.into(), data).unwrap()));
        let t1 = RasterTile2D::new(TimeInterval::default(), [0, 0].into(), geo, r1);

        let scaled_r1 = t1.map_elements_parallel(|p: u8| p * 2 + 1);
        let mat_scaled_r1 = scaled_r1.into_materialized_tile();

        let expected = [15, 15, 17, 17];

        assert_eq!(mat_scaled_r1.grid_array.inner_grid.data, expected);
    }

    #[test]
    fn map_raster_tile() {
        let dim = [2, 2];
        let data = vec![7, 7, 8, 8];
        let geo = GeoTransform::test_default();

        let r1 = GridOrEmpty::Grid(MaskedGrid::from(Grid2D::new(dim.into(), data).unwrap()));
        let t1 = RasterTile2D::new(TimeInterval::default(), [0, 0].into(), geo, r1);

        let scaled_r1 = t1.map_elements(|p| {
            if let Some(p) = p {
                if p == 7 {
                    Some(p * 2 + 1)
                } else {
                    None
                }
            } else {
                None
            }
        });
        let mat_scaled_r1 = scaled_r1.into_materialized_tile();

        let expected = [15, 15, 0, 0];
        let expected_mask = [true, true, false, false];

        assert_eq!(mat_scaled_r1.grid_array.inner_grid.data, expected);
        assert_eq!(mat_scaled_r1.grid_array.validity_mask.data, expected_mask);
    }
}
