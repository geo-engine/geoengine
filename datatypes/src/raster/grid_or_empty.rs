use std::ops::Add;

use super::{
    grid_traits::{ChangeGridBounds, GridShapeAccess},
    no_data_grid::NoDataGrid,
    Grid, GridBoundingBox, GridBounds, GridIdx, GridIndexAccess, GridShape, GridShape1D,
    GridShape2D, GridShape3D, GridSize, GridSpaceToLinearSpace, NoDataValue,
};

use crate::util::Result;
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};

pub type GridOrEmpty1D<T> = GridOrEmpty<GridShape1D, T>;
pub type GridOrEmpty2D<T> = GridOrEmpty<GridShape2D, T>;
pub type GridOrEmpty3D<T> = GridOrEmpty<GridShape3D, T>;

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type")]
pub enum GridOrEmpty<D, T> {
    Grid(Grid<D, T>),
    Empty(NoDataGrid<D, T>),
}

impl<D, T> GridOrEmpty<D, T>
where
    D: GridSize,
    T: Clone,
{
    pub fn is_empty(&self) -> bool {
        matches!(self, GridOrEmpty::Empty(_))
    }

    pub fn is_grid(&self) -> bool {
        matches!(self, GridOrEmpty::Grid(_))
    }

    pub fn shape_ref(&self) -> &D {
        match self {
            GridOrEmpty::Grid(g) => &g.shape,
            GridOrEmpty::Empty(n) => &n.shape,
        }
    }

    /// Converts the data type of the raster by converting it pixel-wise
    pub fn convert_dtype<To>(self) -> GridOrEmpty<D, To>
    where
        T: AsPrimitive<To> + Copy + 'static,
        To: Copy + 'static,
    {
        match self {
            GridOrEmpty::Grid(g) => GridOrEmpty::Grid(g.convert_dtype()),
            GridOrEmpty::Empty(n) => GridOrEmpty::Empty(n.convert_dtype()),
        }
    }

    pub fn into_materialized_grid(self) -> Grid<D, T> {
        match self {
            GridOrEmpty::Grid(g) => g,
            GridOrEmpty::Empty(n) => n.into(),
        }
    }
}

impl<D, T> GridSize for GridOrEmpty<D, T>
where
    D: GridSize + GridSpaceToLinearSpace,
    T: Clone,
{
    type ShapeArray = D::ShapeArray;

    const NDIM: usize = D::NDIM;

    fn axis_size(&self) -> Self::ShapeArray {
        self.shape_ref().axis_size()
    }

    fn number_of_elements(&self) -> usize {
        self.shape_ref().number_of_elements()
    }
}

impl<T, D, I, A> GridIndexAccess<T, I> for GridOrEmpty<D, T>
where
    D: GridSize + GridSpaceToLinearSpace<IndexArray = A> + GridBounds<IndexArray = A>,
    I: Into<GridIdx<A>>,
    A: AsRef<[isize]> + Into<GridIdx<A>> + Clone,
    T: Copy,
{
    fn get_at_grid_index(&self, grid_index: I) -> Result<T> {
        match self {
            GridOrEmpty::Grid(g) => g.get_at_grid_index(grid_index),
            GridOrEmpty::Empty(n) => n.get_at_grid_index(grid_index),
        }
    }

    fn get_at_grid_index_unchecked(&self, grid_index: I) -> T {
        match self {
            GridOrEmpty::Grid(g) => g.get_at_grid_index_unchecked(grid_index),
            GridOrEmpty::Empty(n) => n.get_at_grid_index_unchecked(grid_index),
        }
    }
}

impl<D, T, I> GridBounds for GridOrEmpty<D, T>
where
    D: GridBounds<IndexArray = I> + GridSpaceToLinearSpace<IndexArray = I>,
    T: Clone,
    I: AsRef<[isize]> + Into<GridIdx<I>>,
{
    type IndexArray = I;

    fn min_index(&self) -> GridIdx<Self::IndexArray> {
        match self {
            GridOrEmpty::Grid(g) => g.min_index(),
            GridOrEmpty::Empty(n) => n.min_index(),
        }
    }

    fn max_index(&self) -> GridIdx<Self::IndexArray> {
        match self {
            GridOrEmpty::Grid(g) => g.max_index(),
            GridOrEmpty::Empty(n) => n.max_index(),
        }
    }
}

impl<D, T> GridShapeAccess for GridOrEmpty<D, T>
where
    D: GridSize,
    D::ShapeArray: Into<GridShape<D::ShapeArray>>,
    T: Copy,
{
    type ShapeArray = D::ShapeArray;

    fn grid_shape_array(&self) -> Self::ShapeArray {
        match self {
            GridOrEmpty::Grid(g) => g.grid_shape_array(),
            GridOrEmpty::Empty(n) => n.grid_shape_array(),
        }
    }
}

impl<D, T> From<NoDataGrid<D, T>> for GridOrEmpty<D, T>
where
    T: Clone,
{
    fn from(no_grid_array: NoDataGrid<D, T>) -> Self {
        GridOrEmpty::Empty(no_grid_array)
    }
}

impl<D, T> From<Grid<D, T>> for GridOrEmpty<D, T>
where
    T: Clone,
{
    fn from(grid: Grid<D, T>) -> Self {
        GridOrEmpty::Grid(grid)
    }
}

impl<D, T> NoDataValue for GridOrEmpty<D, T>
where
    T: PartialEq + Clone,
{
    type NoDataType = T;

    fn no_data_value(&self) -> Option<Self::NoDataType> {
        match self {
            GridOrEmpty::Grid(g) => g.no_data_value(),
            GridOrEmpty::Empty(n) => n.no_data_value(),
        }
    }
}

impl<D, T, I> ChangeGridBounds<I> for GridOrEmpty<D, T>
where
    I: AsRef<[isize]> + Clone,
    D: GridBounds<IndexArray = I> + Clone + GridSpaceToLinearSpace<IndexArray = I>,
    T: Clone,
    GridBoundingBox<I>: GridSize,
    GridIdx<I>: Add<Output = GridIdx<I>> + From<I>,
{
    type Output = GridOrEmpty<GridBoundingBox<I>, T>;

    fn shift_by_offset(self, offset: GridIdx<I>) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => GridOrEmpty::Grid(g.shift_by_offset(offset)),
            GridOrEmpty::Empty(n) => GridOrEmpty::Empty(n.shift_by_offset(offset)),
        }
    }

    fn set_grid_bounds(self, bounds: GridBoundingBox<I>) -> Result<Self::Output> {
        match self {
            GridOrEmpty::Grid(g) => g.set_grid_bounds(bounds).map(Into::into),
            GridOrEmpty::Empty(n) => n.set_grid_bounds(bounds).map(Into::into),
        }
    }
}
