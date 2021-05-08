use std::ops::Add;

use super::{
    grid_traits::{ChangeGridBounds, GridShapeAccess},
    Grid, GridBoundingBox, GridBounds, GridIdx, GridIndexAccess, GridShape, GridShape1D,
    GridShape2D, GridShape3D, GridSize, GridSpaceToLinearSpace, NoDataValue,
};
use crate::{
    error::{self},
    raster::GridContains,
    util::Result,
};
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use snafu::ensure;

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoDataGrid<D, T> {
    pub shape: D,
    pub no_data_value: T,
}

pub type NoDataGrid1D<T> = NoDataGrid<GridShape1D, T>;
pub type NoDataGrid2D<T> = NoDataGrid<GridShape2D, T>;
pub type NoDataGrid3D<T> = NoDataGrid<GridShape3D, T>;

impl<D, T> NoDataGrid<D, T>
where
    D: GridSize,
    T: Clone,
{
    /// Creates a new `Grid`
    ///
    /// # Errors
    ///
    /// This constructor fails if the data container's capacity is different from the grid's dimension number
    ///
    pub fn new(shape: D, no_data_value: T) -> Result<Self> {
        Ok(Self {
            shape,
            no_data_value,
        })
    }

    /// Converts the data type of the raster by converting it pixel-wise
    pub fn convert_dtype<To>(self) -> NoDataGrid<D, To>
    where
        T: AsPrimitive<To> + Copy + 'static,
        To: Copy + 'static,
    {
        NoDataGrid::new(self.shape, self.no_data_value.as_())
            .expect("grid array type conversion cannot fail")
    }
}

impl<D, T> GridSize for NoDataGrid<D, T>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    type ShapeArray = D::ShapeArray;

    const NDIM: usize = D::NDIM;

    fn axis_size(&self) -> Self::ShapeArray {
        self.shape.axis_size()
    }

    fn number_of_elements(&self) -> usize {
        self.shape.number_of_elements()
    }
}

impl<T, D, I, A> GridIndexAccess<T, I> for NoDataGrid<D, T>
where
    D: GridSize + GridSpaceToLinearSpace<IndexArray = A> + GridBounds<IndexArray = A>,
    I: Into<GridIdx<A>>,
    A: AsRef<[isize]> + Into<GridIdx<A>> + Clone,
    T: Copy,
{
    fn get_at_grid_index(&self, grid_index: I) -> Result<T> {
        let index = grid_index.into();
        ensure!(
            self.shape.contains(&index),
            error::GridIndexOutOfBounds {
                index: index.as_slice(),
                min_index: self.shape.min_index().as_slice(),
                max_index: self.shape.max_index().as_slice()
            }
        );
        Ok(self.get_at_grid_index_unchecked(index))
    }

    fn get_at_grid_index_unchecked(&self, _grid_index: I) -> T {
        self.no_data_value
    }
}

impl<T, D> GridBounds for NoDataGrid<D, T>
where
    D: GridBounds,
{
    type IndexArray = D::IndexArray;

    fn min_index(&self) -> GridIdx<Self::IndexArray> {
        self.shape.min_index()
    }

    fn max_index(&self) -> GridIdx<Self::IndexArray> {
        self.shape.max_index()
    }
}

impl<D, T> GridShapeAccess for NoDataGrid<D, T>
where
    D: GridSize,
    D::ShapeArray: Into<GridShape<D::ShapeArray>>,
    T: Copy,
{
    type ShapeArray = D::ShapeArray;

    fn grid_shape_array(&self) -> Self::ShapeArray {
        self.shape.axis_size()
    }
}

impl<D, T> From<NoDataGrid<D, T>> for Grid<D, T>
where
    T: Clone,
    D: GridSize,
{
    fn from(no_grid_array: NoDataGrid<D, T>) -> Self {
        Grid::new_filled(
            no_grid_array.shape,
            no_grid_array.no_data_value.clone(),
            Some(no_grid_array.no_data_value),
        )
    }
}

impl<D, T> NoDataValue for NoDataGrid<D, T>
where
    T: PartialEq + Clone,
{
    type NoDataType = T;

    fn no_data_value(&self) -> Option<Self::NoDataType> {
        Some(self.no_data_value.clone())
    }
}

impl<D, T, I> ChangeGridBounds<I> for NoDataGrid<D, T>
where
    I: AsRef<[isize]> + Clone,
    D: GridBounds<IndexArray = I> + Clone,
    T: Clone,
    GridBoundingBox<I>: GridSize,
    GridIdx<I>: Add<Output = GridIdx<I>> + From<I>,
{
    type Output = NoDataGrid<GridBoundingBox<I>, T>;

    fn shift_by_offset(self, offset: GridIdx<I>) -> Self::Output {
        NoDataGrid {
            shape: self.shift_bounding_box(offset),
            no_data_value: self.no_data_value,
        }
    }

    fn set_grid_bounds(self, bounds: GridBoundingBox<I>) -> Result<Self::Output> {
        NoDataGrid::new(bounds, self.no_data_value)
    }
}
