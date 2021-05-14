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
pub struct EmptyGrid<D, T> {
    pub shape: D,
    pub no_data_value: T,
}

pub type NoDataGrid1D<T> = EmptyGrid<GridShape1D, T>;
pub type NoDataGrid2D<T> = EmptyGrid<GridShape2D, T>;
pub type NoDataGrid3D<T> = EmptyGrid<GridShape3D, T>;

impl<D, T> EmptyGrid<D, T>
where
    D: GridSize,
    T: Copy,
{
    /// Creates a new `NoDataGrid`
    pub fn new(shape: D, no_data_value: T) -> Self {
        Self {
            shape,
            no_data_value,
        }
    }

    /// Converts the data type of the raster by converting it pixel-wise
    pub fn convert_dtype<To>(self) -> EmptyGrid<D, To>
    where
        T: AsPrimitive<To> + Copy + 'static,
        To: Copy + 'static,
    {
        EmptyGrid::new(self.shape, self.no_data_value.as_())
    }
}

impl<D, T> GridSize for EmptyGrid<D, T>
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

impl<T, D, I, A> GridIndexAccess<T, I> for EmptyGrid<D, T>
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

impl<T, D> GridBounds for EmptyGrid<D, T>
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

impl<D, T> GridShapeAccess for EmptyGrid<D, T>
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

impl<D, T> From<EmptyGrid<D, T>> for Grid<D, T>
where
    T: Clone,
    D: GridSize,
{
    fn from(no_grid_array: EmptyGrid<D, T>) -> Self {
        Grid::new_filled(
            no_grid_array.shape,
            no_grid_array.no_data_value.clone(),
            Some(no_grid_array.no_data_value),
        )
    }
}

impl<D, T> NoDataValue for EmptyGrid<D, T>
where
    T: PartialEq + Copy,
{
    type NoDataType = T;

    fn no_data_value(&self) -> Option<Self::NoDataType> {
        Some(self.no_data_value)
    }
}

impl<D, T, I> ChangeGridBounds<I> for EmptyGrid<D, T>
where
    I: AsRef<[isize]> + Clone,
    D: GridBounds<IndexArray = I> + Clone,
    T: Copy,
    GridBoundingBox<I>: GridSize,
    GridIdx<I>: Add<Output = GridIdx<I>> + From<I>,
{
    type Output = EmptyGrid<GridBoundingBox<I>, T>;

    fn shift_by_offset(self, offset: GridIdx<I>) -> Self::Output {
        EmptyGrid {
            shape: self.shift_bounding_box(offset),
            no_data_value: self.no_data_value,
        }
    }

    fn set_grid_bounds(self, bounds: GridBoundingBox<I>) -> Result<Self::Output> {
        Ok(EmptyGrid::new(bounds, self.no_data_value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new() {
        let n = NoDataGrid2D::new([2, 2].into(), 42);
        let expected = EmptyGrid {
            shape: GridShape2D::from([2, 2]),
            no_data_value: 42,
        };

        assert_eq!(n.no_data_value, 42);
        assert_eq!(n, expected);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn convert_dtype() {
        let n = NoDataGrid2D::new([2, 2].into(), 42);
        let n_converted = n.convert_dtype::<f64>();
        assert_eq!(n_converted.no_data_value, 42.);
    }

    #[test]
    fn ndim() {
        assert_eq!(NoDataGrid1D::<i32>::NDIM, 1);
        assert_eq!(NoDataGrid2D::<f64>::NDIM, 2);
        assert_eq!(NoDataGrid3D::<u16>::NDIM, 3);
    }

    #[test]
    fn axis_size() {
        let n = NoDataGrid2D::new([2, 2].into(), 42);
        assert_eq!(n.axis_size(), [2, 2]);
    }

    #[test]
    fn number_of_elements() {
        let n = NoDataGrid2D::new([2, 2].into(), 42);
        assert_eq!(n.number_of_elements(), 4);
    }

    #[test]
    fn get_at_grid_index_unchecked() {
        let n = NoDataGrid2D::new([2, 2].into(), 42);
        assert_eq!(n.get_at_grid_index_unchecked([0, 0]), 42);
        assert_eq!(n.get_at_grid_index_unchecked([100, 100]), 42);
    }

    #[test]
    fn get_at_grid_index() {
        let n = NoDataGrid2D::new([2, 2].into(), 42);
        let result = n.get_at_grid_index([0, 0]).unwrap();
        assert_eq!(result, 42);
        assert!(n.get_at_grid_index([100, 100]).is_err());
    }
}
