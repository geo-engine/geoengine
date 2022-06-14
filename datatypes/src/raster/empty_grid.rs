use std::{marker::PhantomData, ops::Add};

use super::{
    grid_traits::{ChangeGridBounds, GridShapeAccess, MaskedGridIndexAccess},
    GridBoundingBox, GridBounds, GridIdx, GridShape, GridShape1D, GridShape2D, GridShape3D,
    GridSize, GridSpaceToLinearSpace,
};
use crate::{
    error::{self},
    raster::GridContains,
    util::Result,
};
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use snafu::ensure;

#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmptyGrid<D, T> {
    pub shape: D,
    pub _phantom_data: PhantomData<T>,
}

pub type EmptyGrid1D<T> = EmptyGrid<GridShape1D, T>;
pub type EmptyGrid2D<T> = EmptyGrid<GridShape2D, T>;
pub type EmptyGrid3D<T> = EmptyGrid<GridShape3D, T>;

impl<D, T> EmptyGrid<D, T>
where
    D: GridSize,
{
    /// Creates a new `NoDataGrid`
    pub fn new(shape: D) -> Self {
        Self {
            shape,
            _phantom_data: PhantomData,
        }
    }

    /// Converts the data type of the raster by converting it pixel-wise
    pub fn convert_dtype<To>(self) -> EmptyGrid<D, To>
    where
        T: 'static,
        To: 'static,
    {
        EmptyGrid::new(self.shape)
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

impl<T, D, I, A> MaskedGridIndexAccess<T, I> for EmptyGrid<D, T>
where
    D: GridSize + GridSpaceToLinearSpace<IndexArray = A> + GridBounds<IndexArray = A>,
    I: Into<GridIdx<A>>,
    A: AsRef<[isize]> + Into<GridIdx<A>> + Clone,
    T: Copy,
{
    fn get_masked_at_grid_index(&self, grid_index: I) -> Result<Option<T>> {
        let index = grid_index.into();
        ensure!(
            self.shape.contains(&index),
            error::GridIndexOutOfBounds {
                index: index.as_slice(),
                min_index: self.shape.min_index().as_slice(),
                max_index: self.shape.max_index().as_slice()
            }
        );
        Ok(None)
    }

    fn get_masked_at_grid_index_unchecked(&self, _grid_index: I) -> Option<T> {
        None
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
        EmptyGrid::new(self.shift_bounding_box(offset))
    }

    fn set_grid_bounds(self, bounds: GridBoundingBox<I>) -> Result<Self::Output> {
        Ok(EmptyGrid::new(bounds))
    }
}

#[cfg(test)]
mod tests {
    use crate::raster::BoundedGrid;
    use crate::raster::GridBoundingBox2D;

    use super::*;

    #[test]
    fn new() {
        let n: EmptyGrid2D<u8> = EmptyGrid2D::new([2, 2].into());
        let expected = EmptyGrid {
            shape: GridShape2D::from([2, 2]),
            _phantom_data: PhantomData,
        };
        assert_eq!(n, expected);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn convert_dtype() {
        let n: EmptyGrid2D<u8> = EmptyGrid2D::new([2, 2].into());
        let _n_converted = n.convert_dtype::<f64>();
    }

    #[test]
    fn ndim() {
        assert_eq!(EmptyGrid1D::<i32>::NDIM, 1);
        assert_eq!(EmptyGrid2D::<f64>::NDIM, 2);
        assert_eq!(EmptyGrid3D::<u16>::NDIM, 3);
    }

    #[test]
    fn axis_size() {
        let n: EmptyGrid2D<u8> = EmptyGrid2D::new([2, 2].into());
        assert_eq!(n.axis_size(), [2, 2]);
    }

    #[test]
    fn number_of_elements() {
        let n: EmptyGrid2D<u8> = EmptyGrid2D::new([2, 2].into());
        assert_eq!(n.number_of_elements(), 4);
    }

    #[test]
    fn get_at_grid_index_unchecked() {
        let n: EmptyGrid2D<u8> = EmptyGrid2D::new([2, 2].into());
        assert_eq!(n.get_masked_at_grid_index_unchecked([0, 0]), None);
        assert_eq!(n.get_masked_at_grid_index_unchecked([100, 100]), None);
    }

    #[test]
    fn get_at_grid_index() {
        let n: EmptyGrid2D<u8> = EmptyGrid2D::new([2, 2].into());
        let result = n.get_masked_at_grid_index([0, 0]).unwrap();
        assert_eq!(result, None);
        assert!(n.get_masked_at_grid_index([100, 100]).is_err());
    }

    #[test]
    fn grid_bounds_2d() {
        let dim: GridShape2D = [3, 2].into();
        let raster2d: EmptyGrid2D<u8> = EmptyGrid::new(dim);

        assert_eq!(raster2d.min_index(), GridIdx([0, 0]));
        assert_eq!(raster2d.max_index(), GridIdx([2, 1]));

        let exp_bbox = GridBoundingBox2D::new([0, 0], [2, 1]).unwrap();
        assert_eq!(raster2d.bounding_box(), exp_bbox);
    }
}
