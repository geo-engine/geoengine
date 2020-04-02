use super::Capacity;
use crate::error;
use crate::util::Result;
use snafu::ensure;

// Index types for 1,2,3 dimensional grids
pub type Ix = usize;
pub type Ix1 = Ix;
pub type Ix2 = (Ix, Ix);
pub type Ix3 = (Ix, Ix, Ix);

use std::fmt::Debug;
use std::ops::{Index, IndexMut};
pub trait GridDimension:
    Clone
    + Eq
    + Debug
    + Send
    + Sync
    + Default
    + IndexMut<usize, Output = usize>
    + Index<usize, Output = usize>
    + Capacity
{
    type IndexPattern;
    const NDIM: usize;
    fn number_of_dimensions(&self) -> usize;
    fn as_pattern(&self) -> Self::IndexPattern;
    fn number_of_elements(&self) -> usize;
    fn strides(&self) -> Self;
    fn slice(&self) -> &[Ix];
    fn stride_offset(index: &Self, strides: &Self) -> usize;
    fn size_of_x_axis(&self) -> usize;
    fn size_of_y_axis(&self) -> usize;
    fn index_inside_dimension(index: &Self, dimension: &Self) -> bool;
}

pub trait GridIndex<D>: Debug {
    fn grid_index_to_1d_index_unchecked(&self, dim: &D) -> usize;

    /// # Errors
    /// This method fails if the grid index is out of bounds.
    ///
    fn grid_index_to_1d_index(&self, dim: &D) -> Result<usize>;
}

impl<D> GridIndex<D> for D
where
    D: GridDimension,
{
    fn grid_index_to_1d_index_unchecked(&self, dim: &D) -> usize {
        D::stride_offset(self, &dim.strides())
    }
    fn grid_index_to_1d_index(&self, dim: &D) -> Result<usize> {
        for (dim_id, (&dim_index, &dim_size)) in
            self.slice().iter().zip(dim.slice().iter()).enumerate()
        {
            ensure!(
                dim_index < dim_size,
                error::GridIndexOutOfBounds {
                    index: dim_index,
                    dimension: dim_id,
                    dimension_size: dim_size
                }
            );
        }

        Ok(D::stride_offset(self, &dim.strides()))
    }
}

// impl GridIndex for usize, (usize, usize) and (usize, usize, usize).
impl GridIndex<Dim<[Ix; 1]>> for Ix1 {
    fn grid_index_to_1d_index_unchecked(&self, dim: &Dim<[Ix; 1]>) -> usize {
        Dim::<[Ix; 1]>::stride_offset(&Dim::from(*self), &dim.strides())
    }
    fn grid_index_to_1d_index(&self, dim: &Dim<[Ix; 1]>) -> Result<usize> {
        Dim::<[Ix; 1]>::from(*self).grid_index_to_1d_index(dim)
    }
}

impl GridIndex<Dim<[Ix; 2]>> for Ix2 {
    fn grid_index_to_1d_index_unchecked(&self, dim: &Dim<[Ix; 2]>) -> usize {
        Dim::<[Ix; 2]>::stride_offset(&Dim::from(*self), &dim.strides())
    }
    fn grid_index_to_1d_index(&self, dim: &Dim<[Ix; 2]>) -> Result<usize> {
        Dim::<[Ix; 2]>::from(*self).grid_index_to_1d_index(dim)
    }
}

impl GridIndex<Dim<[Ix; 3]>> for Ix3 {
    fn grid_index_to_1d_index_unchecked(&self, dim: &Dim<[Ix; 3]>) -> usize {
        Dim::<[Ix; 3]>::stride_offset(&Dim::from(*self), &dim.strides())
    }
    fn grid_index_to_1d_index(&self, dim: &Dim<[Ix; 3]>) -> Result<usize> {
        Dim::<[Ix; 3]>::from(*self).grid_index_to_1d_index(dim)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, Default, Debug)]
pub struct Dim<I> {
    dimension_size: I,
}

impl<I> Dim<I> {
    pub(crate) fn new(dimension_size: I) -> Dim<I> {
        Dim { dimension_size }
    }
    pub(crate) fn dimension_size(&self) -> &I {
        &self.dimension_size
    }
    pub(crate) fn dimension_size_mut(&mut self) -> &mut I {
        &mut self.dimension_size
    }
}

impl<D> Capacity for D
where
    D: GridDimension,
{
    fn capacity(&self) -> usize {
        self.number_of_elements()
    }
}

impl From<Ix> for Dim<[Ix; 1]> {
    fn from(size_1d: Ix1) -> Self {
        Self::new([size_1d])
    }
}

impl From<Ix2> for Dim<[Ix; 2]> {
    fn from(size_2d: Ix2) -> Self {
        Self::new([size_2d.0, size_2d.1])
    }
}

impl From<Ix3> for Dim<[Ix; 3]> {
    fn from(size_3d: Ix3) -> Self {
        Self::new([size_3d.0, size_3d.1, size_3d.2])
    }
}

impl From<[Ix; 2]> for Dim<[Ix; 2]> {
    fn from(size_2d: [Ix; 2]) -> Self {
        Self::new(size_2d)
    }
}

impl From<[Ix; 3]> for Dim<[Ix; 3]> {
    fn from(size_3d: [Ix; 3]) -> Self {
        Self::new(size_3d)
    }
}

impl GridDimension for Dim<[Ix; 1]> {
    type IndexPattern = Ix;
    const NDIM: usize = 1;
    fn number_of_dimensions(&self) -> usize {
        1
    }
    fn number_of_elements(&self) -> usize {
        self.dimension_size()[0]
    }
    fn as_pattern(&self) -> Self::IndexPattern {
        self.dimension_size.len()
    }
    fn strides(&self) -> Self {
        Dim::new([1])
    }
    fn slice(&self) -> &[Ix] {
        &self.dimension_size
    }
    fn stride_offset(index: &Self, strides: &Self) -> usize {
        index[0] * strides[0]
    }
    fn size_of_x_axis(&self) -> usize {
        self.dimension_size[0]
    }
    fn size_of_y_axis(&self) -> usize {
        1
    }
    fn index_inside_dimension(index: &Self, dimension: &Self) -> bool {
        index[0] < dimension[0]
    }
}

impl Index<usize> for Dim<[Ix; 1]> {
    type Output = usize;
    fn index(&self, index: usize) -> &Self::Output {
        &self.dimension_size()[index]
    }
}

impl IndexMut<usize> for Dim<[Ix; 1]> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.dimension_size_mut()[index]
    }
}

impl GridDimension for Dim<[Ix; 2]> {
    type IndexPattern = Ix2;
    const NDIM: usize = 2;
    fn number_of_dimensions(&self) -> usize {
        2
    }
    fn number_of_elements(&self) -> usize {
        self.dimension_size()[0] * self.dimension_size()[1]
    }
    fn as_pattern(&self) -> Self::IndexPattern {
        (self.dimension_size()[1], self.dimension_size()[0])
    }
    fn strides(&self) -> Self {
        Dim::new([self.dimension_size()[1], 1])
    }
    fn slice(&self) -> &[Ix] {
        &self.dimension_size
    }
    fn stride_offset(index: &Self, strides: &Self) -> usize {
        index[1] * strides[1] + index[0] * strides[0]
    }
    fn size_of_x_axis(&self) -> usize {
        self.dimension_size[1]
    }
    fn size_of_y_axis(&self) -> usize {
        self.dimension_size[0]
    }
    fn index_inside_dimension(index: &Self, dimension: &Self) -> bool {
        index[0] < dimension[0] && index[1] < dimension[1]
    }
}

impl Index<usize> for Dim<[Ix; 2]> {
    type Output = usize;
    fn index(&self, index: usize) -> &Self::Output {
        &self.dimension_size()[index]
    }
}

impl IndexMut<usize> for Dim<[Ix; 2]> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.dimension_size_mut()[index]
    }
}

impl GridDimension for Dim<[Ix; 3]> {
    type IndexPattern = Ix3;
    const NDIM: usize = 3;
    fn number_of_dimensions(&self) -> usize {
        3
    }
    fn number_of_elements(&self) -> usize {
        self.dimension_size()[2] * self.dimension_size()[1] * self.dimension_size()[0]
    }
    fn as_pattern(&self) -> Self::IndexPattern {
        (
            self.dimension_size()[0],
            self.dimension_size()[1],
            self.dimension_size()[2],
        )
    }
    fn strides(&self) -> Self {
        Dim::new([
            self.dimension_size()[1] * self.dimension_size()[2],
            self.dimension_size()[2],
            1,
        ])
    }
    fn slice(&self) -> &[Ix] {
        &self.dimension_size
    }
    fn stride_offset(index: &Self, strides: &Self) -> usize {
        index[0] * strides[0] + index[1] * strides[1] + index[2] * strides[2]
    }
    fn size_of_x_axis(&self) -> usize {
        self.dimension_size[2]
    }
    fn size_of_y_axis(&self) -> usize {
        self.dimension_size[1]
    }
    fn index_inside_dimension(index: &Self, dimension: &Self) -> bool {
        index[0] < dimension[0] && index[1] < dimension[1] && index[2] < dimension[2]
    }
}

impl Index<usize> for Dim<[Ix; 3]> {
    type Output = usize;
    fn index(&self, index: usize) -> &Self::Output {
        &self.dimension_size()[index]
    }
}

impl IndexMut<usize> for Dim<[Ix; 3]> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.dimension_size_mut()[index]
    }
}

#[cfg(test)]
mod tests {
    use super::{Dim, GridDimension};
    const TEST_1D_DIM_ARR: [usize; 1] = [8];
    #[test]
    fn dim_1d() {
        let dim_1d = Dim::new(TEST_1D_DIM_ARR);
        assert_eq!(dim_1d.dimension_size, TEST_1D_DIM_ARR);
        assert_eq!(dim_1d.dimension_size(), &TEST_1D_DIM_ARR);
    }
    #[test]
    fn dim_1d_strides() {
        let dim_1d = Dim::new(TEST_1D_DIM_ARR);
        assert_eq!(dim_1d.strides(), Dim::new([1]));
        assert_eq!(dim_1d.strides().slice(), &[1]);
    }
    #[test]
    fn dim_1d_index() {
        let dim_1d = Dim::new(TEST_1D_DIM_ARR);
        assert_eq!(dim_1d[0], 8);
    }
    #[test]
    fn dim_1d_stride_offset() {
        let dim_1d = Dim::new(TEST_1D_DIM_ARR);
        let dim_1d_used_as_index = Dim::new([5]);
        assert_eq!(
            GridDimension::stride_offset(&dim_1d.strides(), &dim_1d_used_as_index),
            5
        );
    }
    #[test]
    fn dim_1d_ix_index() {
        use super::GridIndex;
        let dim_1d = Dim::new(TEST_1D_DIM_ARR);
        let dim_1d_used_as_index = Dim::new([5]);
        assert_eq!(
            dim_1d_used_as_index.grid_index_to_1d_index_unchecked(&dim_1d),
            5.grid_index_to_1d_index_unchecked(&dim_1d)
        )
    }

    const TEST_2D_DIM_ARR: [usize; 2] = [8, 3];
    #[test]
    fn dim_2d() {
        let dim_2d = Dim::new(TEST_2D_DIM_ARR);
        assert_eq!(dim_2d.dimension_size, TEST_2D_DIM_ARR);
        assert_eq!(dim_2d.dimension_size(), &TEST_2D_DIM_ARR);
    }
    #[test]
    fn dim_2d_strides() {
        let dim_2d = Dim::new(TEST_2D_DIM_ARR);
        assert_eq!(dim_2d.strides(), Dim::new([3, 1]));
        assert_eq!(dim_2d.strides().slice(), &[3, 1]);
    }
    #[test]
    fn dim_2d_index() {
        let dim_2d = Dim::new(TEST_2D_DIM_ARR);
        assert_eq!(dim_2d[0], 8);
        assert_eq!(dim_2d[1], 3);
    }
    #[test]
    fn dim_2d_stride_offset() {
        let dim_2d = Dim::new(TEST_2D_DIM_ARR);
        let dim_2d_used_as_index = Dim::new([2, 2]);
        assert_eq!(
            GridDimension::stride_offset(&dim_2d.strides(), &dim_2d_used_as_index),
            8
        );
    }
    #[test]
    fn dim_2d_ix_index() {
        use super::GridIndex;
        let dim_2d = Dim::new(TEST_2D_DIM_ARR);
        let dim_2d_used_as_index = Dim::new([2, 2]);
        assert_eq!(
            dim_2d_used_as_index.grid_index_to_1d_index_unchecked(&dim_2d),
            (2, 2).grid_index_to_1d_index_unchecked(&dim_2d)
        )
    }

    const TEST_3D_DIM_ARR: [usize; 3] = [13, 8, 3];
    #[test]
    fn dim_3d() {
        let dim_3d = Dim::new(TEST_3D_DIM_ARR);
        assert_eq!(dim_3d.dimension_size, TEST_3D_DIM_ARR);
        assert_eq!(dim_3d.dimension_size(), &TEST_3D_DIM_ARR);
    }
    #[test]
    fn dim_3d_strides() {
        let dim_3d = Dim::new(TEST_3D_DIM_ARR);
        assert_eq!(dim_3d.strides(), Dim::new([8 * 3, 3, 1]));
        assert_eq!(dim_3d.strides().slice(), &[8 * 3, 3, 1]);
    }
    #[test]
    fn dim_3d_index() {
        let dim_3d = Dim::new(TEST_3D_DIM_ARR);
        assert_eq!(dim_3d[0], 13);
        assert_eq!(dim_3d[1], 8);
        assert_eq!(dim_3d[2], 3);
    }
    #[test]
    #[allow(clippy::identity_op)]
    fn dim_3d_stride_offset() {
        let dim_3d = Dim::new(TEST_3D_DIM_ARR);
        let dim_3d_used_as_index = Dim::new([2, 2, 2]);
        assert_eq!(
            GridDimension::stride_offset(&dim_3d.strides(), &dim_3d_used_as_index),
            2 * 8 * 3 + 2 * 3 + 2 * 1
        );
    }
    #[test]
    fn dim_3d_ix_index() {
        use super::GridIndex;
        let dim_3d = Dim::new(TEST_3D_DIM_ARR);
        let dim_3d_used_as_index = Dim::new([2, 2, 2]);
        assert_eq!(
            dim_3d_used_as_index.grid_index_to_1d_index_unchecked(&dim_3d),
            (2, 2, 2).grid_index_to_1d_index_unchecked(&dim_3d)
        )
    }
}
