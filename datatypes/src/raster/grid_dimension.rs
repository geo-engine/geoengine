use super::Capacity;
use crate::error;
use crate::util::Result;
use serde::{Deserialize, Serialize};
use snafu::ensure;

// Index types for 1,2,3 dimensional grids
pub type Idx = usize;

pub type GridIdx1D = Dim1D;
pub type GridIdx2D = Dim2D;
pub type GridIdx3D = Dim3D;

pub type Dim1D = Dim<[Idx; 1]>;
pub type Dim2D = Dim<[Idx; 2]>;
pub type Dim3D = Dim<[Idx; 3]>;

use std::ops::{Add, Div, Mul, Rem, Sub};
use std::{cmp::min, fmt::Debug};
pub trait GridDimension: Clone + Eq + Debug + Send + Sync + Default + Capacity {
    /// This is the type used as index for the dimension, e.g. a tuple of (y,x) for a 2D dimension.
    // type IndexPattern;
    type IndexType: GridIndex<Self>;
    /// The number of axis of the dimension.
    const NDIM: usize;
    /// The number of axis of the dimension.
    fn number_of_dimensions(&self) -> usize {
        Self::NDIM
    }
    /// The index of the dimension. Note: this is max index +1 on each axis and therefore out of bounds!
    fn size_as_index(&self) -> Self::IndexType;
    /// The number of elements the dimension can hold. This is bounded by size(usize).
    fn number_of_elements(&self) -> usize;
    /// Strides indicate how many linear space elements the next element in the same dimension is away.
    fn strides(&self) -> Self::IndexType;
    /// The size of each axis
    fn slice(&self) -> &[Idx];
    /// Calculate the zero based linear space location of an index in a dimension.
    fn linear_space_index_unchecked(&self, index: &Self::IndexType) -> usize;
    /// Calculate the zero based linear space location of an index in a dimension.
    /// # Errors
    /// This method fails if the grid index is out of bounds.
    fn linear_space_index(&self, index: &Self::IndexType) -> Result<usize> {
        ensure!(
            self.index_inside_dimension(index),
            error::GridIndexOutOfBounds {
                index: Vec::from(index.as_index_array().as_ref()),
                dimension: Vec::from(self.size_as_index().as_index_array().as_ref()),
            }
        );
        Ok(self.linear_space_index_unchecked(index))
    }
    /// Size of the x-axis
    fn size_of_x_axis(&self) -> usize;
    /// Size of the y-axis
    fn size_of_y_axis(&self) -> usize;
    /// Check if a dimension contains a given index
    fn index_inside_dimension(&self, index: &Self::IndexType) -> bool;
    // The intersection of to dimensions
    fn intersection(&self, other: &Self) -> Self;
}

pub trait GridIndex<D>
where
    Self: Sized,
    D: GridDimension<IndexType = Self>,
{
    type IndexArray: Debug + Sized + AsRef<[Idx]>;

    fn as_index_array(&self) -> Self::IndexArray;

    /// Calculate the zero based linear space location of an index in a dimension.
    fn linear_index_in_dim_unchecked(&self, dim: &D) -> usize {
        dim.linear_space_index_unchecked(self)
    }
    /// Check if a dimension contains this index
    fn inside_dimension(&self, dim: &D) -> bool {
        dim.index_inside_dimension(self)
    }
    /// Calculate the zero based linear space location of an index in a dimension.
    /// # Errors
    /// This method fails if the grid index is out of bounds.
    ///
    fn linear_index_in_dim(&self, dim: &D) -> Result<usize> {
        dim.linear_space_index(self)
    }
}

impl GridIndex<Dim1D> for GridIdx1D {
    type IndexArray = [usize; 1];

    fn as_index_array(&self) -> Self::IndexArray {
        self.dim_array
    }
}

impl GridIndex<Dim2D> for GridIdx2D {
    type IndexArray = [usize; 2];

    fn as_index_array(&self) -> Self::IndexArray {
        self.dim_array
    }
}

impl GridIndex<Dim3D> for GridIdx3D {
    type IndexArray = [usize; 3];

    fn as_index_array(&self) -> Self::IndexArray {
        self.dim_array
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, Default, Debug, Serialize, Deserialize)]
pub struct Dim<I> {
    pub dim_array: I,
}

impl<I> Dim<I> {
    pub fn new(dim_array: I) -> Dim<I> {
        Dim { dim_array }
    }

    pub fn dim_array(&self) -> &I {
        &self.dim_array
    }

    pub fn dim_array_mut(&mut self) -> &mut I {
        &mut self.dim_array
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

impl<A, T> AsRef<[T]> for Dim<A>
where
    A: AsRef<[T]>,
{
    fn as_ref(&self) -> &[T] {
        self.dim_array.as_ref()
    }
}

impl GridDimension for Dim1D {
    type IndexType = Self;

    const NDIM: usize = 1;

    fn number_of_elements(&self) -> usize {
        self.dim_array()[0]
    }

    fn size_as_index(&self) -> Self::IndexType {
        self.dim_array.into()
    }

    fn strides(&self) -> Self::IndexType {
        [1].into()
    }

    fn slice(&self) -> &[Idx] {
        &self.dim_array
    }

    fn size_of_x_axis(&self) -> usize {
        self.dim_array[0]
    }

    fn size_of_y_axis(&self) -> usize {
        1
    }

    fn index_inside_dimension(&self, index: &Self::IndexType) -> bool {
        index.as_index_array()[0] < self.dim_array[0]
    }

    fn linear_space_index_unchecked(&self, index: &Self::IndexType) -> usize {
        let strides = self.strides();
        index.as_index_array()[0] * strides.as_index_array()[0]
    }

    fn intersection(&self, other: &Self) -> Self {
        let [self_x] = self.size_as_index().as_index_array();
        let [other_x] = other.size_as_index().as_index_array();
        Dim1D::new([min(self_x, other_x)])
    }
}

impl From<usize> for Dim1D {
    fn from(scalar: usize) -> Self {
        [scalar].into()
    }
}

impl From<[usize; 1]> for Dim1D {
    fn from(dimension_size: [usize; 1]) -> Self {
        Dim1D::new(dimension_size)
    }
}

impl<R> Add<R> for Dim1D
where
    R: Into<Self>,
{
    type Output = Self;

    fn add(self, rhs: R) -> Self::Output {
        [self.dim_array[0] + rhs.into().dim_array[0]].into()
    }
}

impl<R> Sub<R> for Dim1D
where
    R: Into<Self>,
{
    type Output = Self;

    fn sub(self, rhs: R) -> Self::Output {
        [self.dim_array[0] - rhs.into().dim_array[0]].into()
    }
}

impl<R> Mul<R> for Dim1D
where
    R: Into<Self>,
{
    type Output = Self;

    fn mul(self, rhs: R) -> Self::Output {
        [self.dim_array[0] * rhs.into().dim_array[0]].into()
    }
}

impl<R> Div<R> for Dim1D
where
    R: Into<Self>,
{
    type Output = Self;

    fn div(self, rhs: R) -> Self::Output {
        [self.dim_array[0] / rhs.into().dim_array[0]].into()
    }
}

impl<R> Rem<R> for Dim1D
where
    R: Into<Self>,
{
    type Output = Self;

    fn rem(self, rhs: R) -> Self::Output {
        [self.dim_array[0] % rhs.into().dim_array[0]].into()
    }
}

impl GridDimension for Dim2D {
    type IndexType = GridIdx2D;
    const NDIM: usize = 2;

    fn number_of_elements(&self) -> usize {
        self.dim_array()[0] * self.dim_array()[1]
    }

    fn strides(&self) -> Self::IndexType {
        [self.dim_array[1], 1].into()
    }

    fn slice(&self) -> &[Idx] {
        &self.dim_array
    }

    fn size_of_x_axis(&self) -> usize {
        self.dim_array[1]
    }

    fn size_of_y_axis(&self) -> usize {
        self.dim_array[0]
    }

    fn index_inside_dimension(&self, index: &Self::IndexType) -> bool {
        index.as_index_array()[0] < self.dim_array[0]
            && index.as_index_array()[1] < self.dim_array[1]
    }

    fn size_as_index(&self) -> Self::IndexType {
        self.dim_array.into()
    }

    fn linear_space_index_unchecked(&self, index: &Self::IndexType) -> usize {
        let strides = self.strides();
        index.as_index_array()[1] * strides.as_index_array()[1]
            + index.as_index_array()[0] * strides.as_index_array()[0]
    }

    fn intersection(&self, other: &Self) -> Self {
        let [self_y, self_x] = self.size_as_index().as_index_array();
        let [other_y, other_x] = other.size_as_index().as_index_array();
        Dim2D::new([min(self_y, other_y), min(self_x, other_x)])
    }
}

impl From<usize> for Dim2D {
    fn from(scalar: usize) -> Self {
        Dim2D::new([scalar, scalar])
    }
}

impl From<[usize; 2]> for Dim2D {
    fn from(dimension_size: [usize; 2]) -> Self {
        Dim2D::new(dimension_size)
    }
}

impl<R> Add<R> for Dim2D
where
    R: Into<Self>,
{
    type Output = Self;

    fn add(self, rhs: R) -> Self::Output {
        let rhs_dim = rhs.into();
        [
            self.dim_array[0] + rhs_dim.dim_array[0],
            self.dim_array[1] + rhs_dim.dim_array[1],
        ]
        .into()
    }
}

impl<R> Sub<R> for Dim2D
where
    R: Into<Self>,
{
    type Output = Self;

    fn sub(self, rhs: R) -> Self::Output {
        let rhs_dim = rhs.into();
        [
            self.dim_array[0] - rhs_dim.dim_array[0],
            self.dim_array[1] - rhs_dim.dim_array[1],
        ]
        .into()
    }
}

impl<R> Mul<R> for Dim2D
where
    R: Into<Self>,
{
    type Output = Self;

    fn mul(self, rhs: R) -> Self::Output {
        let rhs_dim = rhs.into();
        [
            self.dim_array[0] * rhs_dim.dim_array[0],
            self.dim_array[1] * rhs_dim.dim_array[1],
        ]
        .into()
    }
}

impl<R> Div<R> for Dim2D
where
    R: Into<Self>,
{
    type Output = Self;

    fn div(self, rhs: R) -> Self::Output {
        let rhs_dim = rhs.into();
        [
            self.dim_array[0] / rhs_dim.dim_array[0],
            self.dim_array[1] / rhs_dim.dim_array[1],
        ]
        .into()
    }
}

impl<R> Rem<R> for Dim2D
where
    R: Into<Self>,
{
    type Output = Self;

    fn rem(self, rhs: R) -> Self::Output {
        let rhs_dim = rhs.into();
        [
            self.dim_array[0] % rhs_dim.dim_array[0],
            self.dim_array[1] % rhs_dim.dim_array[1],
        ]
        .into()
    }
}

impl GridDimension for Dim3D {
    type IndexType = GridIdx3D;

    const NDIM: usize = 3;

    fn number_of_elements(&self) -> usize {
        self.dim_array()[2] * self.dim_array()[1] * self.dim_array()[0]
    }

    fn strides(&self) -> Self::IndexType {
        [
            self.dim_array()[1] * self.dim_array()[2],
            self.dim_array()[2],
            1,
        ]
        .into()
    }

    fn slice(&self) -> &[Idx] {
        &self.dim_array
    }

    fn size_of_x_axis(&self) -> usize {
        self.dim_array[2]
    }

    fn size_of_y_axis(&self) -> usize {
        self.dim_array[1]
    }

    fn index_inside_dimension(&self, index: &Self::IndexType) -> bool {
        let index_array = index.as_index_array();
        index_array[0] < self.dim_array[0]
            && index_array[1] < self.dim_array[1]
            && index_array[2] < self.dim_array[2]
    }

    fn size_as_index(&self) -> Self::IndexType {
        self.dim_array.into()
    }

    fn linear_space_index_unchecked(&self, index: &Self::IndexType) -> usize {
        let strides = self.strides().as_index_array();
        let index_array = index.as_index_array();
        index_array[0] * strides[0] + index_array[1] * strides[1] + index_array[2] * strides[2]
    }

    fn intersection(&self, other: &Self) -> Self {
        let [self_z, self_y, self_x] = self.size_as_index().as_index_array();
        let [other_z, other_y, other_x] = other.size_as_index().as_index_array();
        Dim3D::new([
            min(self_z, other_z),
            min(self_y, other_y),
            min(self_x, other_x),
        ])
    }
}

impl From<usize> for Dim3D {
    fn from(scalar: usize) -> Self {
        Dim3D::new([scalar, scalar, scalar])
    }
}

impl From<[usize; 3]> for Dim3D {
    fn from(dimension_size: [usize; 3]) -> Self {
        Dim3D::new(dimension_size)
    }
}

impl<R> Add<R> for Dim3D
where
    R: Into<Self>,
{
    type Output = Self;

    fn add(self, rhs: R) -> Self::Output {
        let rhs_dim = rhs.into();
        [
            self.dim_array[0] + rhs_dim.dim_array[0],
            self.dim_array[1] + rhs_dim.dim_array[1],
            self.dim_array[2] + rhs_dim.dim_array[2],
        ]
        .into()
    }
}

impl<R> Sub<R> for Dim3D
where
    R: Into<Self>,
{
    type Output = Self;

    fn sub(self, rhs: R) -> Self::Output {
        let rhs_dim = rhs.into();
        [
            self.dim_array[0] - rhs_dim.dim_array[0],
            self.dim_array[1] - rhs_dim.dim_array[1],
            self.dim_array[2] - rhs_dim.dim_array[2],
        ]
        .into()
    }
}

impl<R> Mul<R> for Dim3D
where
    R: Into<Self>,
{
    type Output = Self;

    fn mul(self, rhs: R) -> Self::Output {
        let rhs_dim = rhs.into();
        [
            self.dim_array[0] * rhs_dim.dim_array[0],
            self.dim_array[1] * rhs_dim.dim_array[1],
            self.dim_array[2] * rhs_dim.dim_array[2],
        ]
        .into()
    }
}

impl<R> Div<R> for Dim3D
where
    R: Into<Self>,
{
    type Output = Self;

    fn div(self, rhs: R) -> Self::Output {
        let rhs_dim = rhs.into();
        [
            self.dim_array[0] / rhs_dim.dim_array[0],
            self.dim_array[1] / rhs_dim.dim_array[1],
            self.dim_array[2] / rhs_dim.dim_array[2],
        ]
        .into()
    }
}

impl<R> Rem<R> for Dim3D
where
    R: Into<Self>,
{
    type Output = Self;

    fn rem(self, rhs: R) -> Self::Output {
        let rhs_dim = rhs.into();
        [
            self.dim_array[0] % rhs_dim.dim_array[0],
            self.dim_array[1] % rhs_dim.dim_array[1],
            self.dim_array[2] % rhs_dim.dim_array[2],
        ]
        .into()
    }
}

#[cfg(test)]
mod tests {
    use super::{Dim, GridDimension, GridIndex};
    const TEST_1D_DIM_ARR: [usize; 1] = [8];
    #[test]
    fn dim_1d() {
        let dim_1d = Dim::new(TEST_1D_DIM_ARR);
        assert_eq!(dim_1d.dim_array, TEST_1D_DIM_ARR);
        assert_eq!(dim_1d.dim_array(), &TEST_1D_DIM_ARR);
    }
    #[test]
    fn dim_1d_strides() {
        let dim_1d = Dim::new(TEST_1D_DIM_ARR);
        assert_eq!(dim_1d.strides(), Dim::new([1]));
    }
    #[test]
    fn dim_1d_index() {
        let dim_1d = Dim::new(TEST_1D_DIM_ARR);
        assert_eq!(dim_1d.size_as_index().as_index_array()[0], 8);
    }
    #[test]
    fn dim_1d_stride_offset() {
        let dim_1d = Dim::new(TEST_1D_DIM_ARR);
        let dim_1d_index = Dim::new([5]);
        assert_eq!(dim_1d.linear_space_index_unchecked(&dim_1d_index), 5);
    }
    #[test]
    fn dim_1d_ix_index() {
        use super::GridIndex;
        let dim_1d = Dim::new(TEST_1D_DIM_ARR);
        let dim_1d_index = Dim::new([5]);
        assert_eq!(
            dim_1d_index.linear_index_in_dim_unchecked(&dim_1d),
            Dim::new([5]).linear_index_in_dim_unchecked(&dim_1d)
        )
    }

    const TEST_2D_DIM_ARR: [usize; 2] = [8, 3];
    #[test]
    fn dim_2d() {
        let dim_2d = Dim::new(TEST_2D_DIM_ARR);
        assert_eq!(dim_2d.dim_array, TEST_2D_DIM_ARR);
        assert_eq!(dim_2d.dim_array(), &TEST_2D_DIM_ARR);
    }
    #[test]
    fn dim_2d_strides() {
        let dim_2d = Dim::new(TEST_2D_DIM_ARR);
        assert_eq!(dim_2d.strides(), Dim::new([3, 1]));
    }
    #[test]
    fn dim_2d_index() {
        let dim_2d = Dim::new(TEST_2D_DIM_ARR);
        assert_eq!(dim_2d.size_as_index().as_index_array()[0], 8);
        assert_eq!(dim_2d.size_as_index().as_index_array()[1], 3);
    }
    #[test]
    fn dim_2d_stride_offset() {
        let dim_2d = Dim::new(TEST_2D_DIM_ARR);
        let dim_2d_index = Dim::new([2, 2]);
        assert_eq!(dim_2d.linear_space_index_unchecked(&dim_2d_index), 8);
    }
    #[test]
    fn dim_2d_ix_index() {
        use super::GridIndex;
        let dim_2d = Dim::new(TEST_2D_DIM_ARR);
        let dim_2d_index = Dim::new([2, 2]);
        assert_eq!(
            dim_2d_index.linear_index_in_dim_unchecked(&dim_2d),
            Dim::new([2, 2]).linear_index_in_dim_unchecked(&dim_2d)
        )
    }

    const TEST_3D_DIM_ARR: [usize; 3] = [13, 8, 3];
    #[test]
    fn dim_3d() {
        let dim_3d = Dim::new(TEST_3D_DIM_ARR);
        assert_eq!(dim_3d.dim_array, TEST_3D_DIM_ARR);
        assert_eq!(dim_3d.dim_array(), &TEST_3D_DIM_ARR);
    }
    #[test]
    fn dim_3d_strides() {
        let dim_3d = Dim::new(TEST_3D_DIM_ARR);
        assert_eq!(dim_3d.strides(), Dim::new([8 * 3, 3, 1]));
    }
    #[test]
    fn dim_3d_index() {
        let dim_3d = Dim::new(TEST_3D_DIM_ARR);
        assert_eq!(dim_3d.size_as_index().as_index_array()[0], 13);
        assert_eq!(dim_3d.size_as_index().as_index_array()[1], 8);
        assert_eq!(dim_3d.size_as_index().as_index_array()[2], 3);
    }
    #[test]
    #[allow(clippy::identity_op)]
    fn dim_3d_stride_offset() {
        let dim_3d = Dim::new(TEST_3D_DIM_ARR);
        let dim_3d_index = Dim::new([2, 2, 2]);
        assert_eq!(
            dim_3d.linear_space_index_unchecked(&dim_3d_index),
            2 * 8 * 3 + 2 * 3 + 2 * 1
        );
    }
    #[test]
    fn dim_3d_ix_index() {
        use super::GridIndex;
        let dim_3d = Dim::new(TEST_3D_DIM_ARR);
        let dim_3d_index = Dim::new([2, 2, 2]);
        assert_eq!(
            dim_3d_index.linear_index_in_dim_unchecked(&dim_3d),
            Dim::new([2, 2, 2]).linear_index_in_dim_unchecked(&dim_3d)
        )
    }
}
