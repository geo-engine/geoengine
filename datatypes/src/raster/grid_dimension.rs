use super::Capacity;
use crate::error;
use crate::util::Result;
use serde::{Deserialize, Serialize};
use snafu::ensure;

// Index types for 1,2,3 dimensional grids
pub type Idx = usize;
pub type SignedIdx = isize;

pub type GridIdx1D = [Idx; 1];
pub type GridIdx2D = [Idx; 2];
pub type GridIdx3D = [Idx; 3];

pub type SignedGridIdx1D = [SignedIdx; 1];
pub type SignedGridIdx2D = [SignedIdx; 2];
pub type SignedGridIdx3D = [SignedIdx; 3];

pub type Dim1D = Dim<[Idx; 1]>;
pub type Dim2D = Dim<[Idx; 2]>;
pub type Dim3D = Dim<[Idx; 3]>;

pub type OffsetDim1D = OffsetDim<Dim<GridIdx1D>, SignedGridIdx1D>;
pub type OffsetDim2D = OffsetDim<Dim<GridIdx2D>, SignedGridIdx2D>;
pub type OffsetDim3D = OffsetDim<Dim<GridIdx3D>, SignedGridIdx3D>;

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
    /// This is the type used as index for the dimension, e.g. a tuple of (y,x) for a 2D dimension.
    // type IndexPattern;
    type IndexType: GridIndex;
    /// The number of axis of the dimension.
    fn number_of_dimensions(&self) -> usize;
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
                index: Vec::from(index.as_ref()),
                dimension: Vec::from(self.size_as_index().as_ref()),
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
}

pub trait GridIndex: Debug + Sized + AsRef<[Idx]> {
    /// Calculate the zero based linear space location of an index in a dimension.
    fn linear_space_index_unchecked<D: GridDimension<IndexType = Self>>(&self, dim: &D) -> usize {
        dim.linear_space_index_unchecked(self)
    }
    /// Check if a dimension contains this index
    fn inside_dimension<D: GridDimension<IndexType = Self>>(&self, dim: &D) -> bool {
        dim.index_inside_dimension(self)
    }
    /// Calculate the zero based linear space location of an index in a dimension.
    /// # Errors
    /// This method fails if the grid index is out of bounds.
    ///
    fn linear_space_index<D: GridDimension<IndexType = Self>>(&self, dim: &D) -> Result<usize> {
        dim.linear_space_index(self)
    }
}

impl GridIndex for GridIdx1D {}

impl GridIndex for GridIdx2D {}

impl GridIndex for GridIdx3D {}

#[derive(Copy, Clone, PartialEq, Eq, Hash, Default, Debug, Serialize, Deserialize)]
pub struct Dim<I> {
    dimension_size: I,
}

impl<I> Dim<I> {
    pub fn new(dimension_size: I) -> Dim<I> {
        Dim { dimension_size }
    }
    #[inline]
    pub fn dimension_size(&self) -> &I {
        &self.dimension_size
    }
    #[inline]
    pub fn dimension_size_mut(&mut self) -> &mut I {
        &mut self.dimension_size
    }
}

impl<D> Capacity for D
where
    D: GridDimension,
{
    #[inline]
    fn capacity(&self) -> usize {
        self.number_of_elements()
    }
}

impl GridDimension for Dim1D {
    type IndexType = GridIdx1D;

    // const NDIM: usize = 1;
    #[inline]
    fn number_of_dimensions(&self) -> usize {
        1
    }

    #[inline]
    fn number_of_elements(&self) -> usize {
        self.dimension_size()[0]
    }

    #[inline]
    fn size_as_index(&self) -> Self::IndexType {
        self.dimension_size
    }

    #[inline]
    fn strides(&self) -> Self::IndexType {
        [1]
    }

    #[inline]
    fn slice(&self) -> &[Idx] {
        &self.dimension_size
    }

    #[inline]
    fn size_of_x_axis(&self) -> usize {
        self.dimension_size[0]
    }

    #[inline]
    fn size_of_y_axis(&self) -> usize {
        1
    }

    #[inline]
    fn index_inside_dimension(&self, index: &Self::IndexType) -> bool {
        index[0] < self.dimension_size[0]
    }

    #[inline]
    fn linear_space_index_unchecked(&self, index: &Self::IndexType) -> usize {
        let strides = self.strides();
        index[0] * strides[0]
    }
}

impl From<[usize; 1]> for Dim1D {
    #[inline]
    fn from(dimension_size: [usize; 1]) -> Self {
        Dim1D::new(dimension_size)
    }
}

impl Index<usize> for Dim1D {
    type Output = usize;
    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.dimension_size()[index]
    }
}

impl IndexMut<usize> for Dim1D {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.dimension_size_mut()[index]
    }
}

impl GridDimension for Dim2D {
    type IndexType = GridIdx2D;
    // const NDIM: usize = 2;
    #[inline]
    fn number_of_dimensions(&self) -> usize {
        2
    }

    #[inline]
    fn number_of_elements(&self) -> usize {
        self.dimension_size()[0] * self.dimension_size()[1]
    }

    #[inline]
    fn strides(&self) -> Self::IndexType {
        [self.dimension_size[1], 1]
    }

    #[inline]
    fn slice(&self) -> &[Idx] {
        &self.dimension_size
    }

    #[inline]
    fn size_of_x_axis(&self) -> usize {
        self.dimension_size[1]
    }

    #[inline]
    fn size_of_y_axis(&self) -> usize {
        self.dimension_size[0]
    }

    #[inline]
    fn index_inside_dimension(&self, index: &Self::IndexType) -> bool {
        index[0] < self.dimension_size[0] && index[1] < self.dimension_size[1]
    }

    #[inline]
    fn size_as_index(&self) -> Self::IndexType {
        self.dimension_size
    }

    #[inline]
    fn linear_space_index_unchecked(&self, index: &Self::IndexType) -> usize {
        let strides = self.strides();
        index[1] * strides[1] + index[0] * strides[0]
    }
}

impl From<[usize; 2]> for Dim2D {
    #[inline]
    fn from(dimension_size: [usize; 2]) -> Self {
        Dim2D::new(dimension_size)
    }
}

impl Index<usize> for Dim2D {
    type Output = usize;
    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.dimension_size()[index]
    }
}

impl IndexMut<usize> for Dim2D {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.dimension_size_mut()[index]
    }
}

impl GridDimension for Dim3D {
    type IndexType = GridIdx3D;

    // const NDIM: usize = 3;
    #[inline]
    fn number_of_dimensions(&self) -> usize {
        3
    }
    #[inline]
    fn number_of_elements(&self) -> usize {
        self.dimension_size()[2] * self.dimension_size()[1] * self.dimension_size()[0]
    }

    #[inline]
    fn strides(&self) -> Self::IndexType {
        [
            self.dimension_size()[1] * self.dimension_size()[2],
            self.dimension_size()[2],
            1,
        ]
    }

    #[inline]
    fn slice(&self) -> &[Idx] {
        &self.dimension_size
    }

    #[inline]
    fn size_of_x_axis(&self) -> usize {
        self.dimension_size[2]
    }

    #[inline]
    fn size_of_y_axis(&self) -> usize {
        self.dimension_size[1]
    }

    #[inline]
    fn index_inside_dimension(&self, index: &Self::IndexType) -> bool {
        index[0] < self.dimension_size[0]
            && index[1] < self.dimension_size[1]
            && index[2] < self.dimension_size[2]
    }

    #[inline]
    fn size_as_index(&self) -> Self::IndexType {
        self.dimension_size
    }

    #[inline]
    fn linear_space_index_unchecked(&self, index: &Self::IndexType) -> usize {
        let strides = self.strides();
        index[0] * strides[0] + index[1] * strides[1] + index[2] * strides[2]
    }
}

impl From<[usize; 3]> for Dim3D {
    #[inline]
    fn from(dimension_size: [usize; 3]) -> Self {
        Dim3D::new(dimension_size)
    }
}

impl Index<usize> for Dim3D {
    type Output = usize;
    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.dimension_size()[index]
    }
}

impl IndexMut<usize> for Dim3D {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.dimension_size_mut()[index]
    }
}

pub trait SignedGridIndex: Debug + Sized + AsRef<[SignedIdx]> {
    type UnsignedIndex: GridIndex;

    /// # Errors
    /// This method fails if the grid index is out of bounds.
    ///
    fn signed_index_to_zero_based<D: OffsetDimension<SignedIndexType = Self>>(
        &self,
        dim: &D,
    ) -> Result<D::IndexType> {
        dim.offset_index_to_zero_based(self)
    }

    fn signed_index_to_zero_based_unchecked<D: OffsetDimension<SignedIndexType = Self>>(
        &self,
        dim: &D,
    ) -> D::IndexType {
        dim.offset_index_to_zero_based_unchecked(self)
    }
    fn inside_offset_dimension<D: OffsetDimension<SignedIndexType = Self>>(&self, dim: &D) -> bool {
        dim.offset_index_inside_dimension(self)
    }

    /// converts an offset index directly into a zero based linear space index
    fn linear_space_index<D: OffsetDimension<SignedIndexType = Self>>(
        &self,
        dim: &D,
    ) -> Result<usize> {
        dim.offset_index_to_linear_space_index(self)
    }

    /// converts an offset index directly into a zero based linear space index
    fn linear_space_index_unchecked<D: OffsetDimension<SignedIndexType = Self>>(
        &self,
        dim: &D,
    ) -> usize {
        dim.offset_index_to_linear_space_index_unchecked(self)
    }
}

pub trait OffsetDimension: GridDimension {
    type SignedIndexType: SignedGridIndex;

    /// The offsets for the dimension axis
    fn offsets_as_slice(&self) -> &[SignedIdx];

    /// This returns the offsets as index which are the min values of each axis. The values are INSIDE the bouds of the axis.
    fn offsets_as_index(&self) -> Self::SignedIndexType;

    /// This returns the max index of each axis. The values are INSIDE the bouds of the axis.
    fn offsets_max_index(&self) -> Self::SignedIndexType;

    /// converts an offset index to a zero based by shifting the origin of each axis to zero.
    fn offset_index_to_zero_based_unchecked(
        &self,
        index: &Self::SignedIndexType,
    ) -> Self::IndexType;

    /// converts an offset index to a zero based by shifting the origin of each axis to zero
    fn offset_index_to_zero_based(&self, index: &Self::SignedIndexType) -> Result<Self::IndexType> {
        ensure!(
            self.offset_index_inside_dimension(index),
            error::GridSignedIndexOutOfBounds {
                index: Vec::from(index.as_ref()),
                dimension: Vec::from(self.slice()),
                offsets: Vec::from(self.offsets_as_slice())
            }
        );
        Ok(self.offset_index_to_zero_based_unchecked(index))
    }

    /// converts an offset index directly into a zero based linear space index
    fn offset_index_to_linear_space_index(&self, index: &Self::SignedIndexType) -> Result<usize> {
        let zero_based = self.offset_index_to_zero_based(index)?;
        Ok(self.linear_space_index_unchecked(&zero_based))
    }

    /// converts an offset index directly into a zero based linear space index
    fn offset_index_to_linear_space_index_unchecked(&self, index: &Self::SignedIndexType) -> usize {
        let zero_based = self.offset_index_to_zero_based_unchecked(index);
        self.linear_space_index_unchecked(&zero_based)
    }

    /// Check if a dimension contains a given index
    fn offset_index_inside_dimension(&self, index: &Self::SignedIndexType) -> bool;
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, Default, Debug, Serialize, Deserialize)]
pub struct OffsetDim<D, O> {
    dimension: D,
    offsets: O,
}

impl<D: GridDimension, O> OffsetDim<D, O> {
    pub fn new(dimension: D, offsets: O) -> OffsetDim<D, O> {
        OffsetDim { dimension, offsets }
    }
}

impl<D, O> GridDimension for OffsetDim<D, O>
where
    D: GridDimension,
    O: Sync + Debug + Send + std::default::Default + Eq + Clone,
{
    type IndexType = D::IndexType;

    #[inline]
    fn number_of_dimensions(&self) -> usize {
        self.dimension.number_of_dimensions()
    }

    #[inline]
    fn size_as_index(&self) -> Self::IndexType {
        self.dimension.size_as_index()
    }

    #[inline]
    fn number_of_elements(&self) -> usize {
        self.dimension.number_of_elements()
    }

    #[inline]
    fn strides(&self) -> Self::IndexType {
        self.dimension.strides()
    }

    #[inline]
    fn slice(&self) -> &[Idx] {
        self.dimension.slice()
    }

    #[inline]
    fn linear_space_index_unchecked(&self, index: &D::IndexType) -> usize {
        self.dimension.linear_space_index_unchecked(index)
    }

    #[inline]
    fn size_of_x_axis(&self) -> usize {
        self.dimension.size_of_x_axis()
    }

    #[inline]
    fn size_of_y_axis(&self) -> usize {
        self.dimension.size_of_y_axis()
    }

    #[inline]
    fn index_inside_dimension(&self, index: &D::IndexType) -> bool {
        self.dimension.index_inside_dimension(index)
    }
}

impl<D, O> Index<usize> for OffsetDim<D, O>
where
    D: GridDimension,
{
    type Output = usize;
    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.dimension[index]
    }
}

impl<D, O> IndexMut<usize> for OffsetDim<D, O>
where
    D: GridDimension,
{
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.dimension[index]
    }
}

impl OffsetDimension for OffsetDim<Dim<[usize; 1]>, [isize; 1]> {
    type SignedIndexType = SignedGridIdx1D;

    #[inline]
    fn offsets_as_index(&self) -> Self::SignedIndexType {
        self.offsets
    }

    #[inline]
    fn offset_index_to_zero_based_unchecked(
        &self,
        index: &Self::SignedIndexType,
    ) -> Self::IndexType {
        [(index[0] - self.offsets[0]) as usize]
    }

    #[inline]
    fn offset_index_inside_dimension(&self, index: &Self::SignedIndexType) -> bool {
        {
            let zero_based = self.offset_index_to_zero_based_unchecked(index);
            self.index_inside_dimension(&zero_based)
        }
    }

    fn offsets_as_slice(&self) -> &[SignedIdx] {
        &self.offsets
    }

    fn offsets_max_index(&self) -> Self::SignedIndexType {
        let [min_x] = self.offsets_as_index();
        let [size_x] = self.size_as_index();
        [min_x + size_x as isize - 1]
    }
}

impl OffsetDimension for OffsetDim<Dim<[usize; 2]>, [isize; 2]> {
    type SignedIndexType = SignedGridIdx2D;

    #[inline]
    fn offsets_as_index(&self) -> Self::SignedIndexType {
        self.offsets
    }

    #[inline]
    fn offset_index_to_zero_based_unchecked(
        &self,
        index: &Self::SignedIndexType,
    ) -> Self::IndexType {
        [
            (index[0] - self.offsets[0]) as usize,
            (index[1] - self.offsets[1]) as usize,
        ]
    }

    #[inline]
    fn offset_index_inside_dimension(&self, index: &Self::SignedIndexType) -> bool {
        {
            let zero_based = self.offset_index_to_zero_based_unchecked(index);
            self.index_inside_dimension(&zero_based)
        }
    }

    fn offsets_as_slice(&self) -> &[SignedIdx] {
        &self.offsets
    }

    fn offsets_max_index(&self) -> Self::SignedIndexType {
        let [min_y, min_x] = self.offsets_as_index();
        let [size_y, size_x] = self.size_as_index();
        [min_y + size_y as isize - 1, min_x + size_x as isize - 1]
    }
}

impl OffsetDimension for OffsetDim<Dim<[usize; 3]>, [isize; 3]> {
    type SignedIndexType = SignedGridIdx3D;

    #[inline]
    fn offsets_as_index(&self) -> Self::SignedIndexType {
        self.offsets
    }

    #[inline]
    fn offset_index_to_zero_based_unchecked(
        &self,
        index: &Self::SignedIndexType,
    ) -> Self::IndexType {
        [
            (index[0] - self.offsets[0]) as usize,
            (index[1] - self.offsets[1]) as usize,
            (index[2] - self.offsets[2]) as usize,
        ]
    }

    #[inline]
    fn offset_index_inside_dimension(&self, index: &Self::SignedIndexType) -> bool {
        {
            let zero_based = self.offset_index_to_zero_based_unchecked(index);
            self.index_inside_dimension(&zero_based)
        }
    }

    fn offsets_as_slice(&self) -> &[SignedIdx] {
        &self.offsets
    }

    fn offsets_max_index(&self) -> Self::SignedIndexType {
        let [min_z, min_y, min_x] = self.offsets_as_index();
        let [size_z, size_y, size_x] = self.size_as_index();
        [
            min_z + size_z as isize - 1,
            min_y + size_y as isize - 1,
            min_x + size_x as isize - 1,
        ]
    }
}

impl SignedGridIndex for SignedGridIdx1D {
    type UnsignedIndex = GridIdx1D;
}

impl SignedGridIndex for SignedGridIdx2D {
    type UnsignedIndex = GridIdx2D;
}

impl SignedGridIndex for SignedGridIdx3D {
    type UnsignedIndex = GridIdx3D;
}

#[cfg(test)]
mod tests {
    use super::{Dim, GridDimension, OffsetDim, OffsetDimension};
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
        assert_eq!(dim_1d.strides(), [1]);
    }
    #[test]
    fn dim_1d_index() {
        let dim_1d = Dim::new(TEST_1D_DIM_ARR);
        assert_eq!(dim_1d[0], 8);
    }
    #[test]
    fn dim_1d_stride_offset() {
        let dim_1d = Dim::new(TEST_1D_DIM_ARR);
        let dim_1d_index = [5];
        assert_eq!(dim_1d.linear_space_index_unchecked(&dim_1d_index), 5);
    }
    #[test]
    fn dim_1d_ix_index() {
        use super::GridIndex;
        let dim_1d = Dim::new(TEST_1D_DIM_ARR);
        let dim_1d_index = [5];
        assert_eq!(
            dim_1d_index.linear_space_index_unchecked(&dim_1d),
            [5].linear_space_index_unchecked(&dim_1d)
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
        assert_eq!(dim_2d.strides(), [3, 1]);
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
        let dim_2d_index = [2, 2];
        assert_eq!(dim_2d.linear_space_index_unchecked(&dim_2d_index), 8);
    }
    #[test]
    fn dim_2d_ix_index() {
        use super::GridIndex;
        let dim_2d = Dim::new(TEST_2D_DIM_ARR);
        let dim_2d_index = [2, 2];
        assert_eq!(
            dim_2d_index.linear_space_index_unchecked(&dim_2d),
            [2, 2].linear_space_index_unchecked(&dim_2d)
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
        assert_eq!(dim_3d.strides(), [8 * 3, 3, 1]);
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
        let dim_3d_index = [2, 2, 2];
        assert_eq!(
            dim_3d.linear_space_index_unchecked(&dim_3d_index),
            2 * 8 * 3 + 2 * 3 + 2 * 1
        );
    }
    #[test]
    fn dim_3d_ix_index() {
        use super::GridIndex;
        let dim_3d = Dim::new(TEST_3D_DIM_ARR);
        let dim_3d_index = [2, 2, 2];
        assert_eq!(
            dim_3d_index.linear_space_index_unchecked(&dim_3d),
            [2, 2, 2].linear_space_index_unchecked(&dim_3d)
        )
    }

    #[test]
    fn offset_dim_1d() {
        let dim_1d = Dim::new(TEST_1D_DIM_ARR);
        let off_dim_1d = OffsetDim::new(dim_1d, [-5]);
        assert_eq!(off_dim_1d[0], 8);
        assert_eq!(off_dim_1d.offsets[0], -5);
        assert_eq!(off_dim_1d.offset_index_to_zero_based_unchecked(&[-5]), [0]);
        assert_eq!(off_dim_1d.offset_index_to_zero_based_unchecked(&[0]), [5]);
        assert_eq!(off_dim_1d.offset_index_to_zero_based_unchecked(&[2]), [7]);
        assert_eq!(off_dim_1d.offset_index_to_zero_based_unchecked(&[3]), [8]);

        assert_eq!(off_dim_1d.offset_index_to_zero_based(&[-5]).unwrap(), [0]);
        assert_eq!(off_dim_1d.offset_index_to_zero_based(&[0]).unwrap(), [5]);
        assert_eq!(off_dim_1d.offset_index_to_zero_based(&[2]).unwrap(), [7]);
        assert!(off_dim_1d.offset_index_to_zero_based(&[3]).is_err());

        assert_eq!(
            off_dim_1d.offset_index_to_linear_space_index_unchecked(&[-5]),
            0
        );
        assert_eq!(
            off_dim_1d.offset_index_to_linear_space_index_unchecked(&[0]),
            5
        );

        assert!(off_dim_1d
            .offset_index_to_linear_space_index(&[-6])
            .is_err());
        assert_eq!(
            off_dim_1d
                .offset_index_to_linear_space_index(&[-5])
                .unwrap(),
            0
        );
        assert_eq!(
            off_dim_1d.offset_index_to_linear_space_index(&[0]).unwrap(),
            5
        );
        assert!(off_dim_1d.offset_index_to_linear_space_index(&[4]).is_err());
    }

    #[test]
    fn offset_dim_2d() {
        let dim_2d = Dim::new(TEST_2D_DIM_ARR);
        let off_dim_2d = OffsetDim::new(dim_2d, [-5, -5]);
        assert_eq!(off_dim_2d[0], 8);
        assert_eq!(off_dim_2d.offsets[0], -5);
        assert_eq!(off_dim_2d.offsets[1], -5);
        assert_eq!(
            off_dim_2d.offset_index_to_zero_based_unchecked(&[-5, -5]),
            [0, 0]
        );
        assert_eq!(
            off_dim_2d.offset_index_to_zero_based_unchecked(&[0, 0]),
            [5, 5]
        );
        assert_eq!(
            off_dim_2d.offset_index_to_zero_based_unchecked(&[2, 2]),
            [7, 7]
        );
        assert_eq!(
            off_dim_2d.offset_index_to_zero_based_unchecked(&[3, 3]),
            [8, 8]
        );

        assert_eq!(
            off_dim_2d.offset_index_to_zero_based(&[-5, -5]).unwrap(),
            [0, 0]
        );
        assert_eq!(
            off_dim_2d.offset_index_to_zero_based(&[0, -3]).unwrap(),
            [5, 2]
        );
        assert_eq!(
            off_dim_2d.offset_index_to_zero_based(&[2, -3]).unwrap(),
            [7, 2]
        );
        assert!(off_dim_2d.offset_index_to_zero_based(&[3, 3]).is_err());

        assert_eq!(
            off_dim_2d.offset_index_to_linear_space_index_unchecked(&[-5, -5]),
            0
        );
        assert_eq!(
            off_dim_2d.offset_index_to_linear_space_index_unchecked(&[-5, 0]),
            5
        );

        assert!(off_dim_2d
            .offset_index_to_linear_space_index(&[-6, 0])
            .is_err());
        assert_eq!(
            off_dim_2d
                .offset_index_to_linear_space_index(&[-5, -3])
                .unwrap(),
            2
        );
        assert_eq!(
            off_dim_2d
                .offset_index_to_linear_space_index(&[-3, -4])
                .unwrap(),
            7
        );
        assert!(off_dim_2d
            .offset_index_to_linear_space_index(&[4, 4])
            .is_err());
    }

    #[test]
    fn offset_dim_3d() {
        let dim_3d = Dim::new(TEST_3D_DIM_ARR);
        let off_dim_3d = OffsetDim::new(dim_3d, [-5, -5, -5]);
        assert_eq!(off_dim_3d[0], 13);
        assert_eq!(off_dim_3d.offsets[0], -5);
        assert_eq!(off_dim_3d.offsets[1], -5);
        assert_eq!(off_dim_3d.offsets[2], -5);
        assert_eq!(
            off_dim_3d.offset_index_to_zero_based_unchecked(&[-5, -5, -5]),
            [0, 0, 0]
        );
        assert_eq!(
            off_dim_3d.offset_index_to_zero_based_unchecked(&[0, 0, 0]),
            [5, 5, 5]
        );
        assert_eq!(
            off_dim_3d.offset_index_to_zero_based_unchecked(&[2, 2, 2]),
            [7, 7, 7]
        );
        assert_eq!(
            off_dim_3d.offset_index_to_zero_based_unchecked(&[3, 3, 3]),
            [8, 8, 8]
        );

        assert_eq!(
            off_dim_3d
                .offset_index_to_zero_based(&[-5, -5, -5])
                .unwrap(),
            [0, 0, 0]
        );
        assert_eq!(
            off_dim_3d.offset_index_to_zero_based(&[-3, 0, -3]).unwrap(),
            [2, 5, 2]
        );
        assert_eq!(
            off_dim_3d.offset_index_to_zero_based(&[-3, 2, -3]).unwrap(),
            [2, 7, 2]
        );
        assert!(off_dim_3d.offset_index_to_zero_based(&[-6, 3, 3]).is_err());

        assert_eq!(
            off_dim_3d.offset_index_to_linear_space_index_unchecked(&[-5, -5, -5]),
            0
        );
        assert_eq!(
            off_dim_3d.offset_index_to_linear_space_index_unchecked(&[-5, -4, -3]),
            5
        );

        assert!(off_dim_3d
            .offset_index_to_linear_space_index(&[-4, -6, 0])
            .is_err());
        assert_eq!(
            off_dim_3d
                .offset_index_to_linear_space_index(&[-5, -5, -3])
                .unwrap(),
            2
        );
        assert_eq!(
            off_dim_3d
                .offset_index_to_linear_space_index(&[-4, -3, -4])
                .unwrap(),
            31
        );
        assert!(off_dim_3d
            .offset_index_to_linear_space_index(&[4, 4, 4])
            .is_err());
    }
}
