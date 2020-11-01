use std::{
    fmt::Debug,
    ops::{Add, Div, Mul, Rem, Sub},
};

use crate::error;
use crate::util::Result;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use super::{Dim, Dim1D, Dim2D, Dim3D, GridDimension, GridIdx2D, GridIdx3D, GridIndex};

pub type SignedIdx = isize;

pub type SignedGridIdx1D = Dim<[SignedIdx; 1]>;
pub type SignedGridIdx2D = Dim<[SignedIdx; 2]>;
pub type SignedGridIdx3D = Dim<[SignedIdx; 3]>;

pub type OffsetDim1D = OffsetDim<[usize; 1], [isize; 1]>;
pub type OffsetDim2D = OffsetDim<[usize; 2], [isize; 2]>;
pub type OffsetDim3D = OffsetDim<[usize; 3], [isize; 3]>;

/// This trait allows to address cells in a grid relative to a origin which differs from the upper left pixel.
/// An example for this is a grid wich is anchored at a geographical coordinate (0,0) but the upper left pixel is at (-180, 90).
pub trait OffsetDimension: Sized {
    type SignedIndexType: SignedGridIndex<Self>;
    type DimensionType: GridDimension<IndexType = Self::UnsignedIndexType>;
    type UnsignedIndexType: GridIndex<Self::DimensionType>;

    /// The number of axis of the dimension.
    const NDIM: usize;

    /// The number of axis of the dimension.
    fn number_of_dimensions(&self) -> usize {
        Self::NDIM
    }

    /// The offsets for the dimension axis
    fn offsets_as_slice(&self) -> &[SignedIdx];

    /// This returns the offsets as index which are the min values of each axis. The values are INSIDE the bouds of the axis.
    fn offsets_as_index(&self) -> Self::SignedIndexType;

    /// This returns the max index of each axis. The values are INSIDE the bouds of the axis.
    fn offsets_max_index(&self) -> Self::SignedIndexType;

    // dimension
    fn grid_dimension(&self) -> Self::DimensionType;

    /// converts an offset index to a zero based by shifting the origin of each axis to zero.
    fn offset_index_to_zero_based_unchecked(
        &self,
        index: &Self::SignedIndexType,
    ) -> Self::UnsignedIndexType;

    /// converts an offset index to a zero based by shifting the origin of each axis to zero
    fn offset_index_to_zero_based(
        &self,
        index: &Self::SignedIndexType,
    ) -> Result<Self::UnsignedIndexType> {
        ensure!(
            self.offset_index_inside_dimension(index),
            error::GridSignedIndexOutOfBounds {
                index: Vec::from(index.as_index_array().as_ref()),
                dimension: Vec::from(self.grid_dimension().slice()),
                offsets: Vec::from(self.offsets_as_slice())
            }
        );
        Ok(self.offset_index_to_zero_based_unchecked(index))
    }

    /// converts an offset index directly into a zero based linear space index
    fn offset_index_to_linear_space_index(&self, index: &Self::SignedIndexType) -> Result<usize> {
        let zero_based = self.offset_index_to_zero_based(index)?;
        Ok(self
            .grid_dimension()
            .linear_space_index_unchecked(&zero_based))
    }

    /// converts an offset index directly into a zero based linear space index
    fn offset_index_to_linear_space_index_unchecked(&self, index: &Self::SignedIndexType) -> usize {
        let zero_based = self.offset_index_to_zero_based_unchecked(index);
        self.grid_dimension()
            .linear_space_index_unchecked(&zero_based)
    }

    /// Check if a dimension contains a given index
    fn offset_index_inside_dimension(&self, index: &Self::SignedIndexType) -> bool;

    fn intersection(&self, other: &Self) -> Option<Self>
    where
        Self: Sized;
}
pub trait SignedGridIndex<S>
where
    S: OffsetDimension<SignedIndexType = Self>,
    Self: Sized,
{
    type IndexArray: Debug + Sized + AsRef<[SignedIdx]>;

    fn as_index_array(&self) -> Self::IndexArray;

    /// # Errors
    /// This method fails if the grid index is out of bounds.
    ///
    fn signed_index_to_zero_based(&self, dim: &S) -> Result<S::UnsignedIndexType> {
        dim.offset_index_to_zero_based(self)
    }

    fn signed_index_to_zero_based_unchecked(&self, dim: &S) -> S::UnsignedIndexType {
        dim.offset_index_to_zero_based_unchecked(self)
    }
    fn inside_offset_dimension<D>(&self, dim: &S) -> bool {
        dim.offset_index_inside_dimension(self)
    }

    /// converts an offset index directly into a zero based linear space index
    fn linear_space_index<D>(&self, dim: &S) -> Result<usize> {
        dim.offset_index_to_linear_space_index(self)
    }

    /// converts an offset index directly into a zero based linear space index
    fn linear_space_index_unchecked<D>(&self, dim: &S) -> usize {
        dim.offset_index_to_linear_space_index_unchecked(self)
    }
}

impl From<[isize; 1]> for SignedGridIdx1D {
    fn from(dimension_size: [isize; 1]) -> Self {
        Dim::new(dimension_size)
    }
}

impl From<[isize; 2]> for SignedGridIdx2D {
    fn from(dimension_size: [isize; 2]) -> Self {
        Dim::new(dimension_size)
    }
}

impl From<[isize; 3]> for SignedGridIdx3D {
    fn from(dimension_size: [isize; 3]) -> Self {
        Dim::new(dimension_size)
    }
}

impl<R> Add<R> for SignedGridIdx1D
where
    R: Into<Self>,
{
    type Output = Self;

    fn add(self, rhs: R) -> Self::Output {
        [self.dim_array[0] + rhs.into().dim_array[0]].into()
    }
}

impl<R> Sub<R> for SignedGridIdx1D
where
    R: Into<Self>,
{
    type Output = Self;

    fn sub(self, rhs: R) -> Self::Output {
        [self.dim_array[0] - rhs.into().dim_array[0]].into()
    }
}

impl<R> Mul<R> for SignedGridIdx1D
where
    R: Into<Self>,
{
    type Output = Self;

    fn mul(self, rhs: R) -> Self::Output {
        [self.dim_array[0] * rhs.into().dim_array[0]].into()
    }
}

impl<R> Div<R> for SignedGridIdx1D
where
    R: Into<Self>,
{
    type Output = Self;

    fn div(self, rhs: R) -> Self::Output {
        [self.dim_array[0] / rhs.into().dim_array[0]].into()
    }
}

impl<R> Rem<R> for SignedGridIdx1D
where
    R: Into<Self>,
{
    type Output = Self;

    fn rem(self, rhs: R) -> Self::Output {
        [self.dim_array[0] % rhs.into().dim_array[0]].into()
    }
}

impl<R> Add<R> for SignedGridIdx2D
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

impl<R> Sub<R> for SignedGridIdx2D
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

impl<R> Mul<R> for SignedGridIdx2D
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

impl<R> Div<R> for SignedGridIdx2D
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

impl<R> Rem<R> for SignedGridIdx2D
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

#[derive(Copy, Clone, PartialEq, Eq, Hash, Default, Debug, Serialize, Deserialize)]
pub struct OffsetDim<D, O> {
    dimension: D,
    offsets: O,
}

impl<D, O> OffsetDim<D, O> {
    pub fn new(dimension: D, offsets: O) -> Self {
        OffsetDim { dimension, offsets }
    }

    pub fn new_with_dim<A, B>(dimension: A, offsets: B) -> OffsetDim<D, O>
    where
        A: Into<Dim<D>>,
        B: Into<Dim<O>>,
    {
        OffsetDim {
            dimension: dimension.into().dim_array,
            offsets: offsets.into().dim_array,
        }
    }
}

impl OffsetDimension for OffsetDim1D {
    type SignedIndexType = SignedGridIdx1D;
    type DimensionType = Dim1D;
    type UnsignedIndexType = Dim1D;

    const NDIM: usize = 1;

    fn offsets_as_index(&self) -> Self::SignedIndexType {
        Dim::new(self.offsets)
    }

    fn offset_index_to_zero_based_unchecked(
        &self,
        index: &Self::SignedIndexType,
    ) -> Self::UnsignedIndexType {
        [(index.as_index_array()[0] - self.offsets[0]) as usize].into()
    }

    fn offset_index_inside_dimension(&self, index: &Self::SignedIndexType) -> bool {
        {
            let zero_based = self.offset_index_to_zero_based_unchecked(index);
            self.grid_dimension().index_inside_dimension(&zero_based)
        }
    }

    fn offsets_as_slice(&self) -> &[SignedIdx] {
        &self.offsets
    }

    fn offsets_max_index(&self) -> Self::SignedIndexType {
        let [min_x] = self.offsets_as_index().as_index_array();
        let [size_x] = self.grid_dimension().size_as_index().as_index_array();
        Dim::new([min_x + size_x as isize - 1])
    }

    fn grid_dimension(&self) -> Self::DimensionType {
        Dim::new(self.dimension)
    }

    fn intersection(&self, other: &Self) -> Option<Self> {
        let [self_x_min] = self.offsets_as_index().as_index_array();
        let [other_x_min] = other.offsets_as_index().as_index_array();

        let [self_x_max] = self.offsets_max_index().as_index_array();
        let [other_x_max] = other.offsets_max_index().as_index_array();

        if let Some((start, end)) =
            crate::util::ranges::overlap((self_x_min, self_x_max), (other_x_min, other_x_max))
        {
            let x_size = (end - start) as usize + 1;

            return Some(Self::new([x_size], [start]));
        }
        None
    }
}

impl OffsetDimension for OffsetDim2D {
    type SignedIndexType = SignedGridIdx2D;
    type DimensionType = Dim2D;
    type UnsignedIndexType = GridIdx2D;

    fn offsets_as_index(&self) -> Self::SignedIndexType {
        Dim::new(self.offsets)
    }

    const NDIM: usize = 2;

    fn offset_index_to_zero_based_unchecked(
        &self,
        index: &Self::SignedIndexType,
    ) -> Self::UnsignedIndexType {
        [
            (index.as_index_array()[0] - self.offsets[0]) as usize,
            (index.as_index_array()[1] - self.offsets[1]) as usize,
        ]
        .into()
    }

    fn offset_index_inside_dimension(&self, index: &Self::SignedIndexType) -> bool {
        {
            let zero_based = self.offset_index_to_zero_based_unchecked(index);
            self.grid_dimension().index_inside_dimension(&zero_based)
        }
    }

    fn offsets_as_slice(&self) -> &[SignedIdx] {
        &self.offsets
    }

    fn offsets_max_index(&self) -> Self::SignedIndexType {
        let [min_y, min_x] = self.offsets_as_index().as_index_array();
        let [size_y, size_x] = self.grid_dimension().size_as_index().as_index_array();
        Dim::new([min_y + size_y as isize - 1, min_x + size_x as isize - 1])
    }

    fn grid_dimension(&self) -> Self::DimensionType {
        Dim::new(self.dimension)
    }

    fn intersection(&self, other: &Self) -> Option<Self> {
        let [self_y_min, self_x_min] = self.offsets_as_index().as_index_array();
        let [other_y_min, other_x_min] = other.offsets_as_index().as_index_array();

        let [self_y_max, self_x_max] = self.offsets_max_index().as_index_array();
        let [other_y_max, other_x_max] = other.offsets_max_index().as_index_array();

        let y_overlap =
            crate::util::ranges::overlap((self_y_min, self_y_max), (other_y_min, other_y_max));
        let x_overlap =
            crate::util::ranges::overlap((self_x_min, self_x_max), (other_x_min, other_x_max));

        if let (Some((y_start, y_end)), Some((x_start, x_end))) = (y_overlap, x_overlap) {
            let y_size = (y_end - y_start) as usize + 1;
            let x_size = (x_end - x_start) as usize + 1;
            return Some(OffsetDim2D::new([y_size, x_size], [y_start, x_start]));
        }
        None
    }
}

impl OffsetDimension for OffsetDim3D {
    type SignedIndexType = SignedGridIdx3D;
    type DimensionType = Dim3D;
    type UnsignedIndexType = GridIdx3D;

    const NDIM: usize = 3;

    fn offsets_as_index(&self) -> Self::SignedIndexType {
        Dim::new(self.offsets)
    }

    fn offset_index_to_zero_based_unchecked(
        &self,
        index: &Self::SignedIndexType,
    ) -> Self::UnsignedIndexType {
        Dim::new([
            (index.as_index_array()[0] - self.offsets[0]) as usize,
            (index.as_index_array()[1] - self.offsets[1]) as usize,
            (index.as_index_array()[2] - self.offsets[2]) as usize,
        ])
    }

    fn offset_index_inside_dimension(&self, index: &Self::SignedIndexType) -> bool {
        {
            let zero_based = self.offset_index_to_zero_based_unchecked(index);
            self.grid_dimension().index_inside_dimension(&zero_based)
        }
    }

    fn offsets_as_slice(&self) -> &[SignedIdx] {
        &self.offsets
    }

    fn offsets_max_index(&self) -> Self::SignedIndexType {
        let [min_z, min_y, min_x] = self.offsets_as_index().as_index_array();
        let [size_z, size_y, size_x] = self.grid_dimension().size_as_index().as_index_array();
        Dim::new([
            min_z + size_z as isize - 1,
            min_y + size_y as isize - 1,
            min_x + size_x as isize - 1,
        ])
    }

    fn grid_dimension(&self) -> Self::DimensionType {
        Dim::new(self.dimension)
    }

    fn intersection(&self, other: &Self) -> Option<Self> {
        let [self_z_min, self_y_min, self_x_min] = self.offsets_as_index().as_index_array();
        let [other_z_min, other_y_min, other_x_min] = other.offsets_as_index().as_index_array();

        let [self_z_max, self_y_max, self_x_max] = self.offsets_max_index().as_index_array();
        let [other_z_max, other_y_max, other_x_max] = other.offsets_max_index().as_index_array();

        let z_overlap =
            crate::util::ranges::overlap((self_z_min, self_z_max), (other_z_min, other_z_max));
        let y_overlap =
            crate::util::ranges::overlap((self_y_min, self_y_max), (other_y_min, other_y_max));
        let x_overlap =
            crate::util::ranges::overlap((self_x_min, self_x_max), (other_x_min, other_x_max));

        if let (Some((z_start, z_end)), Some((y_start, y_end)), Some((x_start, x_end))) =
            (z_overlap, y_overlap, x_overlap)
        {
            let z_size = (z_end - z_start) as usize + 1;
            let y_size = (y_end - y_start) as usize + 1;
            let x_size = (x_end - x_start) as usize + 1;

            return Some(OffsetDim3D::new(
                [z_size, y_size, x_size],
                [z_start, y_start, x_start],
            ));
        }
        None
    }
}

impl SignedGridIndex<OffsetDim1D> for SignedGridIdx1D {
    type IndexArray = [SignedIdx; 1];

    fn as_index_array(&self) -> Self::IndexArray {
        self.dim_array
    }
}

impl SignedGridIndex<OffsetDim2D> for SignedGridIdx2D {
    type IndexArray = [SignedIdx; 2];

    fn as_index_array(&self) -> Self::IndexArray {
        self.dim_array
    }
}

impl SignedGridIndex<OffsetDim3D> for SignedGridIdx3D {
    type IndexArray = [SignedIdx; 3];

    fn as_index_array(&self) -> Self::IndexArray {
        self.dim_array
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const TEST_1D_DIM_ARR: [usize; 1] = [8];
    const TEST_2D_DIM_ARR: [usize; 2] = [8, 3];
    const TEST_3D_DIM_ARR: [usize; 3] = [13, 8, 3];

    #[test]
    fn offset_dim_1d() {
        let off_dim_1d = OffsetDim::new(TEST_1D_DIM_ARR, [-5]);
        assert_eq!(off_dim_1d.dimension[0], 8);
        assert_eq!(off_dim_1d.offsets[0], -5);
        assert_eq!(
            off_dim_1d.offset_index_to_zero_based_unchecked(&Dim::new([-5])),
            Dim::new([0])
        );
        assert_eq!(
            off_dim_1d.offset_index_to_zero_based_unchecked(&Dim::new([0])),
            Dim::new([5])
        );
        assert_eq!(
            off_dim_1d.offset_index_to_zero_based_unchecked(&Dim::new([2])),
            Dim::new([7])
        );
        assert_eq!(
            off_dim_1d.offset_index_to_zero_based_unchecked(&Dim::new([3])),
            Dim::new([8])
        );

        assert_eq!(
            off_dim_1d
                .offset_index_to_zero_based(&Dim::new([-5]))
                .unwrap(),
            Dim::new([0])
        );
        assert_eq!(
            off_dim_1d
                .offset_index_to_zero_based(&Dim::new([0]))
                .unwrap(),
            Dim::new([5])
        );
        assert_eq!(
            off_dim_1d
                .offset_index_to_zero_based(&Dim::new([2]))
                .unwrap(),
            Dim::new([7])
        );
        assert!(off_dim_1d
            .offset_index_to_zero_based(&Dim::new([3]))
            .is_err());

        assert_eq!(
            off_dim_1d.offset_index_to_linear_space_index_unchecked(&Dim::new([-5])),
            0
        );
        assert_eq!(
            off_dim_1d.offset_index_to_linear_space_index_unchecked(&Dim::new([0])),
            5
        );

        assert!(off_dim_1d
            .offset_index_to_linear_space_index(&Dim::new([-6]))
            .is_err());
        assert_eq!(
            off_dim_1d
                .offset_index_to_linear_space_index(&Dim::new([-5]))
                .unwrap(),
            0
        );
        assert_eq!(
            off_dim_1d
                .offset_index_to_linear_space_index(&Dim::new([0]))
                .unwrap(),
            5
        );
        assert!(off_dim_1d
            .offset_index_to_linear_space_index(&Dim::new([4]))
            .is_err());
    }

    #[test]
    fn offset_dim_2d() {
        let off_dim_2d = OffsetDim::new(TEST_2D_DIM_ARR, [-5, -5]);
        assert_eq!(off_dim_2d.dimension[0], 8);
        assert_eq!(off_dim_2d.offsets[0], -5);
        assert_eq!(off_dim_2d.offsets[1], -5);
        assert_eq!(
            off_dim_2d.offset_index_to_zero_based_unchecked(&Dim::new([-5, -5])),
            Dim::new([0, 0])
        );
        assert_eq!(
            off_dim_2d.offset_index_to_zero_based_unchecked(&Dim::new([0, 0])),
            Dim::new([5, 5])
        );
        assert_eq!(
            off_dim_2d.offset_index_to_zero_based_unchecked(&Dim::new([2, 2])),
            Dim::new([7, 7])
        );
        assert_eq!(
            off_dim_2d.offset_index_to_zero_based_unchecked(&Dim::new([3, 3])),
            Dim::new([8, 8])
        );

        assert_eq!(
            off_dim_2d
                .offset_index_to_zero_based(&Dim::new([-5, -5]))
                .unwrap(),
            Dim::new([0, 0])
        );
        assert_eq!(
            off_dim_2d
                .offset_index_to_zero_based(&Dim::new([0, -3]))
                .unwrap(),
            Dim::new([5, 2])
        );
        assert_eq!(
            off_dim_2d
                .offset_index_to_zero_based(&Dim::new([2, -3]))
                .unwrap(),
            Dim::new([7, 2])
        );
        assert!(off_dim_2d
            .offset_index_to_zero_based(&Dim::new([3, 3]))
            .is_err());

        assert_eq!(
            off_dim_2d.offset_index_to_linear_space_index_unchecked(&Dim::new([-5, -5])),
            0
        );
        assert_eq!(
            off_dim_2d.offset_index_to_linear_space_index_unchecked(&Dim::new([-5, 0])),
            5
        );

        assert!(off_dim_2d
            .offset_index_to_linear_space_index(&Dim::new([-6, 0]))
            .is_err());
        assert_eq!(
            off_dim_2d
                .offset_index_to_linear_space_index(&Dim::new([-5, -3]))
                .unwrap(),
            2
        );
        assert_eq!(
            off_dim_2d
                .offset_index_to_linear_space_index(&Dim::new([-3, -4]))
                .unwrap(),
            7
        );
        assert!(off_dim_2d
            .offset_index_to_linear_space_index(&Dim::new([4, 4]))
            .is_err());
    }

    #[test]
    fn offset_dim_3d() {
        let off_dim_3d = OffsetDim::new(TEST_3D_DIM_ARR, [-5, -5, -5]);
        assert_eq!(off_dim_3d.dimension[0], 13);
        assert_eq!(off_dim_3d.offsets[0], -5);
        assert_eq!(off_dim_3d.offsets[1], -5);
        assert_eq!(off_dim_3d.offsets[2], -5);
        assert_eq!(
            off_dim_3d.offset_index_to_zero_based_unchecked(&Dim::new([-5, -5, -5])),
            Dim::new([0, 0, 0])
        );
        assert_eq!(
            off_dim_3d.offset_index_to_zero_based_unchecked(&Dim::new([0, 0, 0])),
            Dim::new([5, 5, 5])
        );
        assert_eq!(
            off_dim_3d.offset_index_to_zero_based_unchecked(&Dim::new([2, 2, 2])),
            Dim::new([7, 7, 7])
        );
        assert_eq!(
            off_dim_3d.offset_index_to_zero_based_unchecked(&Dim::new([3, 3, 3])),
            Dim::new([8, 8, 8])
        );

        assert_eq!(
            off_dim_3d
                .offset_index_to_zero_based(&Dim::new([-5, -5, -5]))
                .unwrap(),
            Dim::new([0, 0, 0])
        );
        assert_eq!(
            off_dim_3d
                .offset_index_to_zero_based(&Dim::new([-3, 0, -3]))
                .unwrap(),
            Dim::new([2, 5, 2])
        );
        assert_eq!(
            off_dim_3d
                .offset_index_to_zero_based(&Dim::new([-3, 2, -3]))
                .unwrap(),
            Dim::new([2, 7, 2])
        );
        assert!(off_dim_3d
            .offset_index_to_zero_based(&Dim::new([-6, 3, 3]))
            .is_err());

        assert_eq!(
            off_dim_3d.offset_index_to_linear_space_index_unchecked(&Dim::new([-5, -5, -5])),
            0
        );
        assert_eq!(
            off_dim_3d.offset_index_to_linear_space_index_unchecked(&Dim::new([-5, -4, -3])),
            5
        );

        assert!(off_dim_3d
            .offset_index_to_linear_space_index(&Dim::new([-4, -6, 0]))
            .is_err());
        assert_eq!(
            off_dim_3d
                .offset_index_to_linear_space_index(&Dim::new([-5, -5, -3]))
                .unwrap(),
            2
        );
        assert_eq!(
            off_dim_3d
                .offset_index_to_linear_space_index(&Dim::new([-4, -3, -4]))
                .unwrap(),
            31
        );
        assert!(off_dim_3d
            .offset_index_to_linear_space_index(&Dim::new([4, 4, 4]))
            .is_err());
    }
}
