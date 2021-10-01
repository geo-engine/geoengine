use snafu::ensure;

use crate::{error, util::Result};

use super::{
    BoundedGrid, GridBounds, GridContains, GridIdx, GridIntersection, GridSize,
    GridSpaceToLinearSpace,
};

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct GridBoundingBox<A>
where
    A: AsRef<[isize]>,
{
    min: A,
    max: A,
}

pub type GridBoundingBox1D = GridBoundingBox<[isize; 1]>;
pub type GridBoundingBox2D = GridBoundingBox<[isize; 2]>;
pub type GridBoundingBox3D = GridBoundingBox<[isize; 3]>;

impl<A> GridBoundingBox<A>
where
    A: AsRef<[isize]>,
{
    pub fn new<I: Into<GridIdx<A>>>(min: I, max: I) -> Result<Self> {
        let idx_min: GridIdx<A> = min.into();
        let idx_max: GridIdx<A> = max.into();
        for (&a, &b) in idx_min.as_slice().iter().zip(idx_max.as_slice()) {
            ensure!(
                a <= b,
                error::InvalidGridBounds {
                    min: Vec::from(idx_min.as_slice()),
                    max: Vec::from(idx_max.as_slice())
                }
            );
        }
        Ok(GridBoundingBox::new_unchecked(idx_min, idx_max))
    }

    pub fn new_unchecked<I: Into<GridIdx<A>>>(min: I, max: I) -> Self {
        let GridIdx(idx_min) = min.into();
        let GridIdx(idx_max) = max.into();
        GridBoundingBox {
            min: idx_min,
            max: idx_max,
        }
    }
}

impl GridSize for GridBoundingBox<[isize; 1]> {
    type ShapeArray = [usize; 1];

    const NDIM: usize = 1;

    fn axis_size(&self) -> Self::ShapeArray {
        let GridIdx([a]) = GridIdx::from(self.max) - GridIdx::from(self.min) + 1;
        [a as usize]
    }

    fn number_of_elements(&self) -> usize {
        let [a] = self.axis_size();
        a
    }
}

impl GridSize for GridBoundingBox<[isize; 2]> {
    type ShapeArray = [usize; 2];

    const NDIM: usize = 2;

    fn axis_size(&self) -> Self::ShapeArray {
        let GridIdx([a, b]) = GridIdx::from(self.max) - GridIdx::from(self.min) + 1;
        [a as usize, b as usize]
    }

    fn number_of_elements(&self) -> usize {
        let [a, b] = self.axis_size();
        a * b
    }
}

impl GridSize for GridBoundingBox<[isize; 3]> {
    type ShapeArray = [usize; 3];

    const NDIM: usize = 3;

    fn axis_size(&self) -> Self::ShapeArray {
        let GridIdx([a, b, c]) = GridIdx::from(self.max) - GridIdx::from(self.min) + 1;
        [a as usize, b as usize, c as usize]
    }

    fn number_of_elements(&self) -> usize {
        let [a, b, c] = self.axis_size();
        a * b * c
    }
}

impl<A> GridBounds for GridBoundingBox<A>
where
    A: AsRef<[isize]> + Into<GridIdx<A>> + Clone,
{
    type IndexArray = A;

    fn min_index(&self) -> GridIdx<A> {
        self.min.clone().into()
    }

    fn max_index(&self) -> GridIdx<A> {
        self.max.clone().into()
    }
}

impl GridIntersection for GridBoundingBox1D {
    fn intersection(&self, other: &Self) -> Option<Self> {
        let [self_x_min] = self.min;
        let [other_x_min] = other.min;

        let [self_x_max] = self.max;
        let [other_x_max] = other.max;

        crate::util::ranges::overlap_inclusive((self_x_min, self_x_max), (other_x_min, other_x_max))
            .map(|(min, max)| GridBoundingBox::new_unchecked([min], [max]))
    }
}

impl GridIntersection for GridBoundingBox2D {
    fn intersection(&self, other: &Self) -> Option<Self> {
        let [self_y_min, self_x_min] = self.min;
        let [other_y_min, other_x_min] = other.min;

        let [self_y_max, self_x_max] = self.max;
        let [other_y_max, other_x_max] = other.max;

        let y_overlap = crate::util::ranges::overlap_inclusive(
            (self_y_min, self_y_max),
            (other_y_min, other_y_max),
        );
        let x_overlap = crate::util::ranges::overlap_inclusive(
            (self_x_min, self_x_max),
            (other_x_min, other_x_max),
        );

        if let (Some((y_min, y_max)), Some((x_min, x_max))) = (y_overlap, x_overlap) {
            return Some(GridBoundingBox2D::new_unchecked(
                [y_min, x_min],
                [y_max, x_max],
            ));
        }
        None
    }
}

impl GridIntersection for GridBoundingBox3D {
    fn intersection(&self, other: &Self) -> Option<Self> {
        let [self_z_min, self_y_min, self_x_min] = self.min;
        let [other_z_min, other_y_min, other_x_min] = other.min;

        let [self_z_max, self_y_max, self_x_max] = self.max;
        let [other_z_max, other_y_max, other_x_max] = other.max;

        let z_overlap = crate::util::ranges::overlap_inclusive(
            (self_z_min, self_z_max),
            (other_z_min, other_z_max),
        );
        let y_overlap = crate::util::ranges::overlap_inclusive(
            (self_y_min, self_y_max),
            (other_y_min, other_y_max),
        );
        let x_overlap = crate::util::ranges::overlap_inclusive(
            (self_x_min, self_x_max),
            (other_x_min, other_x_max),
        );

        if let (Some((z_min, z_max)), Some((y_min, y_max)), Some((x_min, x_max))) =
            (z_overlap, y_overlap, x_overlap)
        {
            return Some(GridBoundingBox3D::new_unchecked(
                [z_min, y_min, x_min],
                [z_max, y_max, x_max],
            ));
        }
        None
    }
}

impl GridSpaceToLinearSpace for GridBoundingBox<[isize; 1]> {
    type IndexArray = [isize; 1];

    fn strides(&self) -> Self::ShapeArray {
        [1]
    }

    fn linear_space_index_unchecked<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> usize {
        let idx = index.into();
        let GridIdx([zero_idx_x]) = idx - GridIdx::from(self.min);
        let [stride_x] = self.strides();
        zero_idx_x as usize * stride_x
    }

    fn linear_space_index<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> Result<usize> {
        let real_index = index.into();
        ensure!(
            self.contains(&real_index),
            error::GridIndexOutOfBounds {
                index: Vec::from(real_index.0),
                min_index: Vec::from(self.min_index().0),
                max_index: Vec::from(self.max_index().0)
            }
        );
        Ok(self.linear_space_index_unchecked(real_index))
    }

    fn grid_idx_unchecked(&self, linear_idx: usize) -> GridIdx<[isize; 1]> {
        let zero_idx = GridIdx([(linear_idx) as isize]);
        zero_idx + GridIdx::from(self.min)
    }
}

impl GridSpaceToLinearSpace for GridBoundingBox<[isize; 2]> {
    type IndexArray = [isize; 2];

    fn strides(&self) -> Self::ShapeArray {
        [self.axis_size_x(), 1]
    }

    fn linear_space_index_unchecked<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> usize {
        let idx = index.into();
        let GridIdx([zero_idx_y, zero_idx_x]) = idx - GridIdx::from(self.min);
        let [stride_y, stride_x] = self.strides();
        zero_idx_y as usize * stride_y + zero_idx_x as usize * stride_x
    }

    fn grid_idx_unchecked(&self, linear_idx: usize) -> GridIdx<[isize; 2]> {
        let [stride_y, _stride_x] = self.strides();
        let zero_idx = GridIdx([
            (linear_idx / stride_y) as isize,
            (linear_idx % stride_y) as isize,
        ]);
        zero_idx + GridIdx::from(self.min)
    }

    fn linear_space_index<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> Result<usize> {
        let real_index = index.into();
        ensure!(
            self.contains(&real_index),
            error::GridIndexOutOfBounds {
                index: Vec::from(real_index.0),
                min_index: Vec::from(self.min_index().0),
                max_index: Vec::from(self.max_index().0)
            }
        );
        Ok(self.linear_space_index_unchecked(real_index))
    }
}

impl GridSpaceToLinearSpace for GridBoundingBox<[isize; 3]> {
    type IndexArray = [isize; 3];

    fn strides(&self) -> Self::ShapeArray {
        [
            self.axis_size_y() * self.axis_size_x(),
            self.axis_size_x(),
            1,
        ]
    }

    fn linear_space_index_unchecked<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> usize {
        let idx = index.into();
        let GridIdx([zero_idx_z, zero_idx_y, zero_idx_x]) = idx - GridIdx::from(self.min);
        let [stride_z, stride_y, stride_x] = self.strides();
        zero_idx_z as usize * stride_z
            + zero_idx_y as usize * stride_y
            + zero_idx_x as usize * stride_x
    }

    fn linear_space_index<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> Result<usize> {
        let real_index = index.into();
        ensure!(
            self.contains(&real_index),
            error::GridIndexOutOfBounds {
                index: Vec::from(real_index.0),
                min_index: Vec::from(self.min_index().0),
                max_index: Vec::from(self.max_index().0)
            }
        );
        Ok(self.linear_space_index_unchecked(real_index))
    }

    fn grid_idx_unchecked(&self, linear_idx: usize) -> GridIdx<[isize; 3]> {
        let [stride_z, stride_y, _stride_x] = self.strides();
        let zero_idx = GridIdx([
            (linear_idx / stride_z) as isize,
            ((linear_idx % stride_z) / stride_y) as isize,
            (linear_idx % stride_y) as isize,
        ]);
        zero_idx + GridIdx::from(self.min)
    }
}

impl<G> BoundedGrid for G
where
    G: GridBounds,
{
    type IndexArray = G::IndexArray;

    fn bounding_box(&self) -> GridBoundingBox<Self::IndexArray> {
        GridBoundingBox::new_unchecked(self.min_index(), self.max_index())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grid_bounding_box_new_1d() {
        GridBoundingBox::new([1], [42]).unwrap();
        GridBoundingBox::new([42], [1]).unwrap_err();
    }

    #[test]
    fn grid_bounding_box_new_2d() {
        GridBoundingBox::new([1, 2], [42, 43]).unwrap();
        GridBoundingBox::new([1, 43], [2, 42]).unwrap_err();
    }

    #[test]
    fn grid_bounding_box_new_3d() {
        GridBoundingBox::new([1, 2, 3], [42, 43, 44]).unwrap();
        GridBoundingBox::new([1, 2, 44], [42, 43, 3]).unwrap_err();
    }

    #[test]
    fn grid_bounding_box_1d_grid_size() {
        let bbox = GridBoundingBox1D::new([0], [9]).unwrap();
        assert_eq!(bbox.axis_size(), [10]);
        assert_eq!(bbox.axis_size_x(), 10);
        assert_eq!(bbox.axis_size_y(), 1);
        assert_eq!(bbox.number_of_elements(), 10);
    }

    #[test]
    fn grid_bounding_box_2d_grid_size() {
        let bbox = GridBoundingBox2D::new([0, 0], [9, 9]).unwrap();
        assert_eq!(bbox.axis_size(), [10, 10]);
        assert_eq!(bbox.axis_size_x(), 10);
        assert_eq!(bbox.axis_size_y(), 10);
        assert_eq!(bbox.number_of_elements(), 100);
    }

    #[test]
    fn grid_bounding_box_3d_grid_size() {
        let bbox = GridBoundingBox3D::new([0, 0, 0], [9, 9, 9]).unwrap();
        assert_eq!(bbox.axis_size(), [10, 10, 10]);
        assert_eq!(bbox.axis_size_x(), 10);
        assert_eq!(bbox.axis_size_y(), 10);
        assert_eq!(bbox.number_of_elements(), 1000);
    }

    #[test]
    fn grid_bounding_box_1d_intersection() {
        let a = GridBoundingBox::new([1], [42]).unwrap();
        let b = GridBoundingBox::new([2], [69]).unwrap();
        let c = a.intersection(&b).unwrap();
        assert_eq!(c, GridBoundingBox::new([2], [42]).unwrap());
    }

    #[test]
    fn grid_bounding_box_2d_intersection() {
        let a = GridBoundingBox::new([1, 2], [42, 69]).unwrap();
        let b = GridBoundingBox::new([2, 1], [69, 42]).unwrap();
        let c = a.intersection(&b).unwrap();
        assert_eq!(c, GridBoundingBox::new([2, 2], [42, 42]).unwrap());
    }

    #[test]
    fn grid_bounding_box_3d_intersection() {
        let a = GridBoundingBox::new([1, 2, 3], [42, 69, 666]).unwrap();
        let b = GridBoundingBox::new([3, 2, 1], [69, 666, 42]).unwrap();
        let c = a.intersection(&b).unwrap();
        assert_eq!(c, GridBoundingBox::new([3, 2, 3], [42, 69, 42]).unwrap());
    }

    #[test]
    fn grid_bounding_box_1d_linear_space() {
        let a = GridBoundingBox::new([1], [42]).unwrap();
        let l = a.linear_space_index([2]).unwrap();
        assert_eq!(l, 1);
        a.linear_space_index([43]).unwrap_err();
    }

    #[test]
    fn grid_bounding_box_2d_linear_space() {
        let a = GridBoundingBox::new([1, 1], [42, 42]).unwrap();

        assert_eq!(a.linear_space_index([1, 1]).unwrap(), 0);
        assert_eq!(a.linear_space_index([2, 2]).unwrap(), 43);
        a.linear_space_index([43, 43]).unwrap_err();
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn grid_bounding_box_3d_linear_space() {
        let a = GridBoundingBox::new([1, 1, 1], [42, 42, 42]).unwrap();

        assert_eq!(a.linear_space_index([1, 1, 1]).unwrap(), 0);
        assert_eq!(
            a.linear_space_index([2, 2, 2]).unwrap(),
            1 * 42 * 42 + 1 * 42 + 1
        );
        a.linear_space_index([43, 43, 43]).unwrap_err();
    }

    #[test]
    fn grid_bounding_box_1d_linear_space_and_back() {
        let a = GridBoundingBox::new([1], [42]).unwrap();
        let l = a.linear_space_index_unchecked([2]);
        assert_eq!(l, 1);
        assert_eq!(a.grid_idx_unchecked(l), [2].into());
    }

    #[test]
    fn grid_bounding_box_2d_linear_space_and_back() {
        let a = GridBoundingBox::new([1, 1], [42, 42]).unwrap();
        let l = a.linear_space_index_unchecked([1, 1]);
        assert_eq!(l, 0);
        assert_eq!(a.grid_idx_unchecked(l), [1, 1].into());

        let l2 = a.linear_space_index_unchecked([2, 2]);
        assert_eq!(l2, 43);
        assert_eq!(a.grid_idx_unchecked(l2), [2, 2].into());
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn grid_bounding_box_3d_linear_space_and_back() {
        let a = GridBoundingBox::new([1, 1, 1], [42, 42, 42]).unwrap();
        let l = a.linear_space_index_unchecked([1, 1, 1]);
        assert_eq!(l, 0);
        assert_eq!(a.grid_idx_unchecked(l), [1, 1, 1].into());

        let l2 = a.linear_space_index_unchecked([2, 2, 2]);
        assert_eq!(l2, 1 * 42 * 42 + 1 * 42 + 1);
        assert_eq!(a.grid_idx_unchecked(l2), [2, 2, 2].into());
    }
}
