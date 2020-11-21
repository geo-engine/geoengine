use snafu::ensure;

use crate::{error, util::Result};

use super::{
    BoundedGrid, GridBounds, GridContains, GridIdx, GridIdx1D, GridIdx2D, GridIdx3D,
    GridIntersection, GridSize, GridSpaceToLinearSpace,
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
        Ok(GridBoundingBox::new_unchecked(min, max))
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
    Self: GridSize,
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

impl GridContains<GridIdx1D> for GridBoundingBox1D {
    fn contains(&self, rhs: GridIdx1D) -> bool {
        let GridIdx([idx]) = rhs;
        let [min] = self.min;
        let [max] = self.max;
        crate::util::ranges::value_in_range_inclusive(idx, min, max)
    }
}

impl GridContains<GridIdx2D> for GridBoundingBox2D {
    fn contains(&self, rhs: GridIdx2D) -> bool {
        let GridIdx([a, b]) = rhs;
        let [min_a, min_b] = self.min;
        let [max_a, max_b] = self.max;
        crate::util::ranges::value_in_range_inclusive(a, min_a, max_a)
            && crate::util::ranges::value_in_range_inclusive(b, min_b, max_b)
    }
}

impl GridContains<GridIdx3D> for GridBoundingBox3D {
    fn contains(&self, rhs: GridIdx3D) -> bool {
        let GridIdx([a, b, c]) = rhs;
        let [min_a, min_b, min_c] = self.min;
        let [max_a, max_b, max_c] = self.max;
        crate::util::ranges::value_in_range_inclusive(a, min_a, max_a)
            && crate::util::ranges::value_in_range_inclusive(b, min_b, max_b)
            && crate::util::ranges::value_in_range_inclusive(c, min_c, max_c)
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
            self.contains(real_index),
            error::GridIndexOutOfBounds {
                index: Vec::from(real_index.0),
                min_index: Vec::from(self.min_index().0),
                max_index: Vec::from(self.max_index().0)
            }
        );
        Ok(self.linear_space_index_unchecked(real_index))
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

    fn linear_space_index<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> Result<usize> {
        let real_index = index.into();
        ensure!(
            self.contains(real_index),
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
            self.contains(real_index),
            error::GridIndexOutOfBounds {
                index: Vec::from(real_index.0),
                min_index: Vec::from(self.min_index().0),
                max_index: Vec::from(self.max_index().0)
            }
        );
        Ok(self.linear_space_index_unchecked(real_index))
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
