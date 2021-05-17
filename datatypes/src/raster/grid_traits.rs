use std::ops::Add;

use crate::util::Result;

use super::{GridBoundingBox, GridIdx, GridShape};

/// Size information of a grid include the size of each axis and the number elements
pub trait GridSize {
    /// An array with one entry representing the size of each axis
    type ShapeArray: AsRef<[usize]>;

    /// The number of axis
    const NDIM: usize;

    // Size per axis in [.. ,z, y, x] order
    fn axis_size(&self) -> Self::ShapeArray;
    /// Size of the x-axis
    fn axis_size_x(&self) -> usize {
        match *self.axis_size().as_ref() {
            [] => 0,
            [.., a] => a,
        }
    }

    /// Size of the y-axis
    fn axis_size_y(&self) -> usize {
        match *self.axis_size().as_ref() {
            [] => 0,
            [_] => 1,
            [.., b, _] => b,
        }
    }

    /// The number of elements in the grid
    fn number_of_elements(&self) -> usize {
        self.axis_size().as_ref().iter().product()
    }
}
/// The bounds of a grid are the minimal / the maximal valid index of each axis.
pub trait GridBounds {
    /// An array with one entry representing a position on an axis
    type IndexArray: AsRef<[isize]> + Into<GridIdx<Self::IndexArray>>;

    /// The minimal valid index. This has the min valid value for each axis
    fn min_index(&self) -> GridIdx<Self::IndexArray>;
    /// The max valid index. This has the max valid value for each axis
    fn max_index(&self) -> GridIdx<Self::IndexArray>;
}

/// Bounded Grids can produce a `GridBoundingBox`
pub trait BoundedGrid {
    /// An array with one entry representing a position on an axis
    type IndexArray: AsRef<[isize]> + Into<GridIdx<Self::IndexArray>>;
    /// Produces a `GridBoundingBox` with min/max values and size
    fn bounding_box(&self) -> GridBoundingBox<Self::IndexArray>;
}

/// Provides the contains method
pub trait GridContains<Rhs = Self> {
    // Returns true if Self contains Rhs
    fn contains(&self, rhs: &Rhs) -> bool;
}

impl<A, T> GridContains<GridIdx<A>> for T
where
    T: GridBounds<IndexArray = A>,
    A: AsRef<[isize]>,
{
    fn contains(&self, rhs: &GridIdx<A>) -> bool {
        for ((&min_idx, &max_idx), &idx) in self
            .min_index()
            .as_slice()
            .iter()
            .zip(self.max_index().as_slice())
            .zip(rhs.as_slice())
        {
            if !crate::util::ranges::value_in_range_inclusive(idx, min_idx, max_idx) {
                return false;
            }
        }
        true
    }
}

/// Provides the intersection method
pub trait GridIntersection<Rhs = Self, Out = Self> {
    // Returns true if Self intesects Rhs
    fn intersection(&self, other: &Rhs) -> Option<Out>;
}

/// Provides the methods needed to map an n-dimensional `GridIdx` to linear space.
pub trait GridSpaceToLinearSpace: GridSize {
    /// An array where each entry represents a position on an axis
    type IndexArray: AsRef<[isize]>;

    /// Strides indicate how many linear space elements the next element at the same position of any axis is away.
    fn strides(&self) -> Self::ShapeArray;
    /// Calculate the zero based linear space location of an index
    fn linear_space_index_unchecked<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> usize;
    /// Calculate the zero based linear space location of an index
    /// # Errors
    /// This method fails if the index is out of bounds
    fn linear_space_index<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> Result<usize>;
}

/// Mutate elements stored in a Grid
pub trait GridIndexAccessMut<T, I> {
    /// Sets the value at a grid index
    ///
    /// # Errors
    ///
    /// The method fails if the grid index is out of bounds.
    ///
    fn set_at_grid_index(&mut self, grid_index: I, value: T) -> Result<()>;

    /// Sets the value at a grid index
    fn set_at_grid_index_unchecked(&mut self, grid_index: I, value: T);
}

/// Read elements stored in a Grid
pub trait GridIndexAccess<T, I> {
    /// Gets a reference to the value at a grid index
    ///
    /// The method fails if the grid index is out of bounds.
    ///
    fn get_at_grid_index(&self, grid_index: I) -> Result<T>;

    /// Gets a reference to the value at a grid index
    fn get_at_grid_index_unchecked(&self, grid_index: I) -> T;
}

/// The shape of an array
pub trait GridShapeAccess {
    type ShapeArray: AsRef<[usize]> + Into<GridShape<Self::ShapeArray>>;

    fn grid_shape_array(&self) -> Self::ShapeArray;

    fn grid_shape(&self) -> GridShape<Self::ShapeArray> {
        GridShape::new(self.grid_shape_array())
    }
}

/// Provides the the value used to represent a no data entry.
pub trait NoDataValue {
    type NoDataType: PartialEq + Copy;

    fn no_data_value(&self) -> Option<Self::NoDataType>;

    fn is_no_data(&self, value: Self::NoDataType) -> bool {
        self.no_data_value()
            .map_or(false, |no_data_value| value == no_data_value)
    }
}

/// Change the bounds of gridded data.
pub trait ChangeGridBounds<I>: BoundedGrid<IndexArray = I>
where
    I: AsRef<[isize]> + Into<GridIdx<I>> + Clone,
    GridBoundingBox<I>: GridSize,
    GridIdx<I>: Add<Output = GridIdx<I>> + From<I>,
{
    type Output;

    fn shift_bounding_box(&self, offset: GridIdx<I>) -> GridBoundingBox<I> {
        let bounds = self.bounding_box();
        GridBoundingBox::new_unchecked(
            bounds.min_index() + offset.clone(),
            bounds.max_index() + offset,
        )
    }

    /// shift using an offset
    fn shift_by_offset(self, offset: GridIdx<I>) -> Self::Output;

    /// set new bounds. will fail if the axis sizes do not match.
    fn set_grid_bounds(self, bounds: GridBoundingBox<I>) -> Result<Self::Output>;
}
