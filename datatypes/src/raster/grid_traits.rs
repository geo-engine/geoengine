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
        debug_assert!(self.min_index().as_slice().len() == rhs.as_slice().len()); // this must never fail since the array type A has a fixed size. However, one might implement that for a Vec so better check it here.

        self.min_index()
            .as_slice()
            .iter()
            .zip(self.max_index().as_slice())
            .zip(rhs.as_slice())
            .all(|((&min_idx, &max_idx), idx)| (min_idx..=max_idx).contains(idx))
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

    /// Returns the `GridIdx` of the linear_idx-th cell in the Grid.
    fn grid_idx_unchecked(&self, linear_idx: usize) -> GridIdx<Self::IndexArray>;

    /// Returns the `GridIdx` of the `linear_idx`-th cell in the Grid. Returns none if `linear_idx` is out of bounds.
    fn grid_idx(&self, linear_idx: usize) -> Option<GridIdx<Self::IndexArray>> {
        if linear_idx >= self.number_of_elements() {
            return None;
        }
        Some(self.grid_idx_unchecked(linear_idx))
    }
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

pub trait MaskedGridIndexAccessMut<T, I> {
    /// Sets a masked value at a grid index. None represents a missing value / no-data.
    ///
    /// # Errors
    ///
    /// The method fails if the grid index is out of bounds.
    ///
    fn set_masked_at_grid_index(&mut self, grid_index: I, value: Option<T>) -> Result<()>;

    /// Sets the value at a grid index
    fn set_masked_at_grid_index_unchecked(&mut self, grid_index: I, value: Option<T>);
}

/// Read elements stored in a Grid
pub trait MaskedGridIndexAccess<T, I> {
    /// Gets a reference to the value at a grid index. None represents a missing value / no-data.
    ///
    /// The method fails if the grid index is out of bounds.
    ///
    fn get_masked_at_grid_index(&self, grid_index: I) -> Result<Option<T>>;

    /// Gets a reference to the value at a grid index
    fn get_masked_at_grid_index_unchecked(&self, grid_index: I) -> Option<T>;
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

    #[allow(clippy::eq_op)]
    fn is_no_data(&self, value: Self::NoDataType) -> bool {
        self.no_data_value().map_or(false, |no_data_value| {
            // value is equal no-data value or both are NAN.
            value == no_data_value || (no_data_value != no_data_value && value != value)
        })
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

pub trait GridStep<I>: GridSpaceToLinearSpace<IndexArray = I>
where
    I: AsRef<[isize]> + Into<GridIdx<I>> + Clone,
    GridBoundingBox<I>: GridSize,
{
    // TODO: implement safe methods?

    /// Move the `GridIdx` along the positive axis directions
    fn inc_idx_unchecked<Idx: Into<GridIdx<I>>>(
        &self,
        idx: Idx,
        step: usize,
    ) -> Option<GridIdx<I>> {
        let l = self.linear_space_index_unchecked(idx);
        if l >= self.number_of_elements() {
            return None;
        }
        let l = l + step;
        self.grid_idx(l)
    }

    /// Move the `GridIdx` along the negative axis directions
    fn dec_idx_unckecked<Idx: Into<GridIdx<I>>>(
        &self,
        idx: Idx,
        step: usize,
    ) -> Option<GridIdx<I>> {
        let l = self.linear_space_index_unchecked(idx);
        if l >= self.number_of_elements() || step > l {
            return None;
        }
        let l = l - step;
        self.grid_idx(l)
    }
}

impl<G, I> GridStep<I> for G
where
    G: GridSpaceToLinearSpace<IndexArray = I>,
    I: AsRef<[isize]> + Into<GridIdx<I>> + Clone,
    GridBoundingBox<I>: GridSize,
{
}

#[cfg(test)]
mod tests {
    use super::*;

    impl NoDataValue for f32 {
        type NoDataType = f32;

        fn no_data_value(&self) -> Option<Self::NoDataType> {
            Some(*self)
        }
    }

    #[test]
    fn no_data_nan() {
        let no_data_value = f32::NAN;

        assert!(!no_data_value.is_no_data(42.));
        assert!(no_data_value.is_no_data(f32::NAN));
    }

    #[test]
    fn no_data_float() {
        let no_data_value: f32 = 42.;

        assert!(no_data_value.is_no_data(42.));
        assert!(!no_data_value.is_no_data(f32::NAN));
    }
}
