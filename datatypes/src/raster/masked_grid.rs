use std::ops::Add;

use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::{error, util::Result};

use super::{
    ChangeGridBounds, EmptyGrid, Grid, GridBoundingBox, GridBounds, GridIdx, GridIndexAccess,
    GridIndexAccessMut, GridShape, GridShape1D, GridShape2D, GridShape3D, GridShapeAccess,
    GridSize, GridSpaceToLinearSpace,
};

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
/// A `MaskedGrid` is an n-dmensional array.
/// For each element/pixel a `validity_mask` stores if the pixel is valid.
pub struct MaskedGrid<D, T> {
    pub inner_grid: Grid<D, T>,
    pub validity_mask: Grid<D, bool>, // TODO: switch to bitmask or something like that
}

pub type MaskedGrid1D<T> = MaskedGrid<GridShape1D, T>;
pub type MaskedGrid2D<T> = MaskedGrid<GridShape2D, T>;
pub type MaskedGrid3D<T> = MaskedGrid<GridShape3D, T>;

impl<D, T> MaskedGrid<D, T>
where
    D: GridSize + PartialEq + Clone,
    T: Clone,
{
    /// Creates a new `MaskedGrid`
    ///
    /// # Errors
    ///
    /// This constructor fails if the data container's capacity is different from the grid's dimension number or if data and validity mask have different shapes.
    ///
    pub fn new(data: Grid<D, T>, validity_mask: Grid<D, bool>) -> Result<Self> {
        ensure!(
            data.shape == validity_mask.shape,
            error::DimensionCapacityDoesNotMatchDataCapacity {
                dimension_cap: data.shape.number_of_elements(),
                data_cap: validity_mask.shape.number_of_elements()
            }
        );

        Ok(Self {
            inner_grid: data,
            validity_mask,
        })
    }

    /// Replace the current mask with a new `Grid` of `bool`.
    pub fn replace_mask(self, validity_mask: Grid<D, bool>) -> Result<Self> {
        Self::new(self.inner_grid, validity_mask)
    }

    /// Create a new `MaskedGrid` where all pixels are valid.
    pub fn new_with_data(data: Grid<D, T>) -> Self {
        let validity_mask = Grid::new_filled(data.shape.clone(), true);

        Self {
            inner_grid: data,
            validity_mask,
        }
    }

    /// Create a new `MaskedGrid` where all pixels have the same value and are valid.
    pub fn new_filled(shape: D, fill_value: T) -> Self {
        let data = Grid::new_filled(shape, fill_value);
        Self::new_with_data(data)
    }

        /// Create a new `MaskedGrid` where all pixels have the same value and are valid.
    pub fn new_empty(shape: D) -> Self where T: Default{
        let data = Grid::new_filled(shape.clone(), T::default());
        let validity = Grid::new_filled(shape, false);
        Self::new(data, validity).expect("new_filled can not fail wen using a valid shape")
    }

    /// Get a reference to the shape of the grid.
    pub fn shape(&self) -> &D {
        &self.inner_grid.shape
    }

    #[inline]
    /// Get a reference to the inner mask.
    pub fn mask_ref(&self) -> &Grid<D, bool> {
        &self.validity_mask
    }

    #[inline]
    /// Get a mutable reference to the inner mask.
    pub fn mask_mut(&mut self) -> &mut Grid<D, bool> {
        &mut self.validity_mask
    }

    /// Returns an `Iterator<Item=Option<&T>>`. The `Iterator` produces all elements of the `MaskedGrid` where invalid elements are `None` and valid elements are `Some(&T)`.
    pub fn masked_element_ref_iterator(&self) -> impl Iterator<Item = Option<&T>> {
        self.inner_grid
            .data
            .iter()
            .zip(self.validity_mask.data.iter())
            .map(|(v, m)| if *m { Some(v) } else { None })
    }

    /// Returns an `Iterator<Item=Option<T>>`. The `Iterator` produces and copies all elements (that are `copy`) of the `MaskedGrid` where invalid elements are `None` and valid elements are `Some(T)`.
    pub fn masked_element_deref_iterator(&self) -> impl Iterator<Item = Option<T>> + '_
    where
        T: Copy,
    {
        self.masked_element_ref_iterator()
            .map(std::option::Option::<&T>::copied)
    }
}

impl<D, T> From<Grid<D, T>> for MaskedGrid<D, T>
where
    D: GridSize + PartialEq + Clone,
    T: Clone,
{
    fn from(data: Grid<D, T>) -> Self {
        Self::new_with_data(data)
    }
}

impl<D, T> From<EmptyGrid<D, T>> for MaskedGrid<D, T>
where
    D: GridSize + PartialEq + Clone,
    T: Clone + Default,
{
    fn from(data: EmptyGrid<D, T>) -> Self {
        Self::new_empty(data.shape)
    }
}

impl<D, T> AsRef<Grid<D, T>> for MaskedGrid<D, T> {
    #[inline]
    fn as_ref(&self) -> &Grid<D, T> {
        &self.inner_grid
    }
}

impl<D, T> AsMut<Grid<D, T>> for MaskedGrid<D, T> {
    #[inline]
    fn as_mut(&mut self) -> &mut Grid<D, T> {
        &mut self.inner_grid
    }
}

impl<D, T> GridSize for MaskedGrid<D, T>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    type ShapeArray = D::ShapeArray;

    const NDIM: usize = D::NDIM;

    fn axis_size(&self) -> Self::ShapeArray {
        self.inner_grid.axis_size()
    }

    fn number_of_elements(&self) -> usize {
        self.inner_grid.number_of_elements()
    }
}

impl<T, D, I> GridIndexAccess<Option<T>, I> for MaskedGrid<D, T>
where
    T: Copy,
    Grid<D, T>: GridIndexAccess<T, I>,
    Grid<D, bool>: GridIndexAccess<bool, I>,
    I: Clone,
{
    fn get_at_grid_index(&self, grid_index: I) -> Result<Option<T>> {
        if !self.validity_mask.get_at_grid_index(grid_index.clone())? {
            return Ok(None);
        }

        self.inner_grid
            .get_at_grid_index(grid_index)
            .map(Option::Some)
    }

    fn get_at_grid_index_unchecked(&self, grid_index: I) -> Option<T> {
        if !self
            .validity_mask
            .get_at_grid_index_unchecked(grid_index.clone())
        {
            return None;
        }

        Some(self.inner_grid.get_at_grid_index_unchecked(grid_index))
    }
}

impl<D, T, I> GridIndexAccessMut<Option<T>, I> for MaskedGrid<D, T>
where
    Grid<D, T>: GridIndexAccessMut<T, I>,
    Grid<D, bool>: GridIndexAccessMut<bool, I>,
    D: PartialEq + GridSize + Clone,
    T: Clone,
    I: Clone,
{
    fn set_at_grid_index(&mut self, grid_index: I, value: Option<T>) -> Result<()> {
        self.mask_mut()
            .set_at_grid_index(grid_index.clone(), value.is_some())?;

        if let Some(v) = value {
            self.inner_grid.set_at_grid_index(grid_index, v)?;
        }
        Ok(())
    }

    fn set_at_grid_index_unchecked(&mut self, grid_index: I, value: Option<T>) {
        self.validity_mask
            .set_at_grid_index_unchecked(grid_index.clone(), value.is_some());

        if let Some(v) = value {
            self.inner_grid.set_at_grid_index_unchecked(grid_index, v);
        }
    }
}

impl<D, T, I> GridIndexAccessMut<T, I> for MaskedGrid<D, T>
where
    Grid<D, T>: GridIndexAccessMut<T, I>,
    Grid<D, bool>: GridIndexAccessMut<bool, I>,
    D: PartialEq + GridSize + Clone,
    T: Clone,
    I: Clone,
{
    fn set_at_grid_index(&mut self, grid_index: I, value: T) -> Result<()> {
        self.set_at_grid_index(grid_index, Some(value))
    }

    fn set_at_grid_index_unchecked(&mut self, grid_index: I, value: T) {
        self.set_at_grid_index_unchecked(grid_index, Some(value));
    }
}

impl<T, D> GridBounds for MaskedGrid<D, T>
where
    D: GridBounds,
{
    type IndexArray = D::IndexArray;
    fn min_index(&self) -> GridIdx<<Self as GridBounds>::IndexArray> {
        self.inner_grid.min_index()
    }
    fn max_index(&self) -> GridIdx<<Self as GridBounds>::IndexArray> {
        self.inner_grid.max_index()
    }
}

impl<D, T> GridShapeAccess for MaskedGrid<D, T>
where
    D: GridSize,
    D::ShapeArray: Into<GridShape<D::ShapeArray>>,
    T: Copy,
{
    type ShapeArray = D::ShapeArray;

    fn grid_shape_array(&self) -> Self::ShapeArray {
        self.inner_grid.grid_shape_array()
    }
}

impl<D, T, I> ChangeGridBounds<I> for MaskedGrid<D, T>
where
    I: AsRef<[isize]> + Clone,
    D: GridBounds<IndexArray = I> + Clone,
    T: Clone,
    GridBoundingBox<I>: GridSize,
    GridIdx<I>: Add<Output = GridIdx<I>> + From<I> + Clone,
{
    type Output = MaskedGrid<GridBoundingBox<I>, T>;

    fn shift_by_offset(self, offset: GridIdx<I>) -> Self::Output {
        MaskedGrid {
            inner_grid: self.inner_grid.shift_by_offset(offset.clone()),
            validity_mask: self.validity_mask.shift_by_offset(offset),
        }
    }

    fn set_grid_bounds(self, bounds: GridBoundingBox<I>) -> Result<Self::Output> {
        Ok(MaskedGrid {
            inner_grid: self.inner_grid.set_grid_bounds(bounds.clone())?,
            validity_mask: self.validity_mask.set_grid_bounds(bounds)?,
        })
    }
}
