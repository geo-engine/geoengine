use std::ops::Add;

use serde::{Deserialize, Serialize};

use crate::util::Result;

use super::{
    grid_traits::{MaskedGridIndexAccess, MaskedGridIndexAccessMut},
    ChangeGridBounds, EmptyGrid, Grid, GridBoundingBox, GridBounds, GridIdx, GridIndexAccess,
    GridIndexAccessMut, GridShape, GridShape1D, GridShape2D, GridShape3D, GridShapeAccess,
    GridSize, GridSpaceToLinearSpace,
};

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MaskedGrid<D, T> {
    pub data: Grid<D, T>,
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
    /// Creates a new `Grid`
    ///
    /// # Errors
    ///
    /// This constructor fails if the data container's capacity is different from the grid's dimension number
    ///
    pub fn new(data: Grid<D, T>, validity_mask: Grid<D, bool>) -> Result<Self> {
        //        ensure!(
        //            data.shape == mask.shape,
        //            error::DimensionCapacityDoesNotMatchDataCapacity {
        //                dimension_cap: shape.number_of_elements(),
        //                data_cap: data.len()
        //            }
        //        );

        Ok(Self {
            data,
            validity_mask,
        })
    }

    pub fn replace_mask(self, validity_mask: Grid<D, bool>) -> Result<Self> {
        Self::new(self.data, validity_mask)
    }

    pub fn new_with_data(data: Grid<D, T>) -> Self {
        let validity_mask = Grid::new_filled(data.shape.clone(), true);

        Self {
            data,
            validity_mask,
        }
    }

    pub fn new_filled(shape: D, fill_value: T) -> Self {
        let data = Grid::new_filled(shape, fill_value);
        Self::new_with_data(data)
    }

    pub fn shape(&self) -> &D {
        &self.data.shape
    }

    #[inline]
    pub fn mask_ref(&self) -> &Grid<D, bool> {
        &self.validity_mask
    }

    #[inline]
    pub fn mask_mut(&mut self) -> &mut Grid<D, bool> {
        &mut self.validity_mask
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
        Self::new_filled(data.shape, T::default())
    }
}

impl<D, T> AsRef<Grid<D, T>> for MaskedGrid<D, T> {
    #[inline]
    fn as_ref(&self) -> &Grid<D, T> {
        &self.data
    }
}

impl<D, T> AsMut<Grid<D, T>> for MaskedGrid<D, T> {
    #[inline]
    fn as_mut(&mut self) -> &mut Grid<D, T> {
        &mut self.data
    }
}

impl<D, T> GridSize for MaskedGrid<D, T>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    type ShapeArray = D::ShapeArray;

    const NDIM: usize = D::NDIM;

    fn axis_size(&self) -> Self::ShapeArray {
        self.data.axis_size()
    }

    fn number_of_elements(&self) -> usize {
        self.data.number_of_elements()
    }
}

impl<D, T, I> MaskedGridIndexAccess<T, I> for MaskedGrid<D, T>
where
    Grid<D, T>: GridIndexAccess<T, I>,
    Grid<D, bool>: GridIndexAccess<bool, I>,
    I: Clone,
{
    fn get_masked_at_grid_index(&self, grid_index: I) -> Result<Option<T>> {
        if !self.validity_mask.get_at_grid_index(grid_index.clone())? {
            return Ok(None);
        }

        self.data.get_at_grid_index(grid_index).map(Option::Some)
    }

    fn get_masked_at_grid_index_unchecked(&self, grid_index: I) -> Option<T> {
        if !self
            .validity_mask
            .get_at_grid_index_unchecked(grid_index.clone())
        {
            return None;
        }

        Some(self.data.get_at_grid_index_unchecked(grid_index))
    }
}

impl<D, T, I> MaskedGridIndexAccessMut<T, I> for MaskedGrid<D, T>
where
    Grid<D, T>: GridIndexAccessMut<T, I>,
    Grid<D, bool>: GridIndexAccessMut<bool, I>,
    D: PartialEq + GridSize + Clone,
    T: Clone,
    I: Clone,
{
    fn set_masked_at_grid_index(&mut self, grid_index: I, value: Option<T>) -> Result<()> {
        &mut self
            .validity_mask
            .set_at_grid_index(grid_index.clone(), value.is_some())?;

        if let Some(v) = value {
            self.data.set_at_grid_index(grid_index, v)?;
        }
        Ok(())
    }

    fn set_masked_at_grid_index_unchecked(&mut self, grid_index: I, value: Option<T>) {
        self.validity_mask
            .set_at_grid_index_unchecked(grid_index.clone(), value.is_some());

        if let Some(v) = value {
            self.data.set_at_grid_index_unchecked(grid_index, v);
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
        self.set_masked_at_grid_index(grid_index, Some(value))
    }

    fn set_at_grid_index_unchecked(&mut self, grid_index: I, value: T) {
        self.set_masked_at_grid_index_unchecked(grid_index, Some(value))
    }
}

impl<T, D> GridBounds for MaskedGrid<D, T>
where
    D: GridBounds,
{
    type IndexArray = D::IndexArray;
    fn min_index(&self) -> GridIdx<<Self as GridBounds>::IndexArray> {
        self.data.min_index()
    }
    fn max_index(&self) -> GridIdx<<Self as GridBounds>::IndexArray> {
        self.data.max_index()
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
        self.data.grid_shape_array()
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
            data: self.data.shift_by_offset(offset.clone()),
            validity_mask: self.validity_mask.shift_by_offset(offset),
        }
    }

    fn set_grid_bounds(self, bounds: GridBoundingBox<I>) -> Result<Self::Output> {
        Ok(MaskedGrid {
            data: self.data.set_grid_bounds(bounds.clone())?,
            validity_mask: self.validity_mask.set_grid_bounds(bounds)?,
        })
    }
}
