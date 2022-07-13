use super::{Grid, GridIndexAccess, GridShape1D, GridShape2D, GridShape3D, GridSize, MaskedGrid};
use crate::util::{helpers::equals_or_both_nan, Result};

pub type NoDataValueGrid1D<T> = NoDataValueGrid<GridShape1D, T>;
pub type NoDataValueGrid2D<T> = NoDataValueGrid<GridShape2D, T>;
pub type NoDataValueGrid3D<T> = NoDataValueGrid<GridShape3D, T>;

/// A `NoDataValueGrid` is an n-dmensional array.
/// In this grid type each element/pixel can be invalid. This is the case if the element/pixel value is equal to the value storef in the `no_data_value` field.
pub struct NoDataValueGrid<D, T> {
    pub inner_grid: Grid<D, T>,
    pub no_data_value: Option<T>, // TODO: do we need the option or is there always a no_data_value?
}

impl<D, T> NoDataValueGrid<D, T>
where
    T: PartialEq,
{
    /// Create a new `NoDataValueGrid` from a `Grid` and a `no_data_value`.
    pub fn new(inner_grid: Grid<D, T>, no_data_value: Option<T>) -> Self {
        NoDataValueGrid {
            inner_grid,
            no_data_value,
        }
    }

    /// Set the `no_data_value` which represents invalid elements / pixels.
    pub fn set_no_data_value(&mut self, no_data_value: Option<T>) {
        self.no_data_value = no_data_value;
    }

    /// Test if the provided value is equal to the `no_data_value` of this `NoDataValueGrid`.
    pub fn is_no_data_value(&self, value: &T) -> bool {
        if let Some(no_data_value) = &self.no_data_value {
            equals_or_both_nan(value, no_data_value)
        } else {
            false
        }
    }

    /// Create a new `NoDataValueGrid` from a `MaskedGrid` where each invalid pixel is assigned the `no_data_value`.
    pub fn from_masked_grid(masked_grid: &MaskedGrid<D, T>, no_data_value: T) -> Self
    where
        D: GridSize + Clone + PartialEq,
        T: Copy,
    {
        let innder_data = masked_grid
            .masked_element_deref_iterator()
            .map(|value_option| {
                if let Some(value) = value_option {
                    value
                } else {
                    no_data_value
                }
            })
            .collect::<Vec<T>>();

        let inner_grid =
            Grid::new(masked_grid.shape().clone(), innder_data).expect("shape missmatch");
        Self::new(inner_grid, Some(no_data_value))
    }
}

impl<D, I, T> GridIndexAccess<Option<T>, I> for NoDataValueGrid<D, T>
where
    T: Copy + PartialEq,
    Grid<D, T>: GridIndexAccess<T, I>,
{
    fn get_at_grid_index(&self, grid_index: I) -> Result<Option<T>> {
        let grid_value = self.inner_grid.get_at_grid_index(grid_index)?;
        if self.is_no_data_value(&grid_value) {
            return Ok(None);
        }
        Ok(Some(grid_value))
    }

    fn get_at_grid_index_unchecked(&self, grid_index: I) -> Option<T> {
        let grid_value = self.inner_grid.get_at_grid_index_unchecked(grid_index);
        if self.is_no_data_value(&grid_value) {
            return None;
        }
        Some(grid_value)
    }
}

impl<D, T> From<Grid<D, T>> for NoDataValueGrid<D, T>
where
    T: Copy + PartialEq,
{
    fn from(inner_grid: Grid<D, T>) -> Self {
        NoDataValueGrid {
            inner_grid,
            no_data_value: None,
        }
    }
}

impl<D, T> From<NoDataValueGrid<D, T>> for MaskedGrid<D, T>
where
    T: PartialEq + Clone,
    D: Clone + GridSize + PartialEq,
{
    fn from(n: NoDataValueGrid<D, T>) -> Self {
        if n.no_data_value.is_none() {
            return MaskedGrid::from(n.inner_grid);
        }

        let validity_mask = n
            .inner_grid
            .data
            .iter()
            .map(|v| !n.is_no_data_value(v))
            .collect();
        let validity_grid =
            Grid::new(n.inner_grid.shape.clone(), validity_mask).expect("shape mismatch");

        MaskedGrid {
            inner_grid: n.inner_grid,
            validity_mask: validity_grid,
        }
    }
}
