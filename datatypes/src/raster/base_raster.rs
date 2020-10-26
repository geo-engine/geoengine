use crate::error;
use crate::raster::{
    Capacity, GridDimension, GridIndex, GridPixelAccess, GridPixelAccessMut, Pixel,
};
use crate::util::Result;
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::convert::AsRef;
use std::fmt::Debug;

use super::{Dim1D, Dim2D, Dim3D};

pub type Raster1D<T> = BaseRaster<Dim1D, T, Vec<T>>;
pub type Raster2D<T> = BaseRaster<Dim2D, T, Vec<T>>;
pub type Raster3D<T> = BaseRaster<Dim3D, T, Vec<T>>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BaseRaster<D, T, C>
where
    T: Pixel,
{
    pub grid_dimension: D,
    pub data_container: C,
    pub no_data_value: Option<T>,
}

impl<D, T, C> BaseRaster<D, T, C>
where
    D: GridDimension,
    C: Capacity,
    T: Pixel,
{
    /// Generates a new raster
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::raster::{Raster, Dim, Raster2D};
    /// use geoengine_datatypes::primitives::TimeInterval;
    ///
    /// let mut raster2d = Raster2D::new(
    ///    [3, 2].into(),
    ///    vec![1,2,3,4,5,6],
    ///    None,
    ///    TimeInterval::default(),
    ///    [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
    /// ).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// This constructor fails if the data container's capacity is different from the grid's dimension capacity
    ///
    pub fn new(grid_dimension: D, data_container: C, no_data_value: Option<T>) -> Result<Self> {
        ensure!(
            grid_dimension.capacity() == data_container.capacity(),
            error::DimensionCapacityDoesNotMatchDataCapacity {
                dimension_cap: grid_dimension.capacity(),
                data_cap: data_container.capacity()
            }
        );

        Ok(Self {
            grid_dimension,
            data_container,
            no_data_value,
        })
    }

    /// Converts the data type of the raster by converting it pixel-wise
    /// TODO: Is is correct to always return a `Vec`?
    pub fn convert<To>(self) -> BaseRaster<D, To, Vec<To>>
    where
        C: AsRef<[T]>,
        To: Pixel,
        T: AsPrimitive<To>,
    {
        BaseRaster::new(
            self.grid_dimension,
            self.data_container
                .as_ref()
                .iter()
                .map(|&pixel| pixel.as_())
                .collect(),
            self.no_data_value.map(AsPrimitive::as_),
        )
        .expect("raster type conversion cannot fail")
    }
}

impl<D: GridDimension, T: Pixel> BaseRaster<D, T, Vec<T>> {
    pub fn new_filled(grid_dimension: D, fill_value: T, no_data_value: Option<T>) -> Self {
        let data = vec![fill_value; grid_dimension.number_of_elements()];
        Self::new(grid_dimension, data, no_data_value).expect("sizes must match")
    }
}

impl<D, T, C, I> GridPixelAccess<T, I> for BaseRaster<D, T, C>
where
    D: GridDimension<IndexType = I>,
    I: GridIndex,
    C: AsRef<[T]> + Capacity,
    T: Pixel,
{
    fn pixel_value_at_grid_index(&self, grid_index: &I) -> Result<T> {
        let index = grid_index.linear_space_index(&self.grid_dimension)?;
        Ok(self.data_container.as_ref()[index])
    }
}

impl<D, T, C, I> GridPixelAccessMut<T, I> for BaseRaster<D, T, C>
where
    D: GridDimension<IndexType = I>,
    I: GridIndex,
    C: AsMut<[T]> + Capacity,
    T: Pixel,
{
    fn set_pixel_value_at_grid_index(&mut self, grid_index: &I, value: T) -> Result<()> {
        let index = grid_index.linear_space_index(&self.grid_dimension)?;
        self.data_container.as_mut()[index] = value;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{GridPixelAccess, GridPixelAccessMut, Raster2D};

    #[test]
    fn simple_raster_2d() {
        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        Raster2D::new(dim.into(), data, None).unwrap();
    }

    #[test]
    fn simple_raster_2d_at_tuple() {
        let tuple_index = [1, 1];

        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        let raster2d = Raster2D::new(dim.into(), data, None).unwrap();
        assert_eq!(raster2d.pixel_value_at_grid_index(&tuple_index).unwrap(), 4);
    }

    #[test]
    fn simple_raster_2d_at_arr() {
        let index = [1, 1];

        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        let raster2d = Raster2D::new(dim.into(), data, None).unwrap();
        let value = raster2d.pixel_value_at_grid_index(&index).unwrap();
        assert_eq!(value, 4);
    }

    #[test]
    fn simple_raster_2d_set_at_tuple() {
        let tuple_index = [1, 1];

        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        let mut raster2d = Raster2D::new(dim.into(), data, None).unwrap();

        raster2d
            .set_pixel_value_at_grid_index(&tuple_index, 9)
            .unwrap();
        let value = raster2d.pixel_value_at_grid_index(&tuple_index).unwrap();
        assert_eq!(value, 9);
        assert_eq!(raster2d.data_container, [1, 2, 3, 9, 5, 6]);
    }
}
