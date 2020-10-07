use crate::raster::{
    Capacity, Dim, GenericRaster, GeoTransform, GridDimension, GridIndex, GridPixelAccess,
    GridPixelAccessMut, Pixel, Raster,
};
use crate::util::Result;
use crate::{
    error,
    primitives::{BoundingBox2D, SpatialBounded, TemporalBounded, TimeInterval},
};
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::convert::AsRef;
use std::fmt::Debug;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BaseRaster<D, T, C>
where
    T: Pixel,
{
    pub grid_dimension: D,
    pub data_container: C,
    pub no_data_value: Option<T>,
    pub geo_transform: GeoTransform,
    pub temporal_bounds: TimeInterval,
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
    pub fn new(
        grid_dimension: D,
        data_container: C,
        no_data_value: Option<T>,
        temporal_bounds: TimeInterval,
        geo_transform: GeoTransform,
    ) -> Result<Self> {
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
            temporal_bounds,
            geo_transform,
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
            self.temporal_bounds,
            self.geo_transform,
        )
        .expect("raster type conversion cannot fail")
    }
}

impl<D, T, C> TemporalBounded for BaseRaster<D, T, C>
where
    T: Pixel,
{
    fn temporal_bounds(&self) -> TimeInterval {
        self.temporal_bounds
    }
}

impl<D, T, C> SpatialBounded for BaseRaster<D, T, C>
where
    D: GridDimension,
    T: Pixel,
{
    fn spatial_bounds(&self) -> BoundingBox2D {
        let top_left_coord = self.geo_transform.grid_2d_to_coordinate_2d([0, 0]);
        let lower_right_coord = self.geo_transform.grid_2d_to_coordinate_2d([
            self.grid_dimension.size_of_y_axis(),
            self.grid_dimension.size_of_x_axis(),
        ]);
        BoundingBox2D::new_upper_left_lower_right_unchecked(top_left_coord, lower_right_coord)
    }
}

impl<D, T, C> Raster<D, T, C> for BaseRaster<D, T, C>
where
    D: GridDimension,
    T: Pixel,
    C: Capacity,
{
    fn dimension(&self) -> &D {
        &self.grid_dimension
    }
    fn no_data_value(&self) -> Option<T> {
        self.no_data_value
    }
    fn data_container(&self) -> &C {
        &self.data_container
    }
    fn geo_transform(&self) -> &GeoTransform {
        &self.geo_transform
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

pub type Raster2D<T> = BaseRaster<Dim<[usize; 2]>, T, Vec<T>>;
pub type Raster3D<T> = BaseRaster<Dim<[usize; 3]>, T, Vec<T>>;

impl<T: Send + Debug> GenericRaster for Raster2D<T>
where
    T: Pixel,
{
    fn get(&self) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::{GridPixelAccess, GridPixelAccessMut, Raster2D, TimeInterval};

    #[test]
    fn simple_raster_2d() {
        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        let geo_transform = [1.0, 1.0, 0.0, 1.0, 0.0, 1.0];
        let temporal_bounds: TimeInterval = TimeInterval::default();
        Raster2D::new(
            dim.into(),
            data,
            None,
            temporal_bounds,
            geo_transform.into(),
        )
        .unwrap();
    }

    #[test]
    fn simple_raster_2d_at_tuple() {
        let tuple_index = [1, 1];

        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        let geo_transform = [1.0, 1.0, 0.0, 1.0, 0.0, 1.0];
        let temporal_bounds: TimeInterval = TimeInterval::default();
        let raster2d = Raster2D::new(
            dim.into(),
            data,
            None,
            temporal_bounds,
            geo_transform.into(),
        )
        .unwrap();
        assert_eq!(raster2d.pixel_value_at_grid_index(&tuple_index).unwrap(), 4);
    }

    #[test]
    fn simple_raster_2d_at_arr() {
        let index = [1, 1];

        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        let geo_transform = [1.0, 1.0, 0.0, 1.0, 0.0, 1.0];
        let temporal_bounds: TimeInterval = TimeInterval::default();
        let raster2d = Raster2D::new(
            dim.into(),
            data,
            None,
            temporal_bounds,
            geo_transform.into(),
        )
        .unwrap();
        let value = raster2d.pixel_value_at_grid_index(&index).unwrap();
        assert_eq!(value, 4);
    }

    #[test]
    fn simple_raster_2d_set_at_tuple() {
        let tuple_index = [1, 1];

        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        let geo_transform = [1.0, 1.0, 0.0, 1.0, 0.0, 1.0];
        let temporal_bounds: TimeInterval = TimeInterval::default();
        let mut raster2d = Raster2D::new(
            dim.into(),
            data,
            None,
            temporal_bounds,
            geo_transform.into(),
        )
        .unwrap();

        raster2d
            .set_pixel_value_at_grid_index(&tuple_index, 9)
            .unwrap();
        let value = raster2d.pixel_value_at_grid_index(&tuple_index).unwrap();
        assert_eq!(value, 9);
        assert_eq!(raster2d.data_container, [1, 2, 3, 9, 5, 6]);
    }
}
