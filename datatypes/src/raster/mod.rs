mod base_raster;
mod geo_transform;
mod grid_dimension;
pub use self::base_raster::{BaseRaster, Blit, Raster2D, Raster3D};
pub use self::geo_transform::{GdalGeoTransform, GeoTransform};
pub use self::grid_dimension::{Dim, GridDimension, GridIndex, Ix};
use super::primitives::{SpatialBounded, TemporalBounded};
use crate::util::Result;
use std::fmt::Debug;

pub trait GenericRaster: Send + Debug {
    // TODO: make data accessible
    fn get(&self);
}

pub trait Raster<D: GridDimension, T: Copy, C: Capacity>: SpatialBounded + TemporalBounded {
    /// returns the grid dimension object of type D: `GridDimension`
    fn dimension(&self) -> &D;
    /// returns the optional  no-data value used for the raster
    fn no_data_value(&self) -> Option<T>;
    /// returns a reference to the data container used to hold the pixels / cells of the raster
    fn data_container(&self) -> &C;
    /// returns a reference to the geo transform describing the origin of the raster and the pixel size
    fn geo_transform(&self) -> &GeoTransform;
}

pub trait GridPixelAccess<T, I> {
    /// Gets the value at a pixels location
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::raster::{Raster, Dim, Raster2D, GridPixelAccess};
    /// use geoengine_datatypes::primitives::TimeInterval;
    ///
    /// let mut raster2d = Raster2D::new(
    ///    [3, 2].into(),
    ///    vec![1,2,3,4,5,6],
    ///    None,
    ///    TimeInterval::default(),
    ///    [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
    /// ).unwrap();
    /// let value = raster2d.pixel_value_at_grid_index(&(1, 1)).unwrap();
    /// assert_eq!(value, 4);
    /// ```
    ///
    /// # Errors
    ///
    /// The method fails if the grid index is out of bounds.
    ///
    fn pixel_value_at_grid_index(&self, grid_index: &I) -> Result<T>;
}

pub trait GridPixelAccessMut<T, I> {
    /// Sets the value at a pixels location
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::raster::{Raster, Dim, Raster2D, GridPixelAccessMut};
    /// use geoengine_datatypes::primitives::TimeInterval;
    ///
    /// let mut raster2d = Raster2D::new(
    ///    [3, 2].into(),
    ///    vec![1,2,3,4,5,6],
    ///    None,
    ///    TimeInterval::default(),
    ///    [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
    /// ).unwrap();
    /// raster2d.set_pixel_value_at_grid_index(&(1, 1), 9).unwrap();
    /// assert_eq!(raster2d.data_container(), &[1,2,3,9,5,6]);
    /// ```
    ///
    /// # Errors
    ///
    /// The method fails if the grid index is out of bounds.
    ///
    fn set_pixel_value_at_grid_index(&mut self, grid_index: &I, value: T) -> Result<()>;
}

pub trait CoordinatePixelAccess<T> {
    fn pixel_value_at_coord(&self, coordinate: (f64, f64)) -> T;
}

pub trait Capacity {
    fn capacity(&self) -> usize;
}

impl<T> Capacity for [T] {
    fn capacity(&self) -> usize {
        self.len()
    }
}
impl<T> Capacity for &[T] {
    fn capacity(&self) -> usize {
        self.len()
    }
}
impl<T> Capacity for [T; 1] {
    fn capacity(&self) -> usize {
        self.len()
    }
}
impl<T> Capacity for [T; 2] {
    fn capacity(&self) -> usize {
        self.len()
    }
}
impl<T> Capacity for [T; 3] {
    fn capacity(&self) -> usize {
        self.len()
    }
}
impl<T> Capacity for Vec<T> {
    fn capacity(&self) -> usize {
        self.len()
    }
}
