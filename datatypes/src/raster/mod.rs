mod base_raster;
mod data_type;
mod geo_transform;
mod grid_dimension;
mod macros_raster;
mod macros_raster_tile;
mod operations;
mod raster_tile;
mod signed_grid_dimension;
mod typed_raster;
mod typed_raster_conversion;
mod typed_raster_tile;

pub use self::base_raster::{BaseRaster, Raster2D, Raster3D};
pub use self::data_type::{
    DynamicRasterDataType, FromPrimitive, Pixel, RasterDataType, StaticRasterDataType, TypedValue,
};
pub use self::geo_transform::{GdalGeoTransform, GeoTransform};
pub use self::grid_dimension::{
    Dim, Dim1D, Dim2D, Dim3D, GridDimension, GridIdx1D, GridIdx2D, GridIdx3D, GridIndex, Idx,
};
pub use self::operations::{blit::Blit, grid_blit::GridBlit};
pub use self::signed_grid_dimension::{
    OffsetDim, OffsetDim1D, OffsetDim2D, OffsetDim3D, OffsetDimension, SignedGridIdx1D,
    SignedGridIdx2D, SignedGridIdx3D, SignedGridIndex, SignedIdx,
};
pub use self::typed_raster::{TypedRaster, TypedRaster2D, TypedRaster3D};
pub use self::typed_raster_conversion::TypedRasterConversion;
pub use self::typed_raster_tile::{TypedRasterTile2D, TypedRasterTile3D};
use super::primitives::{SpatialBounded, TemporalBounded};
use crate::util::Result;
pub use raster_tile::*;

pub trait Raster<D: GridDimension, T: Pixel, C>: SpatialBounded + TemporalBounded {
    /// returns the grid dimension object of type D: `GridDimension`
    fn dimension(&self) -> D;
    /// returns the optional  no-data value used for the raster
    fn no_data_value(&self) -> Option<T>;
    /// returns a reference to the data container used to hold the pixels / cells of the raster
    fn data_container(&self) -> &C;
    /// returns a reference to the geo transform describing the origin of the raster and the pixel size
    fn geo_transform(&self) -> GeoTransform;
}

pub trait GridPixelAccess<T, I>
where
    T: Pixel,
{
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
    /// ).unwrap();
    /// let value = raster2d.pixel_value_at_grid_index(&[1, 1].into()).unwrap();
    /// assert_eq!(value, 4);
    /// ```
    ///
    /// # Errors
    ///
    /// The method fails if the grid index is out of bounds.
    ///
    fn pixel_value_at_grid_index(&self, grid_index: &I) -> Result<T>;
}

pub trait GridPixelAccessMut<T, I>
where
    T: Pixel,
{
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
    /// ).unwrap();
    /// raster2d.set_pixel_value_at_grid_index(&[1, 1].into(), 9).unwrap();
    /// assert_eq!(raster2d.data_container, &[1,2,3,9,5,6]);
    /// ```
    ///
    /// # Errors
    ///
    /// The method fails if the grid index is out of bounds.
    ///
    fn set_pixel_value_at_grid_index(&mut self, grid_index: &I, value: T) -> Result<()>;
}

pub trait CoordinatePixelAccess<T>
where
    T: Pixel,
{
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
