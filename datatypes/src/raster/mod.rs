mod geo_transform;
mod grid_dimension;
mod raster;
pub use self::geo_transform::{GdalGeoTransform, GeoTransform};
pub use self::grid_dimension::{Dim, GridDimension, GridIndex};
pub use self::raster::{
    BaseRaster, GridPixelAccess, GridPixelAccessMut, Raster, SimpleRaster2d, SimpleRaster3d,
};
pub use super::primitives::{BoundingBox2D, SpatialBounded, TemporalBounded, TimeInterval};

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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
