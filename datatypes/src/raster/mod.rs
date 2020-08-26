mod base_raster;
mod data_type;
mod geo_transform;
mod grid_dimension;
mod operations;
mod raster_tile;
mod typed_raster;

pub use self::base_raster::{BaseRaster, Raster2D, Raster3D};
pub use self::data_type::{
    DynamicRasterDataType, FromPrimitive, Pixel, RasterDataType, StaticRasterDataType, TypedValue,
};
pub use self::geo_transform::{GdalGeoTransform, GeoTransform};
pub use self::grid_dimension::{Dim, GridDimension, GridIndex, Ix};
pub use self::operations::blit::Blit;
pub use self::typed_raster::{TypedRaster2D, TypedRaster3D};
use super::primitives::{SpatialBounded, TemporalBounded};
use crate::util::Result;
pub use raster_tile::*;
use std::fmt::Debug;

pub trait GenericRaster: Send + Debug {
    // TODO: make data accessible
    fn get(&self);
}

pub trait Raster<D: GridDimension, T: Pixel, C>: SpatialBounded + TemporalBounded {
    /// returns the grid dimension object of type D: `GridDimension`
    fn dimension(&self) -> &D;
    /// returns the optional  no-data value used for the raster
    fn no_data_value(&self) -> Option<T>;
    /// returns a reference to the data container used to hold the pixels / cells of the raster
    fn data_container(&self) -> &C;
    /// returns a reference to the geo transform describing the origin of the raster and the pixel size
    fn geo_transform(&self) -> &GeoTransform;
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
    /// use $crate::raster::{Raster, Dim, Raster2D, GridPixelAccess};
    /// use $crate::primitives::TimeInterval;
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

pub trait GridPixelAccessMut<T, I>
where
    T: Pixel,
{
    /// Sets the value at a pixels location
    ///
    /// # Examples
    ///
    /// ```
    /// use $crate::raster::{Raster, Dim, Raster2D, GridPixelAccessMut};
    /// use $crate::primitives::TimeInterval;
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

/// Maps a `TypedRaster2D` to another `TypedRaster2D` by calling a function on all its variants.
/// Call via `map_generic_raster2d!(input, raster => function)`.
#[macro_export]
macro_rules! map_generic_raster2d {
    ($input_raster:expr, $raster:ident => $function_call:expr) => {
        map_generic_raster2d!(
            @variants $input_raster, $raster => $function_call,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $input_raster:expr, $raster:ident => $function_call:expr, $($variant:tt),+) => {
        match $input_raster {
            $(
                $crate::raster::TypedRaster2D::$variant($raster) => {
                    $crate::raster::TypedRaster2D::$variant($function_call)
                }
            )+
        }
    };
}

/// Calls a function on a `TypedRaster2D`  by calling it on all its variants.
/// Call via `call_generic_raster2d!(input, raster => function)`.
#[macro_export]
macro_rules! call_generic_raster2d {
    ($input_raster:expr, $raster:ident => $function_call:expr) => {
        call_generic_raster2d!(
            @variants $input_raster, $raster => $function_call,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $input_raster:expr, $raster:ident => $function_call:expr, $($variant:tt),+) => {
        match $input_raster {
            $(
                $crate::raster::TypedRaster2D::$variant($raster) => $function_call,
            )+
        }
    };
}

/// Generates a a `TypedRaster2D` by calling a function for all variants.
/// Call via `generate_generic_raster2d!(type, function)`.
#[macro_export]
macro_rules! generate_generic_raster2d {
    ($type_enum:expr, $function_call:expr) => {
        generate_generic_raster2d!(
            @variants $type_enum, $function_call,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $type_enum:expr, $function_call:expr, $($variant:tt),+) => {
        match $type_enum {
            $(
                $crate::raster::RasterDataType::$variant => {
                    crate::raster::TypedRaster2D::$variant($function_call)
                }
            )+
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_generic_raster2d() {
        fn map<T: Pixel>(raster: Raster2D<T>) -> Raster2D<T> {
            raster
        }

        let typed_raster = TypedRaster2D::U32(
            Raster2D::new(
                [3, 2].into(),
                vec![1, 2, 3, 4, 5, 6],
                None,
                Default::default(),
                [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
            )
            .unwrap(),
        );

        assert_eq!(
            typed_raster,
            map_generic_raster2d!(typed_raster.clone(), raster => map(raster))
        );
    }

    #[test]
    fn call_generic_raster2d() {
        fn first_pixel<T: Pixel>(raster: &Raster2D<T>) -> i64 {
            raster.pixel_value_at_grid_index(&(0, 0)).unwrap().as_()
        }

        let typed_raster = TypedRaster2D::U32(
            Raster2D::new(
                [3, 2].into(),
                vec![1, 2, 3, 4, 5, 6],
                None,
                Default::default(),
                [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
            )
            .unwrap(),
        );

        assert_eq!(
            call_generic_raster2d!(typed_raster.clone(), _raster => 2),
            2
        );

        assert_eq!(
            call_generic_raster2d!(typed_raster, raster => first_pixel(&raster)),
            1
        );
    }

    #[test]
    fn generate_generic_raster2d() {
        fn generate<T: Pixel>() -> Raster2D<T> {
            let data: Vec<T> = vec![
                T::from_(1),
                T::from_(2),
                T::from_(3),
                T::from_(4),
                T::from_(5),
                T::from_(6),
            ];

            Raster2D::new(
                [3, 2].into(),
                data,
                None,
                Default::default(),
                [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
            )
            .unwrap()
        }

        assert_eq!(
            generate_generic_raster2d!(RasterDataType::U8, generate()),
            TypedRaster2D::U8(
                Raster2D::new(
                    [3, 2].into(),
                    vec![1, 2, 3, 4, 5, 6],
                    None,
                    Default::default(),
                    [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
                )
                .unwrap(),
            )
        );
    }
}
