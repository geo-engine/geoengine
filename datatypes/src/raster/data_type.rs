use crate::error;
use crate::operations::image::RgbaTransmutable;
use crate::raster::TypedRasterConversion;
use num_traits::{AsPrimitive, Bounded, Num};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

use super::{GridShape2D, GridShape3D};

/// A collection of required traits for a pixel type
pub trait Pixel:
    'static
    + Copy
    + std::fmt::Debug
    + Sync
    + Send
    + Num
    + Bounded
    + PartialOrd
    + AsPrimitive<u8>
    + AsPrimitive<i8>
    + AsPrimitive<u16>
    + AsPrimitive<i16>
    + AsPrimitive<u32>
    + AsPrimitive<i32>
    + AsPrimitive<u64>
    + AsPrimitive<i64>
    + AsPrimitive<f32>
    + AsPrimitive<f64>
    + AsPrimitive<Self>
    + FromPrimitive<u8>
    + FromPrimitive<i8>
    + FromPrimitive<u16>
    + FromPrimitive<i16>
    + FromPrimitive<u32>
    + FromPrimitive<i32>
    + FromPrimitive<u64>
    + FromPrimitive<i64>
    + FromPrimitive<f32>
    + FromPrimitive<f64>
    + FromPrimitive<Self>
    + StaticRasterDataType
    + RgbaTransmutable
    + TypedRasterConversion<GridShape2D>
    + TypedRasterConversion<GridShape3D>
    + DefaultNoDataValue
{
}

pub trait FromPrimitive<T>
where
    T: Pixel,
{
    fn from_(value: T) -> Self;
}

impl<T, V> FromPrimitive<V> for T
where
    T: Pixel,
    V: Pixel + AsPrimitive<T>,
{
    fn from_(value: V) -> Self {
        value.as_()
    }
}

impl Pixel for u8 {}
impl Pixel for i8 {}
impl Pixel for u16 {}
impl Pixel for i16 {}
impl Pixel for u32 {}
impl Pixel for i32 {}
impl Pixel for u64 {}
impl Pixel for i64 {}
impl Pixel for f32 {}
impl Pixel for f64 {}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Deserialize, Serialize, Copy, Clone)]
pub enum RasterDataType {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
}

impl RasterDataType {
    /// Returns true if the given `value` is valid for the `RasterDataType` variant,
    /// i.e. it can be represented by a variable of the corresponding primitive data type
    #[allow(clippy::float_cmp)]
    #[allow(clippy::cast_lossless)]
    pub fn is_valid(self, value: f64) -> bool {
        match self {
            RasterDataType::U8 => value as u8 as f64 == value,
            RasterDataType::U16 => value as u16 as f64 == value,
            RasterDataType::U32 => value as u32 as f64 == value,
            RasterDataType::U64 => value as u64 as f64 == value,
            RasterDataType::I8 => value as i8 as f64 == value,
            RasterDataType::I16 => value as i16 as f64 == value,
            RasterDataType::I32 => value as i32 as f64 == value,
            RasterDataType::I64 => value as i64 as f64 == value,
            RasterDataType::F32 => value.is_nan() || value as f32 as f64 == value,
            RasterDataType::F64 => true,
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Copy, Clone)]
pub enum TypedValue {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
}

impl TryFrom<TypedValue> for u8 {
    type Error = crate::error::Error;

    fn try_from(value: TypedValue) -> Result<Self, Self::Error> {
        if let TypedValue::U8(v) = value {
            return Ok(v);
        }
        Err(error::Error::InvalidTypedValueConversion)
    }
}

impl TryFrom<TypedValue> for u16 {
    type Error = crate::error::Error;

    fn try_from(value: TypedValue) -> Result<Self, Self::Error> {
        if let TypedValue::U16(v) = value {
            return Ok(v);
        }
        Err(error::Error::InvalidTypedValueConversion)
    }
}

impl TryFrom<TypedValue> for u32 {
    type Error = crate::error::Error;

    fn try_from(value: TypedValue) -> Result<Self, Self::Error> {
        if let TypedValue::U32(v) = value {
            return Ok(v);
        }
        Err(error::Error::InvalidTypedValueConversion)
    }
}

impl TryFrom<TypedValue> for u64 {
    type Error = crate::error::Error;

    fn try_from(value: TypedValue) -> Result<Self, Self::Error> {
        if let TypedValue::U64(v) = value {
            return Ok(v);
        }
        Err(error::Error::InvalidTypedValueConversion)
    }
}

impl TryFrom<TypedValue> for i8 {
    type Error = crate::error::Error;

    fn try_from(value: TypedValue) -> Result<Self, Self::Error> {
        if let TypedValue::I8(v) = value {
            return Ok(v);
        }
        Err(error::Error::InvalidTypedValueConversion)
    }
}

impl TryFrom<TypedValue> for i16 {
    type Error = crate::error::Error;

    fn try_from(value: TypedValue) -> Result<Self, Self::Error> {
        if let TypedValue::I16(v) = value {
            return Ok(v);
        }
        Err(error::Error::InvalidTypedValueConversion)
    }
}

impl TryFrom<TypedValue> for i32 {
    type Error = crate::error::Error;

    fn try_from(value: TypedValue) -> Result<Self, Self::Error> {
        if let TypedValue::I32(v) = value {
            return Ok(v);
        }
        Err(error::Error::InvalidTypedValueConversion)
    }
}

impl TryFrom<TypedValue> for i64 {
    type Error = crate::error::Error;

    fn try_from(value: TypedValue) -> Result<Self, Self::Error> {
        if let TypedValue::I64(v) = value {
            return Ok(v);
        }
        Err(error::Error::InvalidTypedValueConversion)
    }
}

impl TryFrom<TypedValue> for f32 {
    type Error = crate::error::Error;

    fn try_from(value: TypedValue) -> Result<Self, Self::Error> {
        if let TypedValue::F32(v) = value {
            return Ok(v);
        }
        Err(error::Error::InvalidTypedValueConversion)
    }
}

impl TryFrom<TypedValue> for f64 {
    type Error = crate::error::Error;

    fn try_from(value: TypedValue) -> Result<Self, Self::Error> {
        if let TypedValue::F64(v) = value {
            return Ok(v);
        }
        Err(error::Error::InvalidTypedValueConversion)
    }
}

// TODO: use a macro?
pub trait StaticRasterDataType: Copy + Default + 'static {
    const TYPE: RasterDataType;
}

impl StaticRasterDataType for u8 {
    const TYPE: RasterDataType = RasterDataType::U8;
}

impl StaticRasterDataType for u16 {
    const TYPE: RasterDataType = RasterDataType::U16;
}

impl StaticRasterDataType for u32 {
    const TYPE: RasterDataType = RasterDataType::U32;
}

impl StaticRasterDataType for u64 {
    const TYPE: RasterDataType = RasterDataType::U64;
}

impl StaticRasterDataType for i8 {
    const TYPE: RasterDataType = RasterDataType::I8;
}

impl StaticRasterDataType for i16 {
    const TYPE: RasterDataType = RasterDataType::I16;
}

impl StaticRasterDataType for i32 {
    const TYPE: RasterDataType = RasterDataType::I32;
}

impl StaticRasterDataType for i64 {
    const TYPE: RasterDataType = RasterDataType::I64;
}

impl StaticRasterDataType for f32 {
    const TYPE: RasterDataType = RasterDataType::F32;
}

impl StaticRasterDataType for f64 {
    const TYPE: RasterDataType = RasterDataType::F64;
}

pub trait DynamicRasterDataType {
    fn raster_data_type(&self) -> RasterDataType;
}

impl<R> DynamicRasterDataType for R
where
    R: StaticRasterDataType,
{
    fn raster_data_type(&self) -> RasterDataType {
        R::TYPE
    }
}

pub trait DefaultNoDataValue {
    /// The value that is used as default to represent no data for a data type
    const DEFAULT_NO_DATA_VALUE: Self;
}

impl DefaultNoDataValue for u8 {
    const DEFAULT_NO_DATA_VALUE: Self = Self::MAX;
}

impl DefaultNoDataValue for u16 {
    const DEFAULT_NO_DATA_VALUE: Self = Self::MAX;
}

impl DefaultNoDataValue for u32 {
    const DEFAULT_NO_DATA_VALUE: Self = Self::MAX;
}

impl DefaultNoDataValue for u64 {
    const DEFAULT_NO_DATA_VALUE: Self = Self::MAX;
}

impl DefaultNoDataValue for i8 {
    const DEFAULT_NO_DATA_VALUE: Self = Self::MAX;
}

impl DefaultNoDataValue for i16 {
    const DEFAULT_NO_DATA_VALUE: Self = Self::MAX;
}

impl DefaultNoDataValue for i32 {
    const DEFAULT_NO_DATA_VALUE: Self = Self::MAX;
}

impl DefaultNoDataValue for i64 {
    const DEFAULT_NO_DATA_VALUE: Self = Self::MAX;
}

impl DefaultNoDataValue for f32 {
    const DEFAULT_NO_DATA_VALUE: Self = Self::NAN;
}

impl DefaultNoDataValue for f64 {
    const DEFAULT_NO_DATA_VALUE: Self = Self::NAN;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_values() {
        assert!(RasterDataType::U8.is_valid(0.0));
        assert!(RasterDataType::U8.is_valid(255.0));
        assert!(!RasterDataType::U8.is_valid(0.1));
        assert!(!RasterDataType::U8.is_valid(-1.0));

        assert!(RasterDataType::F32.is_valid(1.5));
        assert!(!RasterDataType::F32.is_valid(f64::MIN));
    }
}
