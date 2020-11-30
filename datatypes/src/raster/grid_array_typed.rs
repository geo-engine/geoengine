use super::{
    DynamicRasterDataType, GridArray, GridShape2D, GridSize, GridSpaceToLinearSpace,
    RasterDataType, TypedRasterConversion,
};
use crate::raster::Pixel;
use std::convert::TryFrom;

pub type TypedGridArray2D = TypedGridArray<GridShape2D>;
pub type TypedGridArray3D = TypedGridArray<GridShape2D>;

#[derive(Clone, Debug, PartialEq)]
pub enum TypedGridArray<D: GridSize> {
    U8(GridArray<D, u8>),
    U16(GridArray<D, u16>),
    U32(GridArray<D, u32>),
    U64(GridArray<D, u64>),
    I8(GridArray<D, i8>),
    I16(GridArray<D, i16>),
    I32(GridArray<D, i32>),
    I64(GridArray<D, i64>),
    F32(GridArray<D, f32>),
    F64(GridArray<D, f64>),
}

// TODO: use a macro?
impl<D> TypedGridArray<D>
where
    D: GridSize,
{
    pub fn get_u8(self) -> Option<GridArray<D, u8>> {
        if let TypedGridArray::U8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u16(self) -> Option<GridArray<D, u16>> {
        if let TypedGridArray::U16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u32(self) -> Option<GridArray<D, u32>> {
        if let TypedGridArray::U32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u64(self) -> Option<GridArray<D, u64>> {
        if let TypedGridArray::U64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i8(self) -> Option<GridArray<D, i8>> {
        if let TypedGridArray::I8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i16(self) -> Option<GridArray<D, i16>> {
        if let TypedGridArray::I16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i32(self) -> Option<GridArray<D, i32>> {
        if let TypedGridArray::I32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i64(self) -> Option<GridArray<D, i64>> {
        if let TypedGridArray::I64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f32(self) -> Option<GridArray<D, f32>> {
        if let TypedGridArray::F32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f64(self) -> Option<GridArray<D, f64>> {
        if let TypedGridArray::F64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u8_ref(&self) -> Option<&GridArray<D, u8>> {
        if let TypedGridArray::U8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u16_ref(&self) -> Option<&GridArray<D, u16>> {
        if let TypedGridArray::U16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u32_ref(&self) -> Option<&GridArray<D, u32>> {
        if let TypedGridArray::U32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u64_ref(&self) -> Option<&GridArray<D, u64>> {
        if let TypedGridArray::U64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i8_ref(&self) -> Option<&GridArray<D, i8>> {
        if let TypedGridArray::I8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i16_ref(&self) -> Option<&GridArray<D, i16>> {
        if let TypedGridArray::I16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i32_ref(&self) -> Option<&GridArray<D, i32>> {
        if let TypedGridArray::I32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i64_ref(&self) -> Option<&GridArray<D, i64>> {
        if let TypedGridArray::I64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f32_ref(&self) -> Option<&GridArray<D, f32>> {
        if let TypedGridArray::F32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f64_ref(&self) -> Option<&GridArray<D, f64>> {
        if let TypedGridArray::F64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u8_ref_mut(&mut self) -> Option<&mut GridArray<D, u8>> {
        if let TypedGridArray::U8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u16_ref_mut(&mut self) -> Option<&mut GridArray<D, u16>> {
        if let TypedGridArray::U16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u32_ref_mut(&mut self) -> Option<&mut GridArray<D, u32>> {
        if let TypedGridArray::U32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u64_ref_mut(&mut self) -> Option<&mut GridArray<D, u64>> {
        if let TypedGridArray::U64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i8_ref_mut(&mut self) -> Option<&mut GridArray<D, i8>> {
        if let TypedGridArray::I8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i16_ref_mut(&mut self) -> Option<&mut GridArray<D, i16>> {
        if let TypedGridArray::I16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i32_ref_mut(&mut self) -> Option<&mut GridArray<D, i32>> {
        if let TypedGridArray::I32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i64_ref_mut(&mut self) -> Option<&mut GridArray<D, i64>> {
        if let TypedGridArray::I64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f32_ref_mut(&mut self) -> Option<&mut GridArray<D, f32>> {
        if let TypedGridArray::F32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f64_ref_mut(&mut self) -> Option<&mut GridArray<D, f64>> {
        if let TypedGridArray::F64(r) = self {
            return Some(r);
        }
        None
    }
}

impl<D, T> Into<TypedGridArray<D>> for GridArray<D, T>
where
    D: GridSize + GridSpaceToLinearSpace,
    T: Pixel + TypedRasterConversion<D>,
{
    fn into(self) -> TypedGridArray<D> {
        T::get_typed_raster(self)
    }
}

impl<D, T> TryFrom<TypedGridArray<D>> for GridArray<D, T>
where
    D: GridSize + GridSpaceToLinearSpace,
    T: Pixel + TypedRasterConversion<D>,
{
    type Error = crate::error::Error;

    fn try_from(raster: TypedGridArray<D>) -> Result<Self, Self::Error> {
        T::get_raster(raster).ok_or(crate::error::Error::InvalidTypedGridArrayConversion)
    }
}

impl<D> DynamicRasterDataType for TypedGridArray<D>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    fn raster_data_type(&self) -> RasterDataType {
        match self {
            TypedGridArray::U8(_) => RasterDataType::U8,
            TypedGridArray::U16(_) => RasterDataType::U16,
            TypedGridArray::U32(_) => RasterDataType::U32,
            TypedGridArray::U64(_) => RasterDataType::U64,
            TypedGridArray::I8(_) => RasterDataType::I8,
            TypedGridArray::I16(_) => RasterDataType::I16,
            TypedGridArray::I32(_) => RasterDataType::I32,
            TypedGridArray::I64(_) => RasterDataType::I64,
            TypedGridArray::F32(_) => RasterDataType::F32,
            TypedGridArray::F64(_) => RasterDataType::F64,
        }
    }
}
