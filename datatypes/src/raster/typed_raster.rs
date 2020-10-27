use super::{
    BaseRaster, Dim2D, Dim3D, DynamicRasterDataType, GridDimension, RasterDataType,
    TypedRasterConversion,
};
use crate::raster::Pixel;
use std::convert::TryFrom;

pub type TypedRaster2D = TypedRaster<Dim2D>;
pub type TypedRaster3D = TypedRaster<Dim3D>;

#[derive(Clone, Debug, PartialEq)]
pub enum TypedRaster<D: GridDimension> {
    U8(BaseRaster<D, u8, Vec<u8>>),
    U16(BaseRaster<D, u16, Vec<u16>>),
    U32(BaseRaster<D, u32, Vec<u32>>),
    U64(BaseRaster<D, u64, Vec<u64>>),
    I8(BaseRaster<D, i8, Vec<i8>>),
    I16(BaseRaster<D, i16, Vec<i16>>),
    I32(BaseRaster<D, i32, Vec<i32>>),
    I64(BaseRaster<D, i64, Vec<i64>>),
    F32(BaseRaster<D, f32, Vec<f32>>),
    F64(BaseRaster<D, f64, Vec<f64>>),
}

// TODO: use a macro?
impl<D> TypedRaster<D>
where
    D: GridDimension,
{
    pub fn get_u8(self) -> Option<BaseRaster<D, u8, Vec<u8>>> {
        if let TypedRaster::U8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u16(self) -> Option<BaseRaster<D, u16, Vec<u16>>> {
        if let TypedRaster::U16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u32(self) -> Option<BaseRaster<D, u32, Vec<u32>>> {
        if let TypedRaster::U32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u64(self) -> Option<BaseRaster<D, u64, Vec<u64>>> {
        if let TypedRaster::U64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i8(self) -> Option<BaseRaster<D, i8, Vec<i8>>> {
        if let TypedRaster::I8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i16(self) -> Option<BaseRaster<D, i16, Vec<i16>>> {
        if let TypedRaster::I16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i32(self) -> Option<BaseRaster<D, i32, Vec<i32>>> {
        if let TypedRaster::I32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i64(self) -> Option<BaseRaster<D, i64, Vec<i64>>> {
        if let TypedRaster::I64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f32(self) -> Option<BaseRaster<D, f32, Vec<f32>>> {
        if let TypedRaster::F32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f64(self) -> Option<BaseRaster<D, f64, Vec<f64>>> {
        if let TypedRaster::F64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u8_ref(&self) -> Option<&BaseRaster<D, u8, Vec<u8>>> {
        if let TypedRaster::U8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u16_ref(&self) -> Option<&BaseRaster<D, u16, Vec<u16>>> {
        if let TypedRaster::U16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u32_ref(&self) -> Option<&BaseRaster<D, u32, Vec<u32>>> {
        if let TypedRaster::U32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u64_ref(&self) -> Option<&BaseRaster<D, u64, Vec<u64>>> {
        if let TypedRaster::U64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i8_ref(&self) -> Option<&BaseRaster<D, i8, Vec<i8>>> {
        if let TypedRaster::I8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i16_ref(&self) -> Option<&BaseRaster<D, i16, Vec<i16>>> {
        if let TypedRaster::I16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i32_ref(&self) -> Option<&BaseRaster<D, i32, Vec<i32>>> {
        if let TypedRaster::I32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i64_ref(&self) -> Option<&BaseRaster<D, i64, Vec<i64>>> {
        if let TypedRaster::I64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f32_ref(&self) -> Option<&BaseRaster<D, f32, Vec<f32>>> {
        if let TypedRaster::F32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f64_ref(&self) -> Option<&BaseRaster<D, f64, Vec<f64>>> {
        if let TypedRaster::F64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u8_ref_mut(&mut self) -> Option<&mut BaseRaster<D, u8, Vec<u8>>> {
        if let TypedRaster::U8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u16_ref_mut(&mut self) -> Option<&mut BaseRaster<D, u16, Vec<u16>>> {
        if let TypedRaster::U16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u32_ref_mut(&mut self) -> Option<&mut BaseRaster<D, u32, Vec<u32>>> {
        if let TypedRaster::U32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u64_ref_mut(&mut self) -> Option<&mut BaseRaster<D, u64, Vec<u64>>> {
        if let TypedRaster::U64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i8_ref_mut(&mut self) -> Option<&mut BaseRaster<D, i8, Vec<i8>>> {
        if let TypedRaster::I8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i16_ref_mut(&mut self) -> Option<&mut BaseRaster<D, i16, Vec<i16>>> {
        if let TypedRaster::I16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i32_ref_mut(&mut self) -> Option<&mut BaseRaster<D, i32, Vec<i32>>> {
        if let TypedRaster::I32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i64_ref_mut(&mut self) -> Option<&mut BaseRaster<D, i64, Vec<i64>>> {
        if let TypedRaster::I64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f32_ref_mut(&mut self) -> Option<&mut BaseRaster<D, f32, Vec<f32>>> {
        if let TypedRaster::F32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f64_ref_mut(&mut self) -> Option<&mut BaseRaster<D, f64, Vec<f64>>> {
        if let TypedRaster::F64(r) = self {
            return Some(r);
        }
        None
    }
}

impl<D, T> Into<TypedRaster<D>> for BaseRaster<D, T, Vec<T>>
where
    D: GridDimension,
    T: Pixel + TypedRasterConversion<D>,
{
    fn into(self) -> TypedRaster<D> {
        T::get_typed_raster(self)
    }
}

impl<D, T> TryFrom<TypedRaster<D>> for BaseRaster<D, T, Vec<T>>
where
    D: GridDimension,
    T: Pixel + TypedRasterConversion<D>,
{
    type Error = crate::error::Error;

    fn try_from(raster: TypedRaster<D>) -> Result<Self, Self::Error> {
        T::get_raster(raster).ok_or(crate::error::Error::InvalidTypedRasterConversion)
    }
}

impl<D> DynamicRasterDataType for TypedRaster<D>
where
    D: GridDimension,
{
    fn raster_data_type(&self) -> RasterDataType {
        match self {
            TypedRaster::U8(_) => RasterDataType::U8,
            TypedRaster::U16(_) => RasterDataType::U16,
            TypedRaster::U32(_) => RasterDataType::U32,
            TypedRaster::U64(_) => RasterDataType::U64,
            TypedRaster::I8(_) => RasterDataType::I8,
            TypedRaster::I16(_) => RasterDataType::I16,
            TypedRaster::I32(_) => RasterDataType::I32,
            TypedRaster::I64(_) => RasterDataType::I64,
            TypedRaster::F32(_) => RasterDataType::F32,
            TypedRaster::F64(_) => RasterDataType::F64,
        }
    }
}
