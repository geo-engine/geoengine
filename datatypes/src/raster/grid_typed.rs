use super::{
    DynamicRasterDataType, Grid, GridShape2D, GridSize, GridSpaceToLinearSpace, RasterDataType,
    TypedRasterConversion,
};
use crate::raster::Pixel;
use std::convert::TryFrom;

pub type TypedGrid2D = TypedGrid<GridShape2D>;
pub type TypedGrid3D = TypedGrid<GridShape2D>;

#[derive(Clone, Debug, PartialEq)]
pub enum TypedGrid<D: GridSize> {
    U8(Grid<D, u8>),
    U16(Grid<D, u16>),
    U32(Grid<D, u32>),
    U64(Grid<D, u64>),
    I8(Grid<D, i8>),
    I16(Grid<D, i16>),
    I32(Grid<D, i32>),
    I64(Grid<D, i64>),
    F32(Grid<D, f32>),
    F64(Grid<D, f64>),
}

// TODO: use a macro?
impl<D> TypedGrid<D>
where
    D: GridSize,
{
    pub fn get_u8(self) -> Option<Grid<D, u8>> {
        if let TypedGrid::U8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u16(self) -> Option<Grid<D, u16>> {
        if let TypedGrid::U16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u32(self) -> Option<Grid<D, u32>> {
        if let TypedGrid::U32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u64(self) -> Option<Grid<D, u64>> {
        if let TypedGrid::U64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i8(self) -> Option<Grid<D, i8>> {
        if let TypedGrid::I8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i16(self) -> Option<Grid<D, i16>> {
        if let TypedGrid::I16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i32(self) -> Option<Grid<D, i32>> {
        if let TypedGrid::I32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i64(self) -> Option<Grid<D, i64>> {
        if let TypedGrid::I64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f32(self) -> Option<Grid<D, f32>> {
        if let TypedGrid::F32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f64(self) -> Option<Grid<D, f64>> {
        if let TypedGrid::F64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u8_ref(&self) -> Option<&Grid<D, u8>> {
        if let TypedGrid::U8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u16_ref(&self) -> Option<&Grid<D, u16>> {
        if let TypedGrid::U16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u32_ref(&self) -> Option<&Grid<D, u32>> {
        if let TypedGrid::U32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u64_ref(&self) -> Option<&Grid<D, u64>> {
        if let TypedGrid::U64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i8_ref(&self) -> Option<&Grid<D, i8>> {
        if let TypedGrid::I8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i16_ref(&self) -> Option<&Grid<D, i16>> {
        if let TypedGrid::I16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i32_ref(&self) -> Option<&Grid<D, i32>> {
        if let TypedGrid::I32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i64_ref(&self) -> Option<&Grid<D, i64>> {
        if let TypedGrid::I64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f32_ref(&self) -> Option<&Grid<D, f32>> {
        if let TypedGrid::F32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f64_ref(&self) -> Option<&Grid<D, f64>> {
        if let TypedGrid::F64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u8_ref_mut(&mut self) -> Option<&mut Grid<D, u8>> {
        if let TypedGrid::U8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u16_ref_mut(&mut self) -> Option<&mut Grid<D, u16>> {
        if let TypedGrid::U16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u32_ref_mut(&mut self) -> Option<&mut Grid<D, u32>> {
        if let TypedGrid::U32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u64_ref_mut(&mut self) -> Option<&mut Grid<D, u64>> {
        if let TypedGrid::U64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i8_ref_mut(&mut self) -> Option<&mut Grid<D, i8>> {
        if let TypedGrid::I8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i16_ref_mut(&mut self) -> Option<&mut Grid<D, i16>> {
        if let TypedGrid::I16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i32_ref_mut(&mut self) -> Option<&mut Grid<D, i32>> {
        if let TypedGrid::I32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i64_ref_mut(&mut self) -> Option<&mut Grid<D, i64>> {
        if let TypedGrid::I64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f32_ref_mut(&mut self) -> Option<&mut Grid<D, f32>> {
        if let TypedGrid::F32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f64_ref_mut(&mut self) -> Option<&mut Grid<D, f64>> {
        if let TypedGrid::F64(r) = self {
            return Some(r);
        }
        None
    }
}

impl<D, T> Into<TypedGrid<D>> for Grid<D, T>
where
    D: GridSize + GridSpaceToLinearSpace,
    T: Pixel + TypedRasterConversion<D>,
{
    fn into(self) -> TypedGrid<D> {
        T::get_typed_raster(self)
    }
}

impl<D, T> TryFrom<TypedGrid<D>> for Grid<D, T>
where
    D: GridSize + GridSpaceToLinearSpace,
    T: Pixel + TypedRasterConversion<D>,
{
    type Error = crate::error::Error;

    fn try_from(raster: TypedGrid<D>) -> Result<Self, Self::Error> {
        T::get_raster(raster).ok_or(crate::error::Error::InvalidTypedGridConversion)
    }
}

impl<D> DynamicRasterDataType for TypedGrid<D>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    fn raster_data_type(&self) -> RasterDataType {
        match self {
            TypedGrid::U8(_) => RasterDataType::U8,
            TypedGrid::U16(_) => RasterDataType::U16,
            TypedGrid::U32(_) => RasterDataType::U32,
            TypedGrid::U64(_) => RasterDataType::U64,
            TypedGrid::I8(_) => RasterDataType::I8,
            TypedGrid::I16(_) => RasterDataType::I16,
            TypedGrid::I32(_) => RasterDataType::I32,
            TypedGrid::I64(_) => RasterDataType::I64,
            TypedGrid::F32(_) => RasterDataType::F32,
            TypedGrid::F64(_) => RasterDataType::F64,
        }
    }
}
