use super::{
    BaseRaster, Dim, DynamicRasterDataType, GridDimension, RasterDataType, StaticRasterDataType,
};

pub type TypedRaster2D = TypedRasterNDim<Dim<[usize; 2]>>;
pub type TypedRaster3D = TypedRasterNDim<Dim<[usize; 3]>>;

#[derive(Debug)]
pub enum TypedRasterNDim<D: GridDimension> {
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
impl<D> TypedRasterNDim<D>
where
    D: GridDimension,
{
    pub fn get_u8(self) -> Option<BaseRaster<D, u8, Vec<u8>>> {
        if let TypedRasterNDim::U8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u16(self) -> Option<BaseRaster<D, u16, Vec<u16>>> {
        if let TypedRasterNDim::U16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u32(self) -> Option<BaseRaster<D, u32, Vec<u32>>> {
        if let TypedRasterNDim::U32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u64(self) -> Option<BaseRaster<D, u64, Vec<u64>>> {
        if let TypedRasterNDim::U64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i8(self) -> Option<BaseRaster<D, i8, Vec<i8>>> {
        if let TypedRasterNDim::I8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i16(self) -> Option<BaseRaster<D, i16, Vec<i16>>> {
        if let TypedRasterNDim::I16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i32(self) -> Option<BaseRaster<D, i32, Vec<i32>>> {
        if let TypedRasterNDim::I32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i64(self) -> Option<BaseRaster<D, i64, Vec<i64>>> {
        if let TypedRasterNDim::I64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f32(self) -> Option<BaseRaster<D, f32, Vec<f32>>> {
        if let TypedRasterNDim::F32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f64(self) -> Option<BaseRaster<D, f64, Vec<f64>>> {
        if let TypedRasterNDim::F64(r) = self {
            return Some(r);
        }
        None
    }
}

// TODO: use a macro?
impl<D> Into<TypedRasterNDim<D>> for BaseRaster<D, u8, Vec<u8>>
where
    D: GridDimension,
{
    fn into(self) -> TypedRasterNDim<D> {
        TypedRasterNDim::U8(self)
    }
}

impl<D> Into<TypedRasterNDim<D>> for BaseRaster<D, u16, Vec<u16>>
where
    D: GridDimension,
{
    fn into(self) -> TypedRasterNDim<D> {
        TypedRasterNDim::U16(self)
    }
}

impl<D> Into<TypedRasterNDim<D>> for BaseRaster<D, u32, Vec<u32>>
where
    D: GridDimension,
{
    fn into(self) -> TypedRasterNDim<D> {
        TypedRasterNDim::U32(self)
    }
}

impl<D> Into<TypedRasterNDim<D>> for BaseRaster<D, u64, Vec<u64>>
where
    D: GridDimension,
{
    fn into(self) -> TypedRasterNDim<D> {
        TypedRasterNDim::U64(self)
    }
}

impl<D> Into<TypedRasterNDim<D>> for BaseRaster<D, i8, Vec<i8>>
where
    D: GridDimension,
{
    fn into(self) -> TypedRasterNDim<D> {
        TypedRasterNDim::I8(self)
    }
}

impl<D> Into<TypedRasterNDim<D>> for BaseRaster<D, i16, Vec<i16>>
where
    D: GridDimension,
{
    fn into(self) -> TypedRasterNDim<D> {
        TypedRasterNDim::I16(self)
    }
}

impl<D> Into<TypedRasterNDim<D>> for BaseRaster<D, i32, Vec<i32>>
where
    D: GridDimension,
{
    fn into(self) -> TypedRasterNDim<D> {
        TypedRasterNDim::I32(self)
    }
}

impl<D> Into<TypedRasterNDim<D>> for BaseRaster<D, i64, Vec<i64>>
where
    D: GridDimension,
{
    fn into(self) -> TypedRasterNDim<D> {
        TypedRasterNDim::I64(self)
    }
}

impl<D> Into<TypedRasterNDim<D>> for BaseRaster<D, f32, Vec<f32>>
where
    D: GridDimension,
{
    fn into(self) -> TypedRasterNDim<D> {
        TypedRasterNDim::F32(self)
    }
}

impl<D> Into<TypedRasterNDim<D>> for BaseRaster<D, f64, Vec<f64>>
where
    D: GridDimension,
{
    fn into(self) -> TypedRasterNDim<D> {
        TypedRasterNDim::F64(self)
    }
}

impl<D> DynamicRasterDataType for TypedRasterNDim<D>
where
    D: GridDimension,
{
    fn raster_data_type(&self) -> RasterDataType {
        match self {
            TypedRasterNDim::U8(_) => RasterDataType::U8,
            TypedRasterNDim::U16(_) => RasterDataType::U16,
            TypedRasterNDim::U32(_) => RasterDataType::U32,
            TypedRasterNDim::U64(_) => RasterDataType::U64,
            TypedRasterNDim::I8(_) => RasterDataType::I8,
            TypedRasterNDim::I16(_) => RasterDataType::I16,
            TypedRasterNDim::I32(_) => RasterDataType::I32,
            TypedRasterNDim::I64(_) => RasterDataType::I64,
            TypedRasterNDim::F32(_) => RasterDataType::F32,
            TypedRasterNDim::F64(_) => RasterDataType::F64,
        }
    }
}
