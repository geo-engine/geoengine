use super::{
    ArrayShape2D, ArrayShape3D, DynamicRasterDataType, GridSize, GridSpaceToLinearSpace,
    RasterDataType, RasterTile,
};

pub type TypedRasterTile2D = TypedRasterTile<ArrayShape2D>;
pub type TypedRasterTile3D = TypedRasterTile<ArrayShape3D>;

#[derive(Clone, Debug, PartialEq)]
pub enum TypedRasterTile<D: GridSize + GridSpaceToLinearSpace> {
    U8(RasterTile<D, u8>),
    U16(RasterTile<D, u16>),
    U32(RasterTile<D, u32>),
    U64(RasterTile<D, u64>),
    I8(RasterTile<D, i8>),
    I16(RasterTile<D, i16>),
    I32(RasterTile<D, i32>),
    I64(RasterTile<D, i64>),
    F32(RasterTile<D, f32>),
    F64(RasterTile<D, f64>),
}

// TODO: use a macro?
impl<D> TypedRasterTile<D>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    pub fn get_u8(self) -> Option<RasterTile<D, u8>> {
        if let TypedRasterTile::U8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u16(self) -> Option<RasterTile<D, u16>> {
        if let TypedRasterTile::U16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u32(self) -> Option<RasterTile<D, u32>> {
        if let TypedRasterTile::U32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_u64(self) -> Option<RasterTile<D, u64>> {
        if let TypedRasterTile::U64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i8(self) -> Option<RasterTile<D, i8>> {
        if let TypedRasterTile::I8(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i16(self) -> Option<RasterTile<D, i16>> {
        if let TypedRasterTile::I16(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i32(self) -> Option<RasterTile<D, i32>> {
        if let TypedRasterTile::I32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_i64(self) -> Option<RasterTile<D, i64>> {
        if let TypedRasterTile::I64(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f32(self) -> Option<RasterTile<D, f32>> {
        if let TypedRasterTile::F32(r) = self {
            return Some(r);
        }
        None
    }

    pub fn get_f64(self) -> Option<RasterTile<D, f64>> {
        if let TypedRasterTile::F64(r) = self {
            return Some(r);
        }
        None
    }
}

// TODO: use a macro?
impl<D> Into<TypedRasterTile<D>> for RasterTile<D, u8>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    fn into(self) -> TypedRasterTile<D> {
        TypedRasterTile::U8(self)
    }
}

impl<D> Into<TypedRasterTile<D>> for RasterTile<D, u16>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    fn into(self) -> TypedRasterTile<D> {
        TypedRasterTile::U16(self)
    }
}

impl<D> Into<TypedRasterTile<D>> for RasterTile<D, u32>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    fn into(self) -> TypedRasterTile<D> {
        TypedRasterTile::U32(self)
    }
}

impl<D> Into<TypedRasterTile<D>> for RasterTile<D, u64>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    fn into(self) -> TypedRasterTile<D> {
        TypedRasterTile::U64(self)
    }
}

impl<D> Into<TypedRasterTile<D>> for RasterTile<D, i8>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    fn into(self) -> TypedRasterTile<D> {
        TypedRasterTile::I8(self)
    }
}

impl<D> Into<TypedRasterTile<D>> for RasterTile<D, i16>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    fn into(self) -> TypedRasterTile<D> {
        TypedRasterTile::I16(self)
    }
}

impl<D> Into<TypedRasterTile<D>> for RasterTile<D, i32>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    fn into(self) -> TypedRasterTile<D> {
        TypedRasterTile::I32(self)
    }
}

impl<D> Into<TypedRasterTile<D>> for RasterTile<D, i64>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    fn into(self) -> TypedRasterTile<D> {
        TypedRasterTile::I64(self)
    }
}

impl<D> Into<TypedRasterTile<D>> for RasterTile<D, f32>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    fn into(self) -> TypedRasterTile<D> {
        TypedRasterTile::F32(self)
    }
}

impl<D> Into<TypedRasterTile<D>> for RasterTile<D, f64>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    fn into(self) -> TypedRasterTile<D> {
        TypedRasterTile::F64(self)
    }
}

impl<D> DynamicRasterDataType for TypedRasterTile<D>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    fn raster_data_type(&self) -> RasterDataType {
        match self {
            TypedRasterTile::U8(_) => RasterDataType::U8,
            TypedRasterTile::U16(_) => RasterDataType::U16,
            TypedRasterTile::U32(_) => RasterDataType::U32,
            TypedRasterTile::U64(_) => RasterDataType::U64,
            TypedRasterTile::I8(_) => RasterDataType::I8,
            TypedRasterTile::I16(_) => RasterDataType::I16,
            TypedRasterTile::I32(_) => RasterDataType::I32,
            TypedRasterTile::I64(_) => RasterDataType::I64,
            TypedRasterTile::F32(_) => RasterDataType::F32,
            TypedRasterTile::F64(_) => RasterDataType::F64,
        }
    }
}
