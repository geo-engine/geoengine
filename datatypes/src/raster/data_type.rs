#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
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

// TODO: use a macro?
pub trait StaticRasterDataType {
    fn raster_data_type() -> RasterDataType;
}

impl StaticRasterDataType for u8 {
    fn raster_data_type() -> RasterDataType {
        RasterDataType::U8
    }
}

impl StaticRasterDataType for u16 {
    fn raster_data_type() -> RasterDataType {
        RasterDataType::U16
    }
}

impl StaticRasterDataType for u32 {
    fn raster_data_type() -> RasterDataType {
        RasterDataType::U32
    }
}

impl StaticRasterDataType for u64 {
    fn raster_data_type() -> RasterDataType {
        RasterDataType::U64
    }
}

impl StaticRasterDataType for i8 {
    fn raster_data_type() -> RasterDataType {
        RasterDataType::I8
    }
}

impl StaticRasterDataType for i16 {
    fn raster_data_type() -> RasterDataType {
        RasterDataType::I16
    }
}

impl StaticRasterDataType for i32 {
    fn raster_data_type() -> RasterDataType {
        RasterDataType::I32
    }
}

impl StaticRasterDataType for i64 {
    fn raster_data_type() -> RasterDataType {
        RasterDataType::I64
    }
}

impl StaticRasterDataType for f32 {
    fn raster_data_type() -> RasterDataType {
        RasterDataType::F32
    }
}

impl StaticRasterDataType for f64 {
    fn raster_data_type() -> RasterDataType {
        RasterDataType::F64
    }
}

pub trait DynamicRasterDataType {
    fn raster_data_type(&self) -> RasterDataType;
}

impl<R> DynamicRasterDataType for R
where
    R: StaticRasterDataType,
{
    fn raster_data_type(&self) -> RasterDataType {
        R::raster_data_type()
    }
}
