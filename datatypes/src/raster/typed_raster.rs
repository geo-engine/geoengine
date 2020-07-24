use crate::raster::Raster2D;

#[derive(Debug)]
pub enum TypedRaster2D {
    U8(Raster2D<u8>),
    U16(Raster2D<u16>),
    U32(Raster2D<u32>),
    U64(Raster2D<u64>),
    I8(Raster2D<i8>),
    I16(Raster2D<i16>),
    I32(Raster2D<i32>),
    I64(Raster2D<i64>),
    F32(Raster2D<f32>),
    F64(Raster2D<f64>),
}
