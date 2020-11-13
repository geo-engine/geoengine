use geoengine_datatypes::raster::{Pixel, RasterDataType};

#[derive(Debug, Clone)]
pub enum TypedSliceRef<'s> {
    U8(&'s [u8]),
    U16(&'s [u16]),
    U32(&'s [u32]),
    U64(&'s [u64]),
    I8(&'s [i8]),
    I16(&'s [i16]),
    I32(&'s [i32]),
    I64(&'s [i64]),
    F32(&'s [f32]),
    F64(&'s [f64]),
}

#[derive(Debug)]
pub enum TypedSliceMut<'s> {
    U8(&'s mut [u8]),
    U16(&'s mut [u16]),
    U32(&'s mut [u32]),
    U64(&'s mut [u64]),
    I8(&'s mut [i8]),
    I16(&'s mut [i16]),
    I32(&'s mut [i32]),
    I64(&'s mut [i64]),
    F32(&'s mut [f32]),
    F64(&'s mut [f64]),
}

impl<'s> TypedSliceMut<'s> {
    pub fn data_type(&self) -> RasterDataType {
        match self {
            TypedSliceMut::U8(_) => RasterDataType::U8,
            TypedSliceMut::U16(_) => RasterDataType::U16,
            TypedSliceMut::U32(_) => RasterDataType::U32,
            TypedSliceMut::U64(_) => RasterDataType::U64,
            TypedSliceMut::I8(_) => RasterDataType::I8,
            TypedSliceMut::I16(_) => RasterDataType::I16,
            TypedSliceMut::I32(_) => RasterDataType::I32,
            TypedSliceMut::I64(_) => RasterDataType::I64,
            TypedSliceMut::F32(_) => RasterDataType::F32,
            TypedSliceMut::F64(_) => RasterDataType::F64,
        }
    }
}

pub trait GenericSliceType: Pixel {
    fn create_typed_slice_ref(slice: &[Self]) -> TypedSliceRef;

    fn create_typed_slice_mut(slice: &mut [Self]) -> TypedSliceMut;
}

impl GenericSliceType for i32 {
    fn create_typed_slice_ref(slice: &[Self]) -> TypedSliceRef {
        TypedSliceRef::I32(slice)
    }

    fn create_typed_slice_mut(slice: &mut [Self]) -> TypedSliceMut {
        TypedSliceMut::I32(slice)
    }
}
