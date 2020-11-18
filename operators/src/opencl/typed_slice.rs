use geoengine_datatypes::raster::RasterDataType;
use ocl::{Buffer, OclPrm};

/// A reference to a slice with type information
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

/// Call a function on `TypedSliceRef` by applying it to its variant.
/// Call via `match_generic_slice_ref!(input, data => function)`.
macro_rules! match_generic_slice_ref {
    ($input:expr, $var_name:ident => $variant_fn:expr) => {
        match $input {
            TypedSliceRef::U8($var_name) => $variant_fn,
            TypedSliceRef::U16($var_name) => $variant_fn,
            TypedSliceRef::U32($var_name) => $variant_fn,
            TypedSliceRef::U64($var_name) => $variant_fn,
            TypedSliceRef::I8($var_name) => $variant_fn,
            TypedSliceRef::I16($var_name) => $variant_fn,
            TypedSliceRef::I32($var_name) => $variant_fn,
            TypedSliceRef::I64($var_name) => $variant_fn,
            TypedSliceRef::F32($var_name) => $variant_fn,
            TypedSliceRef::F64($var_name) => $variant_fn,
        }
    };
}

/// Call a function on `TypedSliceMut` by applying it to its variant.
/// Call via `match_generic_slice_mut!(input, data => function)`.
macro_rules! match_generic_slice_mut {
    ($input:expr, ($var_name:ident, $var_type:ident) => $variant_fn:expr) => {
        match_generic_slice_mut!(
            @variants $input, ($var_name, $var_type) => $variant_fn,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $input:expr, ($var_name:ident, $var_type:ident) => $variant_fn:expr, $($variant:tt),+ ) => {
        paste::paste! {
            match $input {
                $(
                    $crate::opencl::TypedSliceMut::$variant($var_name) => {
                        type $var_type = [<$variant:lower>];
                        $variant_fn
                    }
                )+
            }
        }
    };
}

/// A mutable reference to a slice with type information
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

#[derive(Clone)]
pub enum SliceOutputBuffer {
    U8(Buffer<u8>),
    U16(Buffer<u16>),
    U32(Buffer<u32>),
    U64(Buffer<u64>),
    I8(Buffer<i8>),
    I16(Buffer<i16>),
    I32(Buffer<i32>),
    I64(Buffer<i64>),
    F32(Buffer<f32>),
    F64(Buffer<f64>),
}

impl<T: GenericSliceType> From<Buffer<T>> for SliceOutputBuffer {
    fn from(buffer: Buffer<T>) -> Self {
        T::create_slice_output_buffer(buffer)
    }
}

impl<'s> TypedSliceMut<'s> {
    pub fn data_type(&self) -> SliceDataType {
        match self {
            TypedSliceMut::U8(_) => SliceDataType::U8,
            TypedSliceMut::U16(_) => SliceDataType::U16,
            TypedSliceMut::U32(_) => SliceDataType::U32,
            TypedSliceMut::U64(_) => SliceDataType::U64,
            TypedSliceMut::I8(_) => SliceDataType::I8,
            TypedSliceMut::I16(_) => SliceDataType::I16,
            TypedSliceMut::I32(_) => SliceDataType::I32,
            TypedSliceMut::I64(_) => SliceDataType::I64,
            TypedSliceMut::F32(_) => SliceDataType::F32,
            TypedSliceMut::F64(_) => SliceDataType::F64,
        }
    }
}

/// All possible data types for typed slices
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Copy, Clone)]
pub enum SliceDataType {
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

/// Call a function on `SliceDataType` by applying it to its variant.
/// Call via `match_slice_data_type!(input, var_type => function)`.
macro_rules! match_slice_data_type {
    ($input:expr, $var_type:ident => $variant_fn:expr) => {
        match_slice_data_type!(
            @variants $input, $var_type => $variant_fn,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $input:expr, $var_type:ident => $variant_fn:expr, $($variant:tt),+ ) => {
        paste::paste! {
            match $input {
                $(
                    $crate::opencl::SliceDataType::$variant => {
                        type $var_type = [<$variant:lower>];
                        $variant_fn
                    }
                )+
            }
        }
    };
}

impl From<RasterDataType> for SliceDataType {
    fn from(raster_data_type: RasterDataType) -> Self {
        match raster_data_type {
            RasterDataType::U8 => SliceDataType::U8,
            RasterDataType::U16 => SliceDataType::U16,
            RasterDataType::U32 => SliceDataType::U32,
            RasterDataType::U64 => SliceDataType::U64,
            RasterDataType::I8 => SliceDataType::I8,
            RasterDataType::I16 => SliceDataType::I16,
            RasterDataType::I32 => SliceDataType::I32,
            RasterDataType::I64 => SliceDataType::I64,
            RasterDataType::F32 => SliceDataType::F32,
            RasterDataType::F64 => SliceDataType::F64,
        }
    }
}

/// Common functionality for slice data types
pub trait GenericSliceType: Sized + OclPrm {
    const TYPE: SliceDataType;

    fn create_typed_slice_ref(slice: &[Self]) -> TypedSliceRef;

    fn create_typed_slice_mut(slice: &mut [Self]) -> TypedSliceMut;

    fn create_slice_output_buffer(buffer: Buffer<Self>) -> SliceOutputBuffer;
}

/// Implement `GenericSliceType` in a concise way
macro_rules! generic_slice_type_impl {
    ($type:ty, $variant:ident) => {
        impl GenericSliceType for $type {
            const TYPE: SliceDataType = SliceDataType::$variant;

            fn create_typed_slice_ref(slice: &[Self]) -> TypedSliceRef {
                TypedSliceRef::$variant(slice)
            }

            fn create_typed_slice_mut(slice: &mut [Self]) -> TypedSliceMut {
                TypedSliceMut::$variant(slice)
            }

            fn create_slice_output_buffer(buffer: Buffer<Self>) -> SliceOutputBuffer {
                SliceOutputBuffer::$variant(buffer)
            }
        }
    };
}

generic_slice_type_impl!(u8, U8);
generic_slice_type_impl!(u16, U16);
generic_slice_type_impl!(u32, U32);
generic_slice_type_impl!(u64, U64);

generic_slice_type_impl!(i8, I8);
generic_slice_type_impl!(i16, I16);
generic_slice_type_impl!(i32, I32);
generic_slice_type_impl!(i64, I64);

generic_slice_type_impl!(f32, F32);
generic_slice_type_impl!(f64, F64);
