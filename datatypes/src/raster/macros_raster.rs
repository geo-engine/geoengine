/// Maps a `TypedRaster2D` to another `TypedRaster2D` by calling a function on its variant.
/// Call via `map_generic_raster2d!(input, raster => function)`.
#[macro_export]
macro_rules! map_generic_raster_2d {
    ($input_raster:expr, $raster:ident => $function_call:expr) => {
        map_generic_raster_2d!(
            @variants $input_raster, $raster => $function_call,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $input_raster:expr, $raster:ident => $function_call:expr, $($variant:tt),+) => {
        match $input_raster {
            $(
                $crate::raster::TypedRaster2D::$variant($raster) => {
                    $crate::raster::TypedRaster2D::$variant($function_call)
                }
            )+
        }
    };
}

/// Calls a function on a `TypedRaster2D` by calling it on its variant.
/// Call via `call_generic_raster2d!(input, raster => function)`.
#[macro_export]
macro_rules! call_generic_raster_2d {
    ($input_raster:expr, $raster:ident => $function_call:expr) => {
        call_generic_raster_2d!(
            @variants $input_raster, $raster => $function_call,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $input_raster:expr, $raster:ident => $function_call:expr, $($variant:tt),+) => {
        match $input_raster {
            $(
                $crate::raster::TypedRaster2D::$variant($raster) => $function_call,
            )+
        }
    };
}

/// Calls a function on a `TypedRaster2D` and some `RasterDataType`-like enum, effectively matching
/// the raster with the corresponding enum value of the other enum.
/// Call via `call_generic_raster2d_ext!(input, (raster, e) => function)`.
#[macro_export]
macro_rules! call_generic_raster_2d_ext {
    ($input_raster:expr, $other_enum:ty, ($raster:ident, $enum:ident) => $func:expr) => {
        call_generic_raster_2d_ext!(
            @variants $input_raster, $other_enum, ($raster, $enum) => $func,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $input_raster:expr, $other_enum:ty, ($raster:ident, $enum:ident) => $func:expr, $($variant:tt),+) => {
        match $input_raster {
            $(
                $crate::raster::TypedRaster2D::$variant($raster) => {
                    let $enum = <$other_enum>::$variant;
                    $func
                }
            )+
        }
    };
}
