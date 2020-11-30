/// Maps a `TypedGrid2D` to another `TypedGrid2D` by calling a function on its variant.
/// Call via `map_generic_grid_2d!(input, raster => function)`.
#[macro_export]
macro_rules! map_generic_grid_2d {
    ($input_grid:expr, $grid:ident => $function_call:expr) => {
        map_generic_grid_2d!(
            @variants $input_grid, $raster => $function_call,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $input_grid:expr, $grid:ident => $function_call:expr, $($variant:tt),+) => {
        match $input_grid {
            $(
                $crate::raster::TypedGrid2D::$variant($raster) => {
                    $crate::raster::TypedGrid2D::$variant($function_call)
                }
            )+
        }
    };
}

/// Calls a function on a `TypedGrid2D` by calling it on its variant.
/// Call via `call_generic_grid_2d!(input, raster => function)`.
#[macro_export]
macro_rules! call_generic_grid_2d {
    ($input_grid:expr, $grid:ident => $function_call:expr) => {
        call_generic_grid_2d!(
            @variants $input_grid, $grid => $function_call,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $input_grid:expr, $grid:ident => $function_call:expr, $($variant:tt),+) => {
        match $input_grid {
            $(
                $crate::raster::TypedGrid2D::$variant($grid) => $function_call,
            )+
        }
    };
}

/// Calls a function on a `TypedGrid2D` and some `RasterDataType`-like enum, effectively matching
/// the raster with the corresponding enum value of the other enum.
/// Call via `call_generic_grid_2d_ext!(input, (raster, e) => function)`.
#[macro_export]
macro_rules! call_generic_grid_2d_ext {
    ($input_grid:expr, $other_enum:ty, ($grid:ident, $enum:ident) => $func:expr) => {
        call_generic_grid_2d_ext!(
            @variants $input_grid, $other_enum, ($grid, $enum) => $func,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $input_grid:expr, $other_enum:ty, ($grid:ident, $enum:ident) => $func:expr, $($variant:tt),+) => {
        match $input_grid {
            $(
                $crate::raster::TypedGrid2D::$variant($grid) => {
                    let $enum = <$other_enum>::$variant;
                    $func
                }
            )+
        }
    };
}
