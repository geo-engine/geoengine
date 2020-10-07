/// Maps a `TypedRaster2D` to another `TypedRaster2D` by calling a function on its variant.
/// Call via `map_generic_raster2d!(input, raster => function)`.
#[macro_export]
macro_rules! map_generic_raster2d {
    ($input_raster:expr, $raster:ident => $function_call:expr) => {
        map_generic_raster2d!(
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
macro_rules! call_generic_raster2d {
    ($input_raster:expr, $raster:ident => $function_call:expr) => {
        call_generic_raster2d!(
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

/// Calls a function on two `TypedRaster2D`s by calling it on their variant combination.
/// Call via `call_bi_generic_raster2d!(input, (raster_a, raster_b) => function)`.
#[macro_export]
macro_rules! call_bi_generic_raster2d {
    (
        $input_a:expr, $input_b:expr,
        ( $raster_a:ident, $raster_b:ident ) => $function_call:expr
    ) => {
        // TODO: this should be automated, but it seems like this requires a procedural macro
        call_bi_generic_raster2d!(
            @variants
            $input_a, $input_b,
            ( $raster_a, $raster_b ) => $function_call,
            (U8, U8), (U8, U16), (U8, U32), (U8, U64), (U8, I8), (U8, I16), (U8, I32), (U8, I64), (U8, F32), (U8, F64),
            (U16, U8), (U16, U16), (U16, U32), (U16, U64), (U16, I8), (U16, I16), (U16, I32), (U16, I64), (U16, F32), (U16, F64),
            (U32, U8), (U32, U16), (U32, U32), (U32, U64), (U32, I8), (U32, I16), (U32, I32), (U32, I64), (U32, F32), (U32, F64),
            (U64, U8), (U64, U16), (U64, U32), (U64, U64), (U64, I8), (U64, I16), (U64, I32), (U64, I64), (U64, F32), (U64, F64),
            (I8, U8), (I8, U16), (I8, U32), (I8, U64), (I8, I8), (I8, I16), (I8, I32), (I8, I64), (I8, F32), (I8, F64),
            (I16, U8), (I16, U16), (I16, U32), (I16, U64), (I16, I8), (I16, I16), (I16, I32), (I16, I64), (I16, F32), (I16, F64),
            (I32, U8), (I32, U16), (I32, U32), (I32, U64), (I32, I8), (I32, I16), (I32, I32), (I32, I64), (I32, F32), (I32, F64),
            (I64, U8), (I64, U16), (I64, U32), (I64, U64), (I64, I8), (I64, I16), (I64, I32), (I64, I64), (I64, F32), (I64, F64),
            (F32, U8), (F32, U16), (F32, U32), (F32, U64), (F32, I8), (F32, I16), (F32, I32), (F32, I64), (F32, F32), (F32, F64),
            (F64, U8), (F64, U16), (F64, U32), (F64, U64), (F64, I8), (F64, I16), (F64, I32), (F64, I64), (F64, F32), (F64, F64)
        )
    };

    (@variants
        $input_a:expr, $input_b:expr,
        ( $raster_a:ident, $raster_b:ident ) => $function_call:expr,
        $(($variant_a:tt,$variant_b:tt)),+
    ) => {
        match ($input_a, $input_b) {
            $(
                (
                    $crate::raster::TypedRaster2D::$variant_a($raster_a),
                    $crate::raster::TypedRaster2D::$variant_b($raster_b),
                ) => $function_call,
            )+
        }
    };

}

/// Calls a function on two `TypedRaster2D`s by calling it on their variant combination.
/// Call via `call_bi_generic_raster2d_same!(input, (raster_a, raster_b) => function)`.
/// The resulting call requires the rasters to be of the same type.
/// Otherwise, the last optional parameter is a catch-all function (or it just panics).
#[macro_export]
macro_rules! call_bi_generic_raster2d_same {
    (
        $input_a:expr, $input_b:expr,
        ( $raster_a:ident, $raster_b:ident ) => $function_call:expr
    ) => {
        call_bi_generic_raster2d_same!(
            $input_a, $input_b,
            ( $raster_a, $raster_b ) => $function_call,
            panic!("this method must not be called on rasters with varying types")
        )
    };

    (
        $input_a:expr, $input_b:expr,
        ( $raster_a:ident, $raster_b:ident ) => $function_call:expr,
        $on_error:expr
    ) => {
        call_bi_generic_raster2d_same!(
            @variants
            $input_a, $input_b,
            ( $raster_a, $raster_b ) => $function_call,
            $on_error,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (
        @variants
        $input_a:expr, $input_b:expr,
        ( $raster_a:ident, $raster_b:ident ) => $function_call:expr,
        $on_error:expr,
        $($variant:tt),+
    ) => {
        match ($input_a, $input_b) {
            $(
                (
                    $crate::raster::TypedRaster2D::$variant($raster_a),
                    $crate::raster::TypedRaster2D::$variant($raster_b)
                ) => $function_call,
            )+
            _ => $on_error
        }
    };
}

/// Calls a function on two `TypedRaster2D`s by calling it on their variant combination.
/// Call via `call_bi_generic_raster2d_staircase!(input, (raster_a, raster_b) => function)`.
/// This macro requires the first raster type to be greater or equal to the second one.
/// Otherwise, the last optional parameter is a catch-all function (or it just panics).
#[macro_export]
macro_rules! call_bi_generic_raster2d_staircase {
    (
        $input_a:expr, $input_b:expr,
        ( $raster_a:ident, $raster_b:ident ) => $function_call:expr
    ) => {
        call_bi_generic_raster2d_staircase!(
            $input_a, $input_b,
            ( $raster_a, $raster_b ) => $function_call,
            panic!("this method must not be called on rasters with incompatible types")
        )
    };

    (
        $input_a:expr, $input_b:expr,
        ( $raster_a:ident, $raster_b:ident ) => $function_call:expr,
        $on_error:expr
    ) => {
        call_bi_generic_raster2d_staircase!(
            @variants
            $input_a, $input_b,
            ( $raster_a, $raster_b ) => $function_call,
            $on_error,
            (U8, U8),
            (U16, U8), (U16, U16),
            (U32, U8), (U32, U16), (U32, U32),
            (U64, U8), (U64, U16), (U64, U32), (U64, U64),
            (I8, I8),
            (I16, U8), (I16, I8), (I16, I16),
            (I32, U8), (I32, U16), (I32, I8), (I32, I16), (I32, I32),
            (I64, U8), (I64, U16), (I64, U32), (I64, I8), (I64, I16), (I64, I32), (I64, I64),
            (F32, U8), (F32, U16), (F32, I8), (F32, I16), (F32, F32),
            (F64, U8), (F64, U16), (F64, U32), (F64, I8), (F64, I16), (F64, I32), (F64, F32), (F64, F64)
        )
    };

    (
        @variants
        $input_a:expr, $input_b:expr,
        ( $raster_a:ident, $raster_b:ident ) => $function_call:expr,
        $on_error:expr,
        $(($variant_a:tt,$variant_b:tt)),+
    ) => {
        match ($input_a, $input_b) {
            $(
                (
                    $crate::raster::TypedRaster2D::$variant_a($raster_a),
                    $crate::raster::TypedRaster2D::$variant_b($raster_b)
                ) => $function_call,
            )+
            _ => $on_error
        }
    };
}

/// Generates a a `TypedRaster2D` by calling a function with the specified data type variant.
/// Call via `generate_generic_raster2d!(type, function)`.
#[macro_export]
macro_rules! generate_generic_raster2d {
    ($type_enum:expr, $function_call:expr) => {
        generate_generic_raster2d!(
            @variants $type_enum, $function_call,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $type_enum:expr, $function_call:expr, $($variant:tt),+) => {
        match $type_enum {
            $(
                $crate::raster::RasterDataType::$variant => {
                    crate::raster::TypedRaster2D::$variant($function_call)
                }
            )+
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::raster::{GridPixelAccess, Pixel, Raster2D, RasterDataType, TypedRaster2D};
    use crate::util::test::catch_unwind_silent;

    #[test]
    fn map_generic_raster2d() {
        fn map<T: Pixel>(raster: Raster2D<T>) -> Raster2D<T> {
            raster
        }

        let typed_raster = TypedRaster2D::U32(
            Raster2D::new(
                [3, 2].into(),
                vec![1, 2, 3, 4, 5, 6],
                None,
                Default::default(),
                [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
            )
            .unwrap(),
        );

        assert_eq!(
            typed_raster,
            map_generic_raster2d!(typed_raster.clone(), raster => map(raster))
        );
    }

    #[test]
    fn call_generic_raster2d() {
        fn first_pixel<T: Pixel>(raster: &Raster2D<T>) -> i64 {
            raster.pixel_value_at_grid_index(&[0, 0]).unwrap().as_()
        }

        let typed_raster = TypedRaster2D::U32(
            Raster2D::new(
                [3, 2].into(),
                vec![1, 2, 3, 4, 5, 6],
                None,
                Default::default(),
                [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
            )
            .unwrap(),
        );

        assert_eq!(
            call_generic_raster2d!(typed_raster.clone(), _raster => 2),
            2
        );

        assert_eq!(
            call_generic_raster2d!(typed_raster, raster => first_pixel(&raster)),
            1
        );
    }

    #[test]
    fn generate_generic_raster2d() {
        fn generate<T: Pixel>() -> Raster2D<T> {
            let data: Vec<T> = vec![
                T::from_(1),
                T::from_(2),
                T::from_(3),
                T::from_(4),
                T::from_(5),
                T::from_(6),
            ];

            Raster2D::new(
                [3, 2].into(),
                data,
                None,
                Default::default(),
                [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
            )
            .unwrap()
        }

        assert_eq!(
            generate_generic_raster2d!(RasterDataType::U8, generate()),
            TypedRaster2D::U8(
                Raster2D::new(
                    [3, 2].into(),
                    vec![1, 2, 3, 4, 5, 6],
                    None,
                    Default::default(),
                    [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
                )
                .unwrap(),
            )
        );
    }

    #[test]
    fn call_bi_generic_raster2d() {
        fn first_pixel_add<T: Pixel, U: Pixel>(a: &Raster2D<T>, b: &Raster2D<U>) -> i64 {
            let pixel_a: i64 = a.pixel_value_at_grid_index(&[0, 0]).unwrap().as_();
            let pixel_b: i64 = b.pixel_value_at_grid_index(&[0, 0]).unwrap().as_();
            pixel_a + pixel_b
        }

        let typed_raster_a = TypedRaster2D::U32(
            Raster2D::new(
                [3, 2].into(),
                vec![1, 2, 3, 4, 5, 6],
                None,
                Default::default(),
                [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
            )
            .unwrap(),
        );
        let typed_raster_b = TypedRaster2D::U16(
            Raster2D::new(
                [3, 2].into(),
                vec![1, 2, 3, 4, 5, 6],
                None,
                Default::default(),
                [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
            )
            .unwrap(),
        );

        assert_eq!(
            call_bi_generic_raster2d!(
                typed_raster_a, typed_raster_b,
                (a, b) => first_pixel_add(&a, &b)
            ),
            2
        );
    }

    #[test]
    fn call_bi_generic_raster2d_same() {
        fn first_pixel_add<T: Pixel>(a: &Raster2D<T>, b: &Raster2D<T>) -> i64 {
            let pixel_a = a.pixel_value_at_grid_index(&[0, 0]).unwrap();
            let pixel_b = b.pixel_value_at_grid_index(&[0, 0]).unwrap();
            (pixel_a + pixel_b).as_()
        }

        let typed_raster_a = TypedRaster2D::U32(
            Raster2D::new(
                [3, 2].into(),
                vec![1, 2, 3, 4, 5, 6],
                None,
                Default::default(),
                [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
            )
            .unwrap(),
        );
        let typed_raster_b = TypedRaster2D::U16(
            Raster2D::new(
                [3, 2].into(),
                vec![1, 2, 3, 4, 5, 6],
                None,
                Default::default(),
                [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
            )
            .unwrap(),
        );

        assert_eq!(
            call_bi_generic_raster2d_same!(
                &typed_raster_a, &typed_raster_a,
                (a, b) => first_pixel_add(a, b)
            ),
            2
        );

        assert!(call_bi_generic_raster2d_same!(
            &typed_raster_a, &typed_raster_b,
            (a, b) => Ok(first_pixel_add(a, b)),
            Err(())
        )
        .is_err());

        assert!(catch_unwind_silent(|| call_bi_generic_raster2d_same!(
            &typed_raster_a, &typed_raster_b,
            (a, b) => first_pixel_add(a, b)
        ))
        .is_err());
    }

    #[test]
    fn call_bi_generic_raster2d_staircase() {
        fn first_pixel_add<T: Pixel, U: Pixel + Into<T>>(a: &Raster2D<T>, b: &Raster2D<U>) -> i64 {
            let pixel_a: T = a.pixel_value_at_grid_index(&[0, 0]).unwrap();
            let pixel_b: T = b.pixel_value_at_grid_index(&[0, 0]).unwrap().into();
            (pixel_a + pixel_b).as_()
        }

        let typed_raster_a = TypedRaster2D::U32(
            Raster2D::new(
                [3, 2].into(),
                vec![1, 2, 3, 4, 5, 6],
                None,
                Default::default(),
                [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
            )
            .unwrap(),
        );
        let typed_raster_b = TypedRaster2D::U16(
            Raster2D::new(
                [3, 2].into(),
                vec![1, 2, 3, 4, 5, 6],
                None,
                Default::default(),
                [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
            )
            .unwrap(),
        );

        assert_eq!(
            call_bi_generic_raster2d_staircase!(
                &typed_raster_a, &typed_raster_b,
                (a, b) => first_pixel_add(a, b)
            ),
            2
        );

        assert!(call_bi_generic_raster2d_staircase!(
            &typed_raster_b, &typed_raster_a,
            (a, b) => Ok(first_pixel_add(a, b)),
            Err(())
        )
        .is_err());

        assert!(catch_unwind_silent(|| call_bi_generic_raster2d_staircase!(
            &typed_raster_b, &typed_raster_a,
            (a, b) => first_pixel_add(a, b)
        ))
        .is_err());
    }
}
