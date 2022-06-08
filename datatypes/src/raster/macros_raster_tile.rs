/// Maps a `TypedRasterTile2D` to another `TypedRasterTile2D` by calling a function on its variant.
/// Call via `map_generic_raster_tile_2d!(input, raster => function)`.
#[macro_export]
macro_rules! map_generic_raster_tile_2d {
    ($input_raster:expr, $raster:ident => $function_call:expr) => {
        map_generic_raster_tile_2d!(
            @variants $input_raster, $raster => $function_call,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $input_raster:expr, $raster:ident => $function_call:expr, $($variant:tt),+) => {
        match $input_raster {
            $(
                $crate::raster::TypedRasterTile2D::$variant($raster) => {
                    $crate::raster::TypedRasterTile2D::$variant($function_call)
                }
            )+
        }
    };
}

/// Calls a function on a `TypedRasterTile2D` by calling it on its variant.
/// Call via `call_generic_raster_tile_2d!(input, raster => function)`.
#[macro_export]
macro_rules! call_generic_raster_tile_2d {
    ($input_raster:expr, $raster:ident => $function_call:expr) => {
        call_generic_raster_tile_2d!(
            @variants $input_raster, $raster => $function_call,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $input_raster:expr, $raster:ident => $function_call:expr, $($variant:tt),+) => {
        match $input_raster {
            $(
                $crate::raster::TypedRasterTile2D::$variant($raster) => $function_call,
            )+
        }
    };
}

/// Calls a function on two `TypedRasterTile2D`s by calling it on their variant combination.
/// Call via `call_bi_generic_raster_tile_2d!(input_a, input_b, (raster_a, raster_b) => function)`.
#[macro_export]
macro_rules! call_bi_generic_raster_tile_2d {
    (
        $input_a:expr, $input_b:expr,
        ( $raster_a:ident, $raster_b:ident ) => $function_call:expr
    ) => {
        // TODO: this should be automated, but it seems like this requires a procedural macro
        call_bi_generic_raster_tile_2d!(
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
                    $crate::raster::TypedRasterTile2D::$variant_a($raster_a),
                    $crate::raster::TypedRasterTile2D::$variant_b($raster_b),
                ) => $function_call,
            )+
        }
    };

}

/// Calls a function on two `TypedRasterTile2D`s by calling it on their variant combination.
/// Call via `call_bi_generic_raster_tile_2d_same!(input_a, input_b, (raster_a, raster_b) => function)`.
/// The resulting call requires the rasters to be of the same type.
/// Otherwise, the last optional parameter is a catch-all function (or it just panics).
#[macro_export]
macro_rules! call_bi_generic_raster_tile_2d_same {
    (
        $input_a:expr, $input_b:expr,
        ( $raster_a:ident, $raster_b:ident ) => $function_call:expr
    ) => {
        call_bi_generic_raster_tile_2d_same!(
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
        call_bi_generic_raster_tile_2d_same!(
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
                    $crate::raster::TypedRasterTile2D::$variant($raster_a),
                    $crate::raster::TypedRasterTile2D::$variant($raster_b)
                ) => $function_call,
            )+
            _ => $on_error
        }
    };
}

/// Calls a function on two `TypedRasterTile2D`s by calling it on their variant combination.
/// Call via `call_bi_generic_raster_tile_2d_staircase!(input_a, input_b, (raster_a, raster_b) => function)`.
/// This macro requires the first raster type to be greater or equal to the second one.
/// Otherwise, the last optional parameter is a catch-all function (or it just panics).
#[macro_export]
macro_rules! call_bi_generic_raster_tile_2d_staircase {
    (
        $input_a:expr, $input_b:expr,
        ( $raster_a:ident, $raster_b:ident ) => $function_call:expr
    ) => {
        call_bi_generic_raster_tile_2d_staircase!(
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
        call_bi_generic_raster_tile_2d_staircase!(
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
                    $crate::raster::TypedRasterTile2D::$variant_a($raster_a),
                    $crate::raster::TypedRasterTile2D::$variant_b($raster_b)
                ) => $function_call,
            )+
            _ => $on_error
        }
    };
}

/// Generates a a `TypedRasterTile2D` by calling a function with the specified data type variant.
/// Call via `generate_generic_raster_tile_2d!(type, function)`.
#[macro_export]
macro_rules! generate_generic_raster_tile_2d {
    ($type_enum:expr, $function_call:expr) => {
        generate_generic_raster_tile_2d!(
            @variants $type_enum, $function_call,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $type_enum:expr, $function_call:expr, $($variant:tt),+) => {
        match $type_enum {
            $(
                $crate::raster::RasterDataType::$variant => {
                    $crate::raster::TypedRasterTile2D::$variant($function_call)
                }
            )+
        }
    };
}

/// Calls a function on a `TypedRasterTile2D` and some `RasterDataType`-like enum, effectively matching
/// the raster with the corresponding enum value of the other enum.
/// Call via `call_generic_raster_tile_2d_ext!(input, (raster, e) => function)`.
#[macro_export]
macro_rules! call_generic_raster_tile_2d_ext {
    ($input_raster:expr, $other_enum:ty, ($raster:ident, $enum:ident) => $func:expr) => {
        call_generic_raster_tile_2d_ext!(
            @variants $input_raster, $other_enum, ($raster, $enum) => $func,
            U8, U16, U32, U64, I8, I16, I32, I64, F32, F64
        )
    };

    (@variants $input_raster:expr, $other_enum:ty, ($raster:ident, $enum:ident) => $func:expr, $($variant:tt),+) => {
        match $input_raster {
            $(
                $crate::raster::TypedRasterTile2D::$variant($raster) => {
                    let $enum = <$other_enum>::$variant;
                    $func
                }
            )+
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::{
        primitives::TimeInterval,
        raster::{
            GeoTransform, Grid2D, MaskedGridIndexAccess, Pixel, RasterTile2D, TypedRasterTile2D,
        },
        util::test::TestDefault,
    };
    use crate::{raster::RasterDataType, util::test::catch_unwind_silent};
    use std::marker::PhantomData;

    #[test]
    fn map_generic_raster2d() {
        fn map<T: Pixel>(raster: RasterTile2D<T>) -> RasterTile2D<T> {
            raster
        }

        let r = Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap();
        let t = RasterTile2D::new_without_offset(
            TimeInterval::default(),
            GeoTransform::test_default(),
            r,
        );
        let typed_raster = TypedRasterTile2D::U32(t);

        assert_eq!(
            typed_raster,
            map_generic_raster_tile_2d!(typed_raster.clone(), raster => map(raster))
        );
    }

    #[test]
    fn call_generic_raster2d() {
        fn first_pixel<T: Pixel>(raster: &RasterTile2D<T>) -> i64 {
            raster
                .get_masked_at_grid_index([0, 0])
                .unwrap()
                .unwrap()
                .as_()
        }

        let r = Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap();
        let t = RasterTile2D::new_without_offset(
            TimeInterval::default(),
            GeoTransform::test_default(),
            r,
        );
        let typed_raster = TypedRasterTile2D::U32(t);

        assert_eq!(
            call_generic_raster_tile_2d!(typed_raster.clone(), _raster => 2),
            2
        );

        assert_eq!(
            call_generic_raster_tile_2d!(typed_raster, raster => first_pixel(&raster)),
            1
        );
    }

    #[test]
    fn generate_generic_raster2d() {
        fn generate<T: Pixel>() -> RasterTile2D<T> {
            let data: Vec<T> = vec![
                T::from_(1),
                T::from_(2),
                T::from_(3),
                T::from_(4),
                T::from_(5),
                T::from_(6),
            ];

            let r = Grid2D::new([3, 2].into(), data).unwrap();
            RasterTile2D::new_without_offset(
                TimeInterval::default(),
                GeoTransform::test_default(),
                r,
            )
        }

        assert_eq!(
            generate_generic_raster_tile_2d!(RasterDataType::U8, generate()),
            TypedRasterTile2D::U8(RasterTile2D::new_without_offset(
                TimeInterval::default(),
                GeoTransform::test_default(),
                Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6],).unwrap()
            ),)
        );
    }

    #[test]
    fn call_bi_generic_raster_tile_2d() {
        fn first_pixel_add<T: Pixel, U: Pixel>(a: &RasterTile2D<T>, b: &RasterTile2D<U>) -> i64 {
            let pixel_a: i64 = a.get_masked_at_grid_index([0, 0]).unwrap().unwrap().as_();
            let pixel_b: i64 = b.get_masked_at_grid_index([0, 0]).unwrap().unwrap().as_();
            pixel_a + pixel_b
        }

        let r = Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap();
        let t = RasterTile2D::new_without_offset(
            TimeInterval::default(),
            GeoTransform::test_default(),
            r,
        );
        let typed_raster_a = TypedRasterTile2D::U32(t);

        let r = Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap();
        let t = RasterTile2D::new_without_offset(
            TimeInterval::default(),
            GeoTransform::test_default(),
            r,
        );
        let typed_raster_b = TypedRasterTile2D::U16(t);

        assert_eq!(
            call_bi_generic_raster_tile_2d!(
                typed_raster_a, typed_raster_b,
                (a, b) => first_pixel_add(&a, &b)
            ),
            2
        );
    }

    #[test]
    fn call_bi_generic_raster_tile_2d_same() {
        fn first_pixel_add<T: Pixel>(a: &RasterTile2D<T>, b: &RasterTile2D<T>) -> i64 {
            let pixel_a = a.get_masked_at_grid_index([0, 0]).unwrap().unwrap();
            let pixel_b = b.get_masked_at_grid_index([0, 0]).unwrap().unwrap();
            (pixel_a + pixel_b).as_()
        }

        let r = Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap();
        let t = RasterTile2D::new_without_offset(
            TimeInterval::default(),
            GeoTransform::test_default(),
            r,
        );
        let typed_raster_a = TypedRasterTile2D::U32(t);

        let r = Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap();
        let t = RasterTile2D::new_without_offset(
            TimeInterval::default(),
            GeoTransform::test_default(),
            r,
        );
        let typed_raster_b = TypedRasterTile2D::U16(t);

        assert_eq!(
            call_bi_generic_raster_tile_2d_same!(
                &typed_raster_a, &typed_raster_a,
                (a, b) => first_pixel_add(a, b)
            ),
            2
        );

        assert!(call_bi_generic_raster_tile_2d_same!(
            &typed_raster_a, &typed_raster_b,
            (a, b) => Ok(first_pixel_add(a, b)),
            Err(())
        )
        .is_err());

        assert!(catch_unwind_silent(|| call_bi_generic_raster_tile_2d_same!(
            &typed_raster_a, &typed_raster_b,
            (a, b) => first_pixel_add(a, b)
        ))
        .is_err());
    }

    #[test]
    fn call_bi_generic_raster2d_staircase() {
        fn first_pixel_add<T: Pixel, U: Pixel + Into<T>>(
            a: &RasterTile2D<T>,
            b: &RasterTile2D<U>,
        ) -> i64 {
            let pixel_a: T = a.get_masked_at_grid_index([0, 0]).unwrap().unwrap();
            let pixel_b: T = b.get_masked_at_grid_index([0, 0]).unwrap().unwrap().into();
            (pixel_a + pixel_b).as_()
        }

        let r = Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap();
        let t = RasterTile2D::new_without_offset(
            TimeInterval::default(),
            GeoTransform::test_default(),
            r,
        );
        let typed_raster_a = TypedRasterTile2D::U32(t);

        let r = Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap();
        let t = RasterTile2D::new_without_offset(
            TimeInterval::default(),
            GeoTransform::test_default(),
            r,
        );
        let typed_raster_b = TypedRasterTile2D::U16(t);

        assert_eq!(
            call_bi_generic_raster_tile_2d_staircase!(
                &typed_raster_a, &typed_raster_b,
                (a, b) => first_pixel_add(a, b)
            ),
            2
        );

        assert!(call_bi_generic_raster_tile_2d_staircase!(
            &typed_raster_b, &typed_raster_a,
            (a, b) => Ok(first_pixel_add(a, b)),
            Err(())
        )
        .is_err());

        assert!(
            catch_unwind_silent(|| call_bi_generic_raster_tile_2d_staircase!(
                &typed_raster_b, &typed_raster_a,
                (a, b) => first_pixel_add(a, b)
            ))
            .is_err()
        );
    }

    #[test]
    fn ext() {
        struct T<S> {
            s: PhantomData<S>,
        }

        enum Foo {
            U8(T<u8>),
            U16(T<u16>),
            U32(T<u32>),
            U64(T<u64>),
            I8(T<i8>),
            I16(T<i16>),
            I32(T<i32>),
            I64(T<i64>),
            F32(T<f32>),
            F64(T<f64>),
        }

        fn foo<S>(_f: fn(T<S>) -> Foo, _r: &RasterTile2D<S>)
        where
            S: Pixel,
        {
        }

        let typed_raster_a = TypedRasterTile2D::U32(RasterTile2D::new(
            TimeInterval::default(),
            [0, 0].into(),
            [1.0, 1.0, 0.0, 1.0, 0.0, -1.0].into(),
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
        ));

        call_generic_raster_tile_2d_ext!(typed_raster_a, Foo, (raster, e) => {
            foo(e, &raster);
        });
    }
}
