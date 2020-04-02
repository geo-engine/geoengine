/// This trait ensures a conversion into a type `T`.
/// Unlike `Into<T>`, it accepts a loss in precision.
pub trait LossyInto<T> {
    /// Convert into `T` and accept a loss in precision for types with larger value ranges
    fn lossy_into(self) -> T;
}

/// Implement `IntoLossy<T>` for types that are `Into<T>`
macro_rules! non_lossy_into_impl {
    ($from:ty, $into:ty) => {
        impl LossyInto<$into> for $from {
            fn lossy_into(self) -> $into {
                self.into()
            }
        }
    };
}

/// Implement `IntoLossy<T>` for types that be casted `as T`
macro_rules! type_cast_lossy_into_impl {
    ($from:ty, $into:ty) => {
        impl LossyInto<$into> for $from {
            fn lossy_into(self) -> $into {
                self as $into
            }
        }
    };
}

non_lossy_into_impl!(f64, f64);
non_lossy_into_impl!(f32, f64);

non_lossy_into_impl!(u32, f64);
non_lossy_into_impl!(i32, f64);
non_lossy_into_impl!(u16, f64);
non_lossy_into_impl!(i16, f64);
non_lossy_into_impl!(u8, f64);
non_lossy_into_impl!(i8, f64);

type_cast_lossy_into_impl!(u64, f64);
type_cast_lossy_into_impl!(i64, f64);

impl LossyInto<f64> for bool {
    /// This function allows transforming booleans to 0/1 `f64`s
    fn lossy_into(self) -> f64 {
        if self {
            1.
        } else {
            0.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn conversions() {
        assert_eq!(42.0_f64.lossy_into(), 42.0_f64);
        assert_eq!(42.0_f32.lossy_into(), 42.0_f64);

        assert_eq!(42_u32.lossy_into(), 42.0_f64);
        assert_eq!(42_i32.lossy_into(), 42.0_f64);
        assert_eq!(42_u16.lossy_into(), 42.0_f64);
        assert_eq!(42_i16.lossy_into(), 42.0_f64);
        assert_eq!(42_u8.lossy_into(), 42.0_f64);
        assert_eq!(42_i8.lossy_into(), 42.0_f64);

        assert_eq!(42_u64.lossy_into(), 42.0_f64);
        assert_eq!(42_i64.lossy_into(), 42.0_f64);

        assert_eq!(true.lossy_into(), 1.0_f64);
        assert_eq!(false.lossy_into(), 0.0_f64);
    }
}
