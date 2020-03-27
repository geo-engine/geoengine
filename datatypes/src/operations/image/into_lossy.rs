/// This trait ensures a conversion into f64 and accepts a loss in precision.
pub trait IntoLossy<T> {
    /// Convert into f64 and accept a loss in precision for larger float or integer types
    fn into_lossy(self) -> T;
}

/// Implement `IntoLossyF64` for types that are Into<f64>
macro_rules! non_lossy_into_lossy_impl {
    ($from:ty, $into:ty) => {
        impl IntoLossy<$into> for $from {
            fn into_lossy(self) -> $into {
                self.into()
            }
        }
    };
}

/// Implement `IntoLossyF64` for types that are Into<f64>
macro_rules! type_cast_into_lossy_impl {
    ($from:ty, $into:ty) => {
        impl IntoLossy<$into> for $from {
            fn into_lossy(self) -> $into {
                self as $into
            }
        }
    };
}

non_lossy_into_lossy_impl!(f64, f64);
non_lossy_into_lossy_impl!(f32, f64);

non_lossy_into_lossy_impl!(u32, f64);
non_lossy_into_lossy_impl!(i32, f64);
non_lossy_into_lossy_impl!(u16, f64);
non_lossy_into_lossy_impl!(i16, f64);
non_lossy_into_lossy_impl!(u8, f64);
non_lossy_into_lossy_impl!(i8, f64);

type_cast_into_lossy_impl!(u64, f64);
type_cast_into_lossy_impl!(i64, f64);

impl IntoLossy<f64> for bool {
    fn into_lossy(self) -> f64 {
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
    fn conversions() {
        assert_eq!(42.0_f64.into_lossy(), 42.0_f64);
        assert_eq!(42.0_f32.into_lossy(), 42.0_f64);

        assert_eq!(42_u32.into_lossy(), 42.0_f64);
        assert_eq!(42_u32.into_lossy(), 42.0_f64);
        assert_eq!(42_u16.into_lossy(), 42.0_f64);
        assert_eq!(42_i16.into_lossy(), 42.0_f64);
        assert_eq!(42_u8.into_lossy(), 42.0_f64);
        assert_eq!(42_i8.into_lossy(), 42.0_f64);

        assert_eq!(42_u64.into_lossy(), 42.0_f64);
        assert_eq!(42_i64.into_lossy(), 42.0_f64);

        assert_eq!(true.into_lossy(), 1.0_f64);
        assert_eq!(false.into_lossy(), 0.0_f64);
    }
}
