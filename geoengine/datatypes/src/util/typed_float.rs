//! Validated `f64` types that can be stored in the database.
//!
//! The orphan rule forbids implementing `ToSql`/`FromSql` for the [`typed_floats`] types directly,
//! so each type here is a transparent proxy around one of them.
//! It derefs to the inner type, so all of its methods (`get()`, `abs()`, …) are available.

use float_cmp::{ApproxEq, F64Margin};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use typed_floats::tf64;

macro_rules! typed_f64_proxy {
    ($(#[$attr:meta])* $Name:ident, $Inner:ty) => {
        $(#[$attr])*
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        #[repr(transparent)]
        pub struct $Name($Inner);

        impl $Name {
            /// Creates a new value, if `value` satisfies the constraints of the type.
            pub const fn new(value: f64) -> Result<Self, typed_floats::InvalidNumber> {
                match <$Inner>::new(value) {
                    Ok(value) => Ok(Self(value)),
                    Err(error) => Err(error),
                }
            }

            /// Creates a new value from a known-valid `value`, e.g. a constant.
            ///
            /// # Panics
            ///
            /// If `value` does not satisfy the constraints of the type.
            pub const fn new_unchecked(value: f64) -> Self {
                match Self::new(value) {
                    Ok(value) => value,
                    Err(_) => panic!(concat!("invalid value for ", stringify!($Name))),
                }
            }
        }

        impl std::ops::Deref for $Name {
            type Target = $Inner;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl From<$Inner> for $Name {
            fn from(value: $Inner) -> Self {
                Self(value)
            }
        }

        impl From<$Name> for $Inner {
            fn from(value: $Name) -> Self {
                value.0
            }
        }

        impl From<$Name> for f64 {
            fn from(value: $Name) -> Self {
                value.0.get()
            }
        }

        impl TryFrom<f64> for $Name {
            type Error = typed_floats::InvalidNumber;

            fn try_from(value: f64) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl std::str::FromStr for $Name {
            type Err = typed_floats::FromStrError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                <$Inner>::from_str(s).map(Self)
            }
        }

        impl std::fmt::Display for $Name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }

        impl ApproxEq for $Name {
            type Margin = F64Margin;

            fn approx_eq<M: Into<Self::Margin>>(self, other: Self, margin: M) -> bool {
                self.0.get().approx_eq(other.0.get(), margin)
            }
        }

        impl ToSql for $Name {
            fn to_sql(
                &self,
                ty: &postgres_types::Type,
                w: &mut bytes::BytesMut,
            ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                <f64 as ToSql>::to_sql(&self.0.get(), ty, w)
            }

            fn accepts(ty: &postgres_types::Type) -> bool {
                <f64 as ToSql>::accepts(ty)
            }

            postgres_types::to_sql_checked!();
        }

        impl<'a> FromSql<'a> for $Name {
            fn from_sql(
                ty: &postgres_types::Type,
                raw: &'a [u8],
            ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                let value = <f64 as FromSql>::from_sql(ty, raw)?;

                Ok(Self::new(value)?)
            }

            fn accepts(ty: &postgres_types::Type) -> bool {
                <f64 as FromSql>::accepts(ty)
            }
        }
    };
}

typed_f64_proxy!(
    /// An `f64` that is not NaN. Infinity is allowed, and `0.0 == -0.0`.
    NonNanF64,
    tf64::NonNaN
);

typed_f64_proxy!(
    /// An `f64` that is finite and not zero.
    NonZeroFiniteF64,
    tf64::NonZeroNonNaNFinite
);

typed_f64_proxy!(
    /// An `f64` that is finite and greater than or equal to zero. Negative zero is rejected.
    NonNegativeFiniteF64,
    tf64::PositiveFinite
);

typed_f64_proxy!(
    /// An `f64` that is finite and strictly greater than zero.
    PositiveFiniteF64,
    tf64::StrictlyPositiveFinite
);

impl PositiveFiniteF64 {
    pub const ONE: Self = Self::new_unchecked(1.);
    pub const TWO: Self = Self::new_unchecked(2.);
}

impl std::ops::Neg for NonZeroFiniteF64 {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self(-self.0)
    }
}

impl From<PositiveFiniteF64> for NonZeroFiniteF64 {
    fn from(value: PositiveFiniteF64) -> Self {
        Self(value.0.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_validates_values() {
        assert!(NonNanF64::new(0.0).is_ok());
        assert!(NonNanF64::new(f64::INFINITY).is_ok());
        assert!(NonNanF64::new(f64::NAN).is_err());

        assert!(NonZeroFiniteF64::new(-1.0).is_ok());
        assert!(NonZeroFiniteF64::new(1.0).is_ok());
        for invalid in [0.0, -0.0, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            assert!(NonZeroFiniteF64::new(invalid).is_err(), "{invalid}");
        }

        assert!(NonNegativeFiniteF64::new(0.0).is_ok());
        for invalid in [-0.0, -1.0, f64::INFINITY, f64::NAN] {
            assert!(NonNegativeFiniteF64::new(invalid).is_err(), "{invalid}");
        }

        assert!(PositiveFiniteF64::new(f64::MIN_POSITIVE).is_ok());
        for invalid in [0.0, -0.0, -1.0, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            assert!(PositiveFiniteF64::new(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn it_serializes_and_validates_on_deserialize() {
        let value = PositiveFiniteF64::new_unchecked(1.5);
        assert_eq!(serde_json::to_string(&value).unwrap(), "1.5");
        assert_eq!(
            serde_json::from_str::<PositiveFiniteF64>("1.5").unwrap(),
            value
        );

        assert!(serde_json::from_str::<PositiveFiniteF64>("0.0").is_err());
        assert!(serde_json::from_str::<PositiveFiniteF64>("-1.0").is_err());
        assert!(serde_json::from_str::<NonZeroFiniteF64>("0.0").is_err());
        assert!(serde_json::from_str::<NonZeroFiniteF64>("-1.0").is_ok());
    }

    #[test]
    fn it_round_trips_and_validates_sql() {
        let ty = postgres_types::Type::FLOAT8;

        let value = NonZeroFiniteF64::new_unchecked(-2.5);
        let mut bytes = bytes::BytesMut::new();
        value.to_sql(&ty, &mut bytes).unwrap();
        assert_eq!(NonZeroFiniteF64::from_sql(&ty, &bytes).unwrap(), value);

        let mut bytes = bytes::BytesMut::new();
        0.0_f64.to_sql(&ty, &mut bytes).unwrap();
        assert!(NonZeroFiniteF64::from_sql(&ty, &bytes).is_err());
        assert!(PositiveFiniteF64::from_sql(&ty, &bytes).is_err());
        assert!(NonNanF64::from_sql(&ty, &bytes).is_ok());

        let mut bytes = bytes::BytesMut::new();
        f64::NAN.to_sql(&ty, &mut bytes).unwrap();
        assert!(NonNanF64::from_sql(&ty, &bytes).is_err());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn it_derefs_to_the_inner_type() {
        let value = NonZeroFiniteF64::new_unchecked(-2.0);
        assert_eq!(value.get(), -2.0);
        assert_eq!(f64::from(-value), 2.0);
        assert_eq!(value.abs().get(), 2.0);
    }
}
