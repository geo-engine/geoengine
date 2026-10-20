use std::{convert::TryFrom, ops::Add, ops::Div, ops::Mul, ops::Sub};

use crate::primitives::error;
use crate::util::{PositiveFiniteF64, Result};
use float_cmp::{ApproxEq, F64Margin, approx_eq};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};

/// The spatial resolution in SRS units.
/// Both values are always finite and strictly positive, which is enforced by their type.
#[derive(Copy, Clone, Debug, PartialEq, Deserialize, Serialize, ToSql, FromSql)]
pub struct SpatialResolution {
    x: PositiveFiniteF64,
    y: PositiveFiniteF64,
}

impl SpatialResolution {
    /// Create a new `SpatialResolution` object from known-valid values, e.g. constants.
    /// In a const context, invalid values fail at compile time.
    ///
    /// # Panics
    ///
    /// If `x` or `y` is not positive and finite.
    pub const fn new_unchecked(x: f64, y: f64) -> Self {
        Self::new_typed(
            PositiveFiniteF64::new_unchecked(x),
            PositiveFiniteF64::new_unchecked(y),
        )
    }

    /// Create a new `SpatialResolution` object, if `x` and `y` are positive and finite
    pub fn new(x: f64, y: f64) -> Result<Self> {
        let typed = |value: f64| {
            PositiveFiniteF64::new(value)
                .map_err(|_| error::InvalidSpatialResolution { value }.build())
        };

        Ok(Self::new_typed(typed(x)?, typed(y)?))
    }

    /// Create a new `SpatialResolution` object from already validated values
    pub const fn new_typed(x: PositiveFiniteF64, y: PositiveFiniteF64) -> Self {
        Self { x, y }
    }

    pub fn x(&self) -> f64 {
        self.x.get()
    }

    pub fn y(&self) -> f64 {
        self.y.get()
    }

    pub fn x_typed(&self) -> PositiveFiniteF64 {
        self.x
    }

    pub fn y_typed(&self) -> PositiveFiniteF64 {
        self.y
    }

    pub const fn zero_point_one() -> Self {
        Self::new_unchecked(0.1, 0.1)
    }

    pub const fn zero_point_five() -> Self {
        Self::new_unchecked(0.5, 0.5)
    }

    pub const fn one() -> Self {
        Self::new_unchecked(1., 1.)
    }

    /// Scales the native resolution by the factor of the GDAL overview level.
    /// Fails for overview level 0 and if the result overflows.
    pub fn with_native_resolution_and_gdal_overview_level(
        native_resolution: SpatialResolution,
        gdal_overview_level: u32,
    ) -> Result<SpatialResolution> {
        native_resolution * f64::from(gdal_overview_level)
    }

    /// Multiplies by a factor that is already known to be positive and finite.
    /// Returns `None` if the product overflows to infinity or underflows to zero.
    pub fn checked_mul(self, factor: PositiveFiniteF64) -> Option<Self> {
        (self * factor.get()).ok()
    }

    pub fn div_floor(self, rhs: Self) -> Result<Self> {
        Self::new((self.x() / rhs.x()).floor(), (self.y() / rhs.y()).floor())
    }

    pub fn div_ceil(self, rhs: Self) -> Result<Self> {
        Self::new((self.x() / rhs.x()).ceil(), (self.y() / rhs.y()).ceil())
    }
}

impl TryFrom<(f64, f64)> for SpatialResolution {
    type Error = crate::error::Error;

    fn try_from(value: (f64, f64)) -> Result<Self, Self::Error> {
        Self::new(value.0, value.1)
    }
}

/// Fails if the sum overflows.
impl Add for SpatialResolution {
    type Output = Result<Self>;

    fn add(self, rhs: Self) -> Self::Output {
        Self::new(self.x() + rhs.x(), self.y() + rhs.y())
    }
}

/// Fails if the difference is not positive.
impl Sub for SpatialResolution {
    type Output = Result<Self>;

    fn sub(self, rhs: Self) -> Self::Output {
        Self::new(self.x() - rhs.x(), self.y() - rhs.y())
    }
}

/// Fails if the product is not positive and finite.
impl Mul<f64> for SpatialResolution {
    type Output = Result<Self>;

    fn mul(self, rhs: f64) -> Self::Output {
        Self::new(self.x() * rhs, self.y() * rhs)
    }
}

/// Multiplies by a factor that is already known to be positive and finite.
///
/// # Panics
///
/// If the product overflows to infinity or underflows to zero.
/// Use [`SpatialResolution::checked_mul`] if the product is not bounded.
impl Mul<PositiveFiniteF64> for SpatialResolution {
    type Output = Self;

    fn mul(self, rhs: PositiveFiniteF64) -> Self::Output {
        self.checked_mul(rhs)
            .expect("the product of the resolution and the factor must not overflow")
    }
}

/// Fails if the quotient is not positive and finite.
impl Div<f64> for SpatialResolution {
    type Output = Result<Self>;

    fn div(self, rhs: f64) -> Self::Output {
        Self::new(self.x() / rhs, self.y() / rhs)
    }
}

impl ApproxEq for SpatialResolution {
    type Margin = F64Margin;

    fn approx_eq<M: Into<Self::Margin>>(self, other: Self, margin: M) -> bool {
        let m = margin.into();
        approx_eq!(f64, self.x(), other.x(), m) && approx_eq!(f64, self.y(), other.y(), m)
    }
}

#[allow(clippy::float_cmp)]
impl PartialOrd for SpatialResolution {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.x < other.x && self.y < other.y {
            Some(std::cmp::Ordering::Less)
        } else if self.x > other.x && self.y > other.y {
            Some(std::cmp::Ordering::Greater)
        } else if self.x == other.x && self.y == other.y {
            // TODO: use `approx_eq`?
            Some(std::cmp::Ordering::Equal)
        } else {
            None
        }
    }
}

/// finds the coarsest overview level that is still at least as fine as the required resolution
// TODO: add option to only use overview levels available for a given gdal dataset.
pub fn find_next_best_overview_level(
    native_resolution: SpatialResolution,
    target_resolution: SpatialResolution,
) -> u32 {
    let mut current_overview_level = 0;
    let mut next_overview_level: u32 = 2;

    // stops if the level or the resolution would overflow
    while let Ok(next_resolution) =
        SpatialResolution::with_native_resolution_and_gdal_overview_level(
            native_resolution,
            next_overview_level,
        )
        && next_resolution <= target_resolution
    {
        current_overview_level = next_overview_level;
        let Some(level) = next_overview_level.checked_mul(2) else {
            break;
        };
        next_overview_level = level;
    }

    current_overview_level
}

/// Scale up the given resolution to the next best overview level resolution, i.e., a resolution that is a power of 2 and still finer than the target
pub fn find_next_best_overview_level_resolution(
    mut current_resolution: SpatialResolution,
    target_resolution: SpatialResolution,
) -> SpatialResolution {
    debug_assert!(current_resolution <= target_resolution);

    // stops if the resolution would overflow
    while let Some(next_resolution) = current_resolution.checked_mul(PositiveFiniteF64::TWO)
        && next_resolution <= target_resolution
    {
        current_resolution = next_resolution;
    }

    current_resolution
}

#[cfg(test)]
mod test {
    use super::*;
    use std::mem;

    #[test]
    fn byte_size() {
        assert_eq!(
            mem::size_of::<SpatialResolution>(),
            2 * mem::size_of::<f64>()
        );
        assert_eq!(mem::size_of::<SpatialResolution>(), 2 * 8);
    }

    #[test]
    fn add() {
        let res = (SpatialResolution::new_unchecked(4., 9.)
            + SpatialResolution::new_unchecked(1., 1.))
        .unwrap();
        assert_eq!(res, SpatialResolution::new_unchecked(5., 10.));

        assert!(
            (SpatialResolution::new_unchecked(f64::MAX, 1.)
                + SpatialResolution::new_unchecked(f64::MAX, 1.))
            .is_err()
        );
    }

    #[test]
    fn sub() {
        let res = (SpatialResolution::new_unchecked(4., 9.)
            - SpatialResolution::new_unchecked(1., 1.))
        .unwrap();
        assert_eq!(res, SpatialResolution::new_unchecked(3., 8.));

        assert!(
            (SpatialResolution::new_unchecked(1., 9.) - SpatialResolution::new_unchecked(1., 1.))
                .is_err()
        );
    }

    #[test]
    fn mul_scalar() {
        let res = (SpatialResolution::new_unchecked(4., 9.) * 2.).unwrap();
        assert_eq!(res, SpatialResolution::new_unchecked(8., 18.));

        let res = SpatialResolution::new_unchecked(4., 9.) * PositiveFiniteF64::new_unchecked(2.);
        assert_eq!(res, SpatialResolution::new_unchecked(8., 18.));

        for invalid in [0., -1., f64::INFINITY, f64::NAN, f64::MAX] {
            assert!((SpatialResolution::new_unchecked(4., 9.) * invalid).is_err());
        }
    }

    #[test]
    fn div_scalar() {
        let res = (SpatialResolution::new_unchecked(4., 8.) / 2.).unwrap();
        assert_eq!(res, SpatialResolution::new_unchecked(2., 4.));

        for invalid in [0., -1., f64::NAN, f64::MIN_POSITIVE] {
            assert!((SpatialResolution::new_unchecked(4., 8.) / invalid).is_err());
        }
    }

    #[test]
    fn it_rejects_invalid_resolutions() {
        assert!(SpatialResolution::new(0.1, 0.1).is_ok());

        for invalid in [0., -0.1, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            assert!(SpatialResolution::new(invalid, 0.1).is_err());
            assert!(SpatialResolution::new(0.1, invalid).is_err());
        }
    }

    #[test]
    fn it_rejects_invalid_resolutions_on_deserialize() {
        assert_eq!(
            serde_json::from_str::<SpatialResolution>(r#"{"x": 0.5, "y": 2.0}"#).unwrap(),
            SpatialResolution::new_unchecked(0.5, 2.0)
        );

        for invalid in [r#"{"x": 0.0, "y": 1.0}"#, r#"{"x": 1.0, "y": -1.0}"#] {
            assert!(serde_json::from_str::<SpatialResolution>(invalid).is_err());
        }
    }

    #[test]
    fn it_stops_at_overflowing_overview_levels() {
        // doubling `MAX / 3` once is fine, doubling it twice overflows
        let native = SpatialResolution::new_unchecked(f64::MAX / 3., f64::MAX / 3.);
        let target = SpatialResolution::new_unchecked(f64::MAX, f64::MAX);

        assert_eq!(find_next_best_overview_level(native, target), 2);
        assert_eq!(
            find_next_best_overview_level_resolution(native, target),
            (native * 2.).unwrap()
        );

        let native = SpatialResolution::new_unchecked(f64::MIN_POSITIVE, f64::MIN_POSITIVE);
        assert_eq!(find_next_best_overview_level(native, target), 1 << 31);
    }

    #[test]
    fn it_finds_next_best_overview_level() {
        assert_eq!(
            find_next_best_overview_level(
                SpatialResolution::new_unchecked(0.1, 0.1),
                SpatialResolution::new_unchecked(0.1, 0.1)
            ),
            0
        );

        assert_eq!(
            find_next_best_overview_level(
                SpatialResolution::new_unchecked(0.1, 0.1),
                SpatialResolution::new_unchecked(0.2, 0.2)
            ),
            2
        );

        assert_eq!(
            find_next_best_overview_level(
                SpatialResolution::new_unchecked(0.1, 0.1),
                SpatialResolution::new_unchecked(0.3, 0.3)
            ),
            2
        );

        assert_eq!(
            find_next_best_overview_level(
                SpatialResolution::new_unchecked(0.1, 0.1),
                SpatialResolution::new_unchecked(0.4, 0.4)
            ),
            4
        );
    }
}
