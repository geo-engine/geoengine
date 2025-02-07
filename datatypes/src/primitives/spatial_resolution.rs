use std::{convert::TryFrom, ops::Add, ops::Div, ops::Mul, ops::Sub};

use crate::primitives::error;
use crate::util::Result;
use float_cmp::{approx_eq, ApproxEq, F64Margin};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ensure;

/// The spatial resolution in SRS units
#[derive(Copy, Clone, Debug, PartialEq, Deserialize, Serialize, ToSql, FromSql)]
pub struct SpatialResolution {
    pub x: f64,
    pub y: f64,
}

impl SpatialResolution {
    /// Create a new `SpatialResolution` object
    pub fn new_unchecked(x: f64, y: f64) -> Self {
        SpatialResolution { x, y }
    }

    pub fn new(x: f64, y: f64) -> Result<Self> {
        ensure!(x > 0.0, error::InvalidSpatialResolution { value: x });
        ensure!(y > 0.0, error::InvalidSpatialResolution { value: y });
        Ok(Self::new_unchecked(x, y))
    }

    pub fn zero_point_one() -> Self {
        SpatialResolution { x: 0.1, y: 0.1 }
    }

    pub fn zero_point_five() -> Self {
        SpatialResolution { x: 0.5, y: 0.5 }
    }

    pub fn one() -> Self {
        SpatialResolution { x: 1., y: 1. }
    }

    pub fn with_native_resolution_and_gdal_overview_level(
        native_resolution: SpatialResolution,
        gdal_overview_level: u32,
    ) -> SpatialResolution {
        SpatialResolution::new_unchecked(
            native_resolution.x * f64::from(gdal_overview_level),
            native_resolution.y * f64::from(gdal_overview_level),
        )
    }
}

impl TryFrom<(f64, f64)> for SpatialResolution {
    type Error = crate::error::Error;

    fn try_from(value: (f64, f64)) -> Result<Self, Self::Error> {
        Self::new(value.0, value.1)
    }
}

impl Add for SpatialResolution {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        SpatialResolution {
            x: self.x + rhs.x,
            y: self.y + rhs.y,
        }
    }
}

impl Sub for SpatialResolution {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        SpatialResolution {
            x: self.x - rhs.x,
            y: self.y - rhs.y,
        }
    }
}

impl Mul<f64> for SpatialResolution {
    type Output = Self;

    fn mul(self, rhs: f64) -> Self::Output {
        SpatialResolution {
            x: self.x * rhs,
            y: self.y * rhs,
        }
    }
}

impl Div<f64> for SpatialResolution {
    type Output = Self;

    fn div(self, rhs: f64) -> Self::Output {
        SpatialResolution {
            x: self.x / rhs,
            y: self.y / rhs,
        }
    }
}

impl ApproxEq for SpatialResolution {
    type Margin = F64Margin;

    fn approx_eq<M: Into<Self::Margin>>(self, other: Self, margin: M) -> bool {
        let m = margin.into();
        approx_eq!(f64, self.x, other.x, m) && approx_eq!(f64, self.y, other.y, m)
    }
}

impl PartialOrd for SpatialResolution {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.x < other.x && self.y < other.y {
            Some(std::cmp::Ordering::Less)
        } else if self.x > other.x && self.y > other.y {
            Some(std::cmp::Ordering::Greater)
        } else if self.x == other.x && self.y == other.y {
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
    let mut next_overview_level = 2;

    while SpatialResolution::with_native_resolution_and_gdal_overview_level(
        native_resolution,
        next_overview_level,
    ) <= target_resolution
    {
        current_overview_level = next_overview_level;
        next_overview_level *= 2;
    }

    current_overview_level
}

/// Scale up the given resolution to the next best overview level resolution, i.e., a resolution that is a power of 2 and still finer than the target
pub fn find_next_best_overview_level_resolution(
    mut current_resolution: SpatialResolution,
    target_resolution: SpatialResolution,
) -> SpatialResolution {
    debug_assert!(current_resolution < target_resolution);

    while current_resolution * 2.0 <= target_resolution {
        current_resolution = current_resolution * 2.0;
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
        let res = SpatialResolution { x: 4., y: 9. } + SpatialResolution { x: 1., y: 1. };
        assert_eq!(res, SpatialResolution { x: 5., y: 10. });
    }

    #[test]
    fn sub() {
        let res = SpatialResolution { x: 4., y: 9. } - SpatialResolution { x: 1., y: 1. };
        assert_eq!(res, SpatialResolution { x: 3., y: 8. });
    }

    #[test]
    fn mul_scalar() {
        let res = SpatialResolution { x: 4., y: 9. } * 2.;
        assert_eq!(res, SpatialResolution { x: 8., y: 18. });
    }

    #[test]
    fn div_scalar() {
        let res = SpatialResolution { x: 4., y: 8. } / 2.;
        assert_eq!(res, SpatialResolution { x: 2., y: 4. });
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
