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

    pub fn with_native_resolution_and_zoom_level(
        native_resolution: SpatialResolution,
        zoom_level: u32,
    ) -> SpatialResolution {
        SpatialResolution::new_unchecked(
            native_resolution.x * f64::from(2_u32.pow(zoom_level)),
            native_resolution.y * f64::from(2_u32.pow(zoom_level)),
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
}
