use std::hash::{Hash, Hasher};
use std::{convert::TryFrom, ops::Add, ops::Div, ops::Mul, ops::Sub};

use crate::primitives::error;
use crate::util::Result;
use serde::{Deserialize, Serialize};
use snafu::ensure;

/// The spatial resolution in SRS units
#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
pub struct SpatialResolution {
    pub x: f64,
    pub y: f64,
}

impl PartialEq for SpatialResolution {
    fn eq(&self, other: &Self) -> bool {
        crate::util::helpers::f64_to_bits(self.x) == crate::util::helpers::f64_to_bits(other.x)
            && crate::util::helpers::f64_to_bits(self.y)
                == crate::util::helpers::f64_to_bits(other.y)
    }
}

impl Eq for SpatialResolution {}

impl Hash for SpatialResolution {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Ensure a single NAN representation
        state.write_u64(crate::util::helpers::f64_to_bits(self.x));
        state.write_u64(crate::util::helpers::f64_to_bits(self.y));
    }
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

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
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
    fn partial_eq() {
        let c1 = SpatialResolution { x: 4., y: 8. };
        let c2 = SpatialResolution { x: 4., y: 8. };
        assert_eq!(c1, c2);
    }

    #[test]
    fn partial_eq_nan() {
        let c1 = SpatialResolution { x: f64::NAN, y: 8. };
        let c2 = SpatialResolution { x: f64::NAN, y: 8. };
        assert_eq!(c1, c2);
    }

    #[test]
    fn hash() {
        let mut h1 = DefaultHasher::new();
        let c1 = SpatialResolution { x: 4., y: 8. };
        c1.hash(&mut h1);
        let v1 = h1.finish();

        let mut h2 = DefaultHasher::new();
        let c2 = SpatialResolution { x: 4., y: 8. };
        c2.hash(&mut h2);
        let v2 = h2.finish();

        assert_eq!(v1, v2);
    }

    #[test]
    fn hash_nan() {
        let mut h1 = DefaultHasher::new();
        let c1 = SpatialResolution { x: f64::NAN, y: 8. };
        c1.hash(&mut h1);
        let v1 = h1.finish();

        let mut h2 = DefaultHasher::new();
        let c2 = SpatialResolution { x: f64::NAN, y: 8. };
        c2.hash(&mut h2);
        let v2 = h2.finish();

        assert_eq!(v1, v2);
    }
}
