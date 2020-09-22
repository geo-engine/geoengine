use std::convert::TryFrom;

use crate::primitives::error;
use crate::util::Result;
use snafu::ensure;

/// The spatial resolution in SRS units
#[derive(Copy, Clone, Debug)]
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
}

impl Default for SpatialResolution {
    fn default() -> Self {
        SpatialResolution { x: 0.1, y: 0.1 }
    }
}

impl TryFrom<(f64, f64)> for SpatialResolution {
    type Error = crate::error::Error;

    fn try_from(value: (f64, f64)) -> Result<Self, Self::Error> {
        Self::new(value.0, value.1)
    }
}
