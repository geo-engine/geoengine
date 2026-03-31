use std::num::NonZeroUsize;

use snafu::ensure;

use crate::error;
use crate::util::Result;

/// A radius model that is based on the number of points in the circle.
pub trait CircleRadiusModel {
    fn with_scaled_radii(&self, resolution: f64) -> Result<Self>
    where
        Self: Sized;

    fn min_radius(&self) -> f64;
    fn calculate_radius(&self, number_of_points: NonZeroUsize) -> f64;
    fn delta(&self) -> f64;
}

/// A radius model that scales circles based on the logarithm of the number of points.
#[derive(Clone, Debug, Copy)]
pub struct LogScaledRadius {
    min_radius: f64,
    delta: f64,
    resolution: f64,
}

impl LogScaledRadius {
    pub fn new(min_radius_px: f64, delta_px: f64) -> Result<Self> {
        ensure!(
            min_radius_px > 0.,
            error::InputMustBeGreaterThanZero {
                scope: "CircleRadiusModel",
                name: "min_radius"
            }
        );
        ensure!(
            delta_px >= 0.,
            error::InputMustBeZeroOrPositive {
                scope: "CircleRadiusModel",
                name: "delta_px"
            }
        );

        Ok(LogScaledRadius {
            min_radius: min_radius_px,
            delta: delta_px,
            resolution: 1.,
        })
    }
}

impl CircleRadiusModel for LogScaledRadius {
    fn with_scaled_radii(&self, resolution: f64) -> Result<Self>
    where
        Self: Sized,
    {
        ensure!(
            resolution > 0.,
            error::InputMustBeGreaterThanZero {
                scope: "CircleRadiusModel",
                name: "resolution"
            }
        );

        Ok(Self {
            min_radius: self.min_radius * resolution,
            delta: self.delta * resolution,
            resolution,
        })
    }

    fn min_radius(&self) -> f64 {
        self.min_radius
    }

    fn calculate_radius(&self, number_of_points: NonZeroUsize) -> f64 {
        self.min_radius + (number_of_points.get() as f64).ln() * self.resolution
    }

    fn delta(&self) -> f64 {
        self.delta
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_radius_calculation() {
        let radius_model = LogScaledRadius::new(8., 1.0).unwrap();

        assert_eq!(
            radius_model.calculate_radius(NonZeroUsize::new(1).unwrap()),
            8.
        );
        assert_eq!(
            radius_model.calculate_radius(NonZeroUsize::new(2).unwrap()),
            8. + 2.0_f64.ln()
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_scaled_radius_calculation() {
        let radius_model = LogScaledRadius::new(8., 1.0)
            .unwrap()
            .with_scaled_radii(0.5)
            .unwrap();

        assert_eq!(
            radius_model.calculate_radius(NonZeroUsize::new(1).unwrap()),
            (8. / 2.)
        );
        assert_eq!(
            radius_model.calculate_radius(NonZeroUsize::new(2).unwrap()),
            (8. / 2.) + (2.0_f64.ln() / 2.)
        );
    }
}
