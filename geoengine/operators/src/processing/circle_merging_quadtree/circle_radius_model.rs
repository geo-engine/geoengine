use std::num::NonZeroUsize;

use geoengine_datatypes::util::{NonNegativeFiniteF64, PositiveFiniteF64};
use typed_floats::InvalidNumber;

/// A radius model that is based on the number of points in the circle.
pub trait CircleRadiusModel {
    /// Scales the radii by the `resolution`.
    /// Fails if the scaled radii overflow to infinity or underflow to zero.
    fn with_scaled_radii(&self, resolution: PositiveFiniteF64) -> Result<Self, InvalidNumber>
    where
        Self: Sized;

    fn min_radius(&self) -> f64;
    fn calculate_radius(&self, number_of_points: NonZeroUsize) -> f64;
    fn delta(&self) -> f64;
}

/// A radius model that scales circles based on the logarithm of the number of points.
#[derive(Clone, Debug, Copy)]
pub struct LogScaledRadius {
    min_radius: PositiveFiniteF64,
    delta: NonNegativeFiniteF64,
    resolution: PositiveFiniteF64,
}

impl LogScaledRadius {
    pub fn new(min_radius_px: PositiveFiniteF64, delta_px: NonNegativeFiniteF64) -> Self {
        LogScaledRadius {
            min_radius: min_radius_px,
            delta: delta_px,
            resolution: PositiveFiniteF64::ONE,
        }
    }
}

impl CircleRadiusModel for LogScaledRadius {
    fn with_scaled_radii(&self, resolution: PositiveFiniteF64) -> Result<Self, InvalidNumber>
    where
        Self: Sized,
    {
        Ok(Self {
            min_radius: PositiveFiniteF64::new(self.min_radius.get() * resolution.get())?,
            delta: NonNegativeFiniteF64::new(self.delta.get() * resolution.get())?,
            resolution,
        })
    }

    fn min_radius(&self) -> f64 {
        self.min_radius.get()
    }

    fn calculate_radius(&self, number_of_points: NonZeroUsize) -> f64 {
        self.min_radius.get() + (number_of_points.get() as f64).ln() * self.resolution.get()
    }

    fn delta(&self) -> f64 {
        self.delta.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_radius_calculation() {
        let radius_model = LogScaledRadius::new(
            PositiveFiniteF64::new_unchecked(8.),
            NonNegativeFiniteF64::new_unchecked(1.0),
        );

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
        let radius_model = LogScaledRadius::new(
            PositiveFiniteF64::new_unchecked(8.),
            NonNegativeFiniteF64::new_unchecked(1.0),
        )
        .with_scaled_radii(PositiveFiniteF64::new_unchecked(0.5))
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
