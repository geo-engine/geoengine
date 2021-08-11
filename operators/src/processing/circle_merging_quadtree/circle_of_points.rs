use std::num::NonZeroUsize;

use geoengine_datatypes::primitives::{Circle, Coordinate2D};

use crate::error::Error;
use crate::util::Result;

use super::circle_radius_model::CircleRadiusModel;

/// A `Circle` type that is extended by the amount of submerged points.
#[derive(Clone, Debug, PartialEq)]
pub struct CircleOfPoints {
    pub circle: Circle,
    pub number_of_points: NonZeroUsize,
}

impl CircleOfPoints {
    /// Create a new `CircleOfPoint` by giving a `Cirlce` and a number of submerged points.
    pub fn new(circle: Circle, number_of_points: usize) -> Result<Self> {
        let number_of_points =
            NonZeroUsize::new(number_of_points).ok_or(Error::InputMustBeGreaterThanZero {
                name: "number_of_points",
            })?;

        Ok(CircleOfPoints {
            circle,
            number_of_points,
        })
    }

    pub fn new_with_one_point(circle: Circle) -> Self {
        CircleOfPoints {
            circle,
            number_of_points: unsafe { NonZeroUsize::new_unchecked(1) },
        }
    }

    /// Merge this `CircleOfPoint` with another one.
    ///
    /// Specify the `min_radius` to influence the computation of the new radius (log growth).
    /// This depends on the amount of points which is the sum of the both amounts.
    ///
    /// The `Circle` center is the weighted center of the two `Circle`s.
    // TODO: make merge return a new circle of points?
    pub fn merge<C>(&mut self, other: &CircleOfPoints, circle_radius_model: &C)
    where
        C: CircleRadiusModel,
    {
        let total_number_of_points = unsafe {
            NonZeroUsize::new_unchecked(self.number_of_points.get() + other.number_of_points.get())
        };

        let new_center = {
            let total_length = total_number_of_points.get() as f64;
            let new_x = (self.circle.x() * self.number_of_points.get() as f64
                + other.circle.x() * other.number_of_points.get() as f64)
                / total_length;

            let new_y = (self.circle.y() * self.number_of_points.get() as f64
                + other.circle.y() * other.number_of_points.get() as f64)
                / total_length;

            Coordinate2D::new(new_x, new_y)
        };

        self.number_of_points = total_number_of_points;

        self.circle = Circle::from_coordinate(
            &new_center,
            circle_radius_model.calculate_radius(self.number_of_points),
        );
    }

    pub fn number_of_points(&self) -> usize {
        self.number_of_points.get()
    }
}

#[cfg(test)]
mod tests {
    use crate::processing::circle_merging_quadtree::circle_radius_model::LogScaledRadius;

    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_circle_merging() {
        let mut c1 = CircleOfPoints::new(
            Circle::from_coordinate(&Coordinate2D::new(1.0, 1.0), 1.0),
            1,
        )
        .unwrap();
        let c2 = CircleOfPoints::new(
            Circle::from_coordinate(&Coordinate2D::new(2.0, 1.0), 1.0),
            1,
        )
        .unwrap();

        let radius_model = LogScaledRadius::new(1.0, 0.).unwrap();

        c1.merge(&c2, &radius_model);

        assert_eq!(c1.number_of_points(), 2);
        assert_eq!(c1.circle.x(), 1.5);
        assert_eq!(c1.circle.y(), 1.0);
        assert_eq!(c1.circle.radius(), 1.0 + 2.0_f64.ln());
    }
}
