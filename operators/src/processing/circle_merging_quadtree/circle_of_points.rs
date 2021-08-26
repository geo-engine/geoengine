use std::collections::HashMap;
use std::num::NonZeroUsize;

use geoengine_datatypes::primitives::{Circle, Coordinate2D};

use crate::error::Error;
use crate::util::Result;

use super::aggregates::AttributeAggregate;
use super::circle_radius_model::CircleRadiusModel;

/// A `Circle` type that is extended by the amount of submerged points.
#[derive(Clone, Debug, PartialEq)]
pub struct CircleOfPoints {
    pub circle: Circle,
    pub number_of_points: NonZeroUsize,
    pub attribute_aggregates: HashMap<String, AttributeAggregate>,
}

impl CircleOfPoints {
    /// Create a new `CircleOfPoint` by giving a `Cirlce` and a number of submerged points.
    pub fn new(
        circle: Circle,
        number_of_points: usize,
        attribute_aggregates: HashMap<String, AttributeAggregate>,
    ) -> Result<Self> {
        let number_of_points =
            NonZeroUsize::new(number_of_points).ok_or(Error::InputMustBeGreaterThanZero {
                scope: "VisualPointClustering",
                name: "number_of_points",
            })?;

        Ok(CircleOfPoints {
            circle,
            number_of_points,
            attribute_aggregates,
        })
    }

    pub fn new_with_one_point(
        circle: Circle,
        attribute_aggregates: HashMap<String, AttributeAggregate>,
    ) -> Self {
        CircleOfPoints {
            circle,
            number_of_points: unsafe { NonZeroUsize::new_unchecked(1) },
            attribute_aggregates,
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

        self.merge_attributes(&other.attribute_aggregates);

        self.circle = Circle::from_coordinate(
            &new_center,
            circle_radius_model.calculate_radius(self.number_of_points),
        );
    }

    fn merge_attributes(&mut self, other_aggregates: &HashMap<String, AttributeAggregate>) {
        for (attribute, aggregate) in &mut self.attribute_aggregates {
            if let Some(other_aggregate) = other_aggregates.get(attribute) {
                aggregate.merge(other_aggregate);
            } else {
                // use null if not found - but should not happen
                aggregate.merge(&AttributeAggregate::Null);
            }
        }
    }

    pub fn number_of_points(&self) -> usize {
        self.number_of_points.get()
    }
}

#[cfg(test)]
mod tests {
    use crate::processing::circle_merging_quadtree::{
        aggregates::MeanAggregator, circle_radius_model::LogScaledRadius,
    };

    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_circle_merging() {
        let mut c1 = CircleOfPoints::new(
            Circle::from_coordinate(&Coordinate2D::new(1.0, 1.0), 1.0),
            1,
            Default::default(),
        )
        .unwrap();
        let c2 = CircleOfPoints::new(
            Circle::from_coordinate(&Coordinate2D::new(2.0, 1.0), 1.0),
            1,
            Default::default(),
        )
        .unwrap();

        let radius_model = LogScaledRadius::new(1.0, 0.).unwrap();

        c1.merge(&c2, &radius_model);

        assert_eq!(c1.number_of_points(), 2);
        assert_eq!(c1.circle.x(), 1.5);
        assert_eq!(c1.circle.y(), 1.0);
        assert_eq!(c1.circle.radius(), 1.0 + 2.0_f64.ln());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_circle_merging_with_attribute() {
        let mut c1 = CircleOfPoints::new(
            Circle::from_coordinate(&Coordinate2D::new(1.0, 1.0), 1.0),
            1,
            [(
                "foo".to_string(),
                AttributeAggregate::MeanNumber(MeanAggregator::from_value(42.)),
            )]
            .iter()
            .cloned()
            .collect(),
        )
        .unwrap();
        let c2 = CircleOfPoints::new(
            Circle::from_coordinate(&Coordinate2D::new(2.0, 1.0), 1.0),
            1,
            [(
                "foo".to_string(),
                AttributeAggregate::MeanNumber(MeanAggregator::from_value(44.)),
            )]
            .iter()
            .cloned()
            .collect(),
        )
        .unwrap();

        let radius_model = LogScaledRadius::new(1.0, 0.).unwrap();

        c1.merge(&c2, &radius_model);

        assert_eq!(c1.number_of_points(), 2);
        assert_eq!(c1.circle.x(), 1.5);
        assert_eq!(c1.circle.y(), 1.0);
        assert_eq!(c1.circle.radius(), 1.0 + 2.0_f64.ln());
        assert_eq!(
            c1.attribute_aggregates
                .get("foo")
                .unwrap()
                .mean_number()
                .unwrap()
                .mean,
            43.
        );
    }
}
