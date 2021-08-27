use geo::intersects::Intersects;
use num_traits::abs;

use crate::operations::Contains;

use super::{AxisAlignedRectangle, BoundingBox2D, Coordinate2D};

/// A circle consisting of a center [`Coordinate2D`] and a radius
#[derive(Clone, Debug, PartialEq)]
pub struct Circle {
    center: Coordinate2D,
    radius: f64,
}

impl Circle {
    pub fn new(x: f64, y: f64, radius: f64) -> Self {
        Circle {
            center: (x, y).into(),
            radius,
        }
    }

    pub fn from_coordinate(coordinate: &Coordinate2D, radius: f64) -> Self {
        Circle {
            center: *coordinate,
            radius,
        }
    }

    pub fn intersects(&self, other: &Circle) -> bool {
        self.center.euclidean_distance(&other.center) < (self.radius + other.radius)
    }

    pub fn intersects_with_epsilon(&self, other: &Circle, epsilon_distance: f64) -> bool {
        self.center.euclidean_distance(&other.center)
            < (self.radius + other.radius + epsilon_distance)
    }

    pub fn contains_coordinate(&self, other: &Coordinate2D) -> bool {
        let distance_x = abs(self.center.x - other.x);
        let distance_y = abs(self.center.y - other.y);

        // outside bounding box
        if distance_x > self.radius {
            return false;
        }
        if distance_y > self.radius {
            return false;
        }

        // inside square diamond
        if (distance_x + distance_y) <= self.radius {
            return true;
        }

        // expensive test
        self.center.euclidean_distance(other) <= self.radius
    }

    /// Calculates if the rectangle is completely inside the circle
    pub fn contains_bbox(&self, bbox: &BoundingBox2D) -> bool {
        let corner_coordinates = [
            bbox.lower_left(),
            bbox.upper_right(),
            bbox.upper_left(),
            bbox.lower_right(),
        ];

        for coordinate in corner_coordinates {
            if !self.contains_coordinate(&coordinate) {
                return false;
            }
        }

        true
    }

    // calculates the distance between the circle center and a point
    pub fn center_distance(&self, other: &Coordinate2D) -> f64 {
        self.center.euclidean_distance(other) - self.radius
    }

    pub fn x(&self) -> f64 {
        self.center.x
    }

    pub fn y(&self) -> f64 {
        self.center.y
    }

    pub fn center(&self) -> Coordinate2D {
        self.center
    }

    pub fn radius(&self) -> f64 {
        self.radius
    }

    pub fn diameter(&self) -> f64 {
        2.0 * self.radius
    }

    pub fn area(&self) -> f64 {
        self.radius * self.radius * std::f64::consts::PI
    }

    /// Enlarges the circle by the given delta
    pub fn buffer(&self, delta: f64) -> Self {
        Circle {
            center: self.center,
            radius: self.radius + delta,
        }
    }
}

impl Contains<Circle> for BoundingBox2D {
    fn contains(&self, other: &Circle) -> bool {
        let half_width = self.size_x() / 2.;
        let x_center = self.lower_left().x + half_width;
        let x_center_dist = (x_center - other.x()).abs();

        if x_center_dist > (half_width - other.radius()) {
            return false;
        }

        let half_height = self.size_y() / 2.;
        let y_center = self.lower_left().y + half_height;
        let y_center_dist = (y_center - other.y()).abs();

        if y_center_dist > (half_height - other.radius()) {
            return false;
        }

        true
    }
}

impl Intersects<Circle> for BoundingBox2D {
    fn intersects(&self, other: &Circle) -> bool {
        let half_width = self.size_x() / 2.;
        let x_center = self.lower_left().x + half_width;

        let circle_distance_x = (x_center - other.x()).abs();
        if circle_distance_x > half_width + other.radius() {
            return false;
        }

        let half_height = self.size_y() / 2.;
        let y_center = self.lower_left().y + half_height;

        let circle_distance_y = (y_center - other.y()).abs();
        if circle_distance_y > half_height + other.radius() {
            return false;
        }

        if circle_distance_x <= half_width {
            return true;
        }
        if circle_distance_y <= half_height {
            return true;
        }

        let squared_corner_distanz =
            (circle_distance_x - half_width).powi(2) + (circle_distance_y - half_height).powi(2);

        squared_corner_distanz <= other.radius().powi(2)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn same_x() {
        let circle_zero = Circle::new(0.0, 0.0, 5.0);
        let circle_five = Circle::new(5.0, 0.0, 5.0);
        let circle_ten = Circle::new(10.0, 0.0, 5.0);
        let circle_minus_five = Circle::new(-5.0, 0.0, 5.0);
        let circle_minus_ten = Circle::new(-10.0, 0.0, 5.0);

        assert!(circle_zero.intersects(&circle_zero));
        assert!(circle_zero.intersects(&circle_five));
        assert!(circle_zero.intersects(&circle_minus_five));
        assert!(!circle_zero.intersects(&circle_ten));
        assert!(!circle_zero.intersects(&circle_minus_ten));
    }

    #[test]
    fn same_y() {
        let circle_zero = Circle::new(0.0, 0.0, 5.0);
        let circle_five = Circle::new(0.0, 5.0, 5.0);
        let circle_ten = Circle::new(0.0, 10.0, 5.0);
        let circle_minus_five = Circle::new(0.0, -5.0, 5.0);
        let circle_minus_ten = Circle::new(0.0, -10.0, 5.0);

        assert!(circle_zero.intersects(&circle_zero));
        assert!(circle_zero.intersects(&circle_five));
        assert!(circle_zero.intersects(&circle_minus_five));
        assert!(!circle_zero.intersects(&circle_ten));
        assert!(!circle_zero.intersects(&circle_minus_ten));
    }

    #[test]
    fn diagonal() {
        let circle_zero = Circle::new(0.0, 0.0, 5.0);
        let circle_in = Circle::new(7.0, 7.0, 5.0);
        let circle_out = Circle::new(7.1, 7.1, 5.0);

        assert!(circle_zero.intersects(&circle_zero));
        assert!(circle_zero.intersects(&circle_in));
        assert!(!circle_zero.intersects(&circle_out));
    }

    #[test]
    fn epsilon() {
        let circle_zero = Circle::new(0.0, 0.0, 5.0);
        let circle_in = Circle::new(10.9, 0.0, 5.0);
        let circle_out = Circle::new(11.0, 0.0, 5.0);

        assert!(circle_zero.intersects_with_epsilon(&circle_zero, 1.0));
        assert!(circle_zero.intersects_with_epsilon(&circle_in, 1.0));
        assert!(!circle_zero.intersects_with_epsilon(&circle_out, 1.0));
    }

    #[test]
    fn contains_coordinate() {
        let circle = Circle::new(0.0, 0.0, 1.0);

        assert!(circle.contains_coordinate(&Coordinate2D::new(0.0, 0.0)));

        assert!(circle.contains_coordinate(&Coordinate2D::new(0.0, 1.0)));
        assert!(circle.contains_coordinate(&Coordinate2D::new(-1.0, 0.0)));

        assert!(circle.contains_coordinate(&Coordinate2D::new(0.5, 0.5)));

        assert!(!circle.contains_coordinate(&Coordinate2D::new(1.0, 1.0)));
        assert!(!circle.contains_coordinate(&Coordinate2D::new(2_f64.sqrt() + 0.1, 2_f64.sqrt())));
    }

    #[test]
    fn contains_coordinate_nonuniform() {
        let radius = 2.0;
        let circle = Circle::new(0.0, 0.0, radius);

        assert!(circle.contains_coordinate(&Coordinate2D::new(0.0, 0.0)));

        assert!(circle.contains_coordinate(&Coordinate2D::new(0.0, radius)));
        assert!(circle.contains_coordinate(&Coordinate2D::new(-radius, 0.0)));

        assert!(circle.contains_coordinate(&Coordinate2D::new(radius / 2.0, radius / 2.0)));

        assert!(!circle.contains_coordinate(&Coordinate2D::new(radius, radius)));
        assert!(!circle.contains_coordinate(&Coordinate2D::new(0.0, radius + 0.0001)));
        assert!(!circle.contains_coordinate(&Coordinate2D::new(-radius - 0.0001, 0.0)));
        assert!(!circle.contains_coordinate(&Coordinate2D::new(
            (2.0 * radius).sqrt() + 0.001,
            (2.0 * radius).sqrt()
        )));
    }

    #[test]
    fn test_bbox_contains_with_delta() {
        let bbox = BoundingBox2D::new((-50., -50.).into(), (50., 50.).into()).unwrap();

        assert!(bbox.contains(&Circle::new(0.0, 0.0, 49.0).buffer(1.0)));
        assert!(!bbox.contains(&Circle::new(0.0, 0.0, 49.1).buffer(1.0)));

        assert!(bbox.contains(&Circle::new(44.0, 0.0, 5.0).buffer(1.0)));
        assert!(!bbox.contains(&Circle::new(44.1, 0.0, 5.0).buffer(1.0)));
    }

    #[test]
    fn test_bbox_intersects_with_delta() {
        let bbox = BoundingBox2D::new((-50., -50.).into(), (50., 50.).into()).unwrap();

        assert!(bbox.intersects(&Circle::new(0.0, 0.0, 49.0).buffer(1.0)));
        assert!(bbox.intersects(&Circle::new(0.0, 0.0, 49.1).buffer(1.0)));

        assert!(bbox.intersects(&Circle::new(44.0, 0.0, 5.0).buffer(1.0)));
        assert!(bbox.intersects(&Circle::new(44.1, 0.0, 5.0).buffer(1.0)));

        assert!(bbox.intersects(&Circle::new(56.0, 0.0, 5.0).buffer(1.0)));
        assert!(!bbox.intersects(&Circle::new(56.1, 0.0, 5.0).buffer(1.0)));
    }
}
