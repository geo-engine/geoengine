use super::{BoundingBox2D, Coordinate2D, SpatialBounded};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Line {
    pub start: Coordinate2D,
    pub end: Coordinate2D,
}

impl Line {
    pub fn new(start: Coordinate2D, end: Coordinate2D) -> Self {
        Line { start, end }
    }

    #[inline]
    pub fn x_axis_length(&self) -> f64 {
        self.end.x - self.start.x
    }

    #[inline]
    pub fn y_axis_length(&self) -> f64 {
        self.end.y - self.start.y
    }

    #[inline]
    pub fn vector_length(&self) -> f64 {
        let v_x = self.x_axis_length();
        let v_y = self.y_axis_length();

        ((v_x * v_x) + (v_y * v_y)).sqrt()
    }

    pub fn interpolate_coordinate(&self, factor: f64) -> Coordinate2D {
        let l = self.end - self.start;
        self.start + l * factor
    }

    pub fn equi_spaced_coordinates(self, n: i32) -> impl Iterator<Item = Coordinate2D> {
        (0..n + 1).map(move |f| self.interpolate_coordinate(f as f64 / n as f64))
    }
}

impl SpatialBounded for Line {
    fn spatial_bounds(&self) -> BoundingBox2D {
        BoundingBox2D::new_unchecked(
            self.start.min_elements(self.end),
            self.start.max_elements(self.end),
        )
    }
}
