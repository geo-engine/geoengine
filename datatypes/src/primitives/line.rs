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

    pub fn interpolate_coord_from_start(&self, factor: f64) -> Coordinate2D {
        let l = self.end - self.start;
        self.start + l * factor
    }

    pub fn with_additional_equi_spaced_coords(self, n: i32) -> impl Iterator<Item = Coordinate2D> {
        (0..=n + 1).map(move |f| self.interpolate_coord_from_start(f64::from(f) / f64::from(n + 1)))
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

#[cfg(test)]
mod tests {
    use float_cmp::approx_eq;

    use super::*;

    #[test]
    fn new_line() {
        assert_eq!(
            Line::new((0., 0.).into(), (1., 1.).into()),
            Line {
                start: Coordinate2D { x: 0., y: 0. },
                end: Coordinate2D { x: 1., y: 1. }
            }
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn x_axis_lenght() {
        assert_eq!(
            Line::new((1., 1.).into(), (2., 2.).into()).x_axis_length(),
            1.
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn y_axis_lenght() {
        assert_eq!(
            Line::new((1., 1.).into(), (2., 2.).into()).y_axis_length(),
            1.
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn vector_length() {
        assert_eq!(
            Line::new((1., 1.).into(), (2., 2.).into()).vector_length(),
            2_f64.sqrt()
        );
    }

    #[test]
    fn interpolate_coordinate_00() {
        let line = Line::new((1., 1.).into(), (2., 2.).into());
        assert_eq!(line.interpolate_coord_from_start(0.), line.start);
    }

    #[test]
    fn interpolate_coordinate_05() {
        let line = Line::new((1., 1.).into(), (2., 2.).into());
        assert_eq!(
            line.interpolate_coord_from_start(0.5),
            Coordinate2D::new(1.5, 1.5)
        );
    }

    #[test]
    fn interpolate_coordinate_m05() {
        let line = Line::new((1., 1.).into(), (2., 2.).into());
        assert_eq!(
            line.interpolate_coord_from_start(-0.5),
            Coordinate2D::new(0.5, 0.5)
        );
    }

    #[test]
    fn interpolate_coordinate_70() {
        let line = Line::new((1., 1.).into(), (2., 2.).into());
        assert_eq!(
            line.interpolate_coord_from_start(7.),
            Coordinate2D::new(8., 8.)
        );
    }

    #[test]
    fn interpolate_coordinate_m70() {
        let line = Line::new((1., 1.).into(), (2., 2.).into());
        assert_eq!(
            line.interpolate_coord_from_start(-7.),
            Coordinate2D::new(-6., -6.)
        );
    }

    #[test]
    fn equi_spaced_coordinates_0() {
        let start = (1., 1.).into();
        let end = (2., 2.).into();
        let line = Line::new(start, end);
        let coords: Vec<Coordinate2D> = line.with_additional_equi_spaced_coords(0).collect();

        assert_eq!(&coords, &[start, end]);
    }

    #[test]
    fn equi_spaced_coordinates_1() {
        let start = (1., 1.).into();
        let end = (2., 2.).into();
        let line = Line::new(start, end);
        let coords: Vec<Coordinate2D> = line.with_additional_equi_spaced_coords(1).collect();

        assert_eq!(&coords, &[start, Coordinate2D::new(1.5, 1.5), end]);
    }

    #[test]
    fn equi_spaced_coordinates_2() {
        let start = (1., 1.).into();
        let end = (2., 2.).into();
        let line = Line::new(start, end);
        let coords: Vec<Coordinate2D> = line.with_additional_equi_spaced_coords(2).collect();

        let expected = &[
            start,
            Coordinate2D::new(4. / 3., 4. / 3.),
            Coordinate2D::new(5. / 3., 5. / 3.),
            end,
        ];
        assert_eq!(coords.len(), expected.len());
        coords.iter().zip(expected.iter()).for_each(|(&a, &b)| {
            approx_eq!(f64, a.x, b.x);
            approx_eq!(f64, a.y, b.y);
        });
    }

    #[test]
    fn spatial_bounds() {
        let expected = BoundingBox2D::new_unchecked((0., 0.).into(), (1., 1.).into());
        let l = Line {
            start: (0., 1.).into(),
            end: (1., 0.).into(),
        };

        assert_eq!(l.spatial_bounds(), expected);
    }
}
