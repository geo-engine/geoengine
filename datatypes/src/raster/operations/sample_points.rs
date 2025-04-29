use num::range_inclusive;

use crate::{
    primitives::Coordinate2D,
    raster::{GridBoundingBox2D, GridIdx2D, GridSize, SpatialGridDefinition},
};

pub trait SamplePoints {
    type Coord;

    fn sample_outline(&self, step: usize) -> Vec<Self::Coord>;
    fn sample_cross(&self, step: usize) -> Vec<Self::Coord>;
    fn sample_diagonals(&self, step: usize) -> Vec<Self::Coord>;
}

impl SamplePoints for GridBoundingBox2D {
    type Coord = GridIdx2D;

    fn sample_outline(&self, step: usize) -> Vec<Self::Coord> {
        let [y_min, y_max] = self.y_bounds();
        let [x_min, x_max] = self.x_bounds();

        let x_range = range_inclusive(x_min, x_max);
        let y_range = range_inclusive(y_min, y_max);

        let capacity = (self.axis_size_x() / step) * 2 + (self.axis_size_y() / step) * 2;

        let mut collected: Vec<Self::Coord> = Vec::with_capacity(capacity);

        for x in x_range.step_by(step) {
            collected.push(GridIdx2D::new_y_x(y_min, x));
            collected.push(GridIdx2D::new_y_x(y_max, x));
        }

        for y in y_range.step_by(step) {
            collected.push(GridIdx2D::new_y_x(y, x_min));
            collected.push(GridIdx2D::new_y_x(y, x_max));
        }

        collected
    }

    fn sample_cross(&self, step: usize) -> Vec<Self::Coord> {
        let [y_min, y_max] = self.y_bounds();
        let [x_min, x_max] = self.x_bounds();
        let y_mid = y_min + (self.axis_size_y() / 2) as isize;
        let x_mid = x_min + (self.axis_size_x() / 2) as isize;

        let x_range = range_inclusive(x_min, x_max);
        let y_range = range_inclusive(y_min, y_max);

        let capacity = (self.axis_size_x() / step) + (self.axis_size_y() / step);

        let mut collected: Vec<Self::Coord> = Vec::with_capacity(capacity);

        for x in x_range.step_by(step) {
            collected.push(GridIdx2D::new_y_x(y_mid, x));
        }

        for y in y_range.step_by(step) {
            collected.push(GridIdx2D::new_y_x(y, x_mid));
        }

        collected
    }

    fn sample_diagonals(&self, step: usize) -> Vec<Self::Coord> {
        enum LongAxis {
            X,
            Y,
        }

        let [y_min, y_max] = self.y_bounds();
        let [x_min, x_max] = self.x_bounds();

        let x_range = range_inclusive(x_min, x_max);
        let y_range = range_inclusive(y_min, y_max);

        let capacity = (self.axis_size_x() / step) * 2 + (self.axis_size_y() / step) * 2;

        let (long_range_id, long_range, b, b_max, m) = if self.axis_size_x() > self.axis_size_y() {
            (
                LongAxis::X,
                x_range,
                y_min,
                y_max,
                (self.axis_size_y() as f32 / self.axis_size_x() as f32),
            )
        } else {
            (
                LongAxis::Y,
                y_range,
                x_min,
                x_max,
                (self.axis_size_x() as f32 / self.axis_size_y() as f32),
            )
        };

        let mut collected: Vec<Self::Coord> = Vec::with_capacity(capacity);

        for l in long_range {
            let s = (l as f32 * m) as isize + b;
            let s_inv = b_max - (l as f32 * m) as isize;

            match long_range_id {
                LongAxis::X => {
                    debug_assert!(l >= x_min);
                    debug_assert!(l <= x_max);
                    debug_assert!(s >= y_min);
                    debug_assert!(s <= y_max);
                    collected.push(GridIdx2D::new_y_x(s, l));
                    collected.push(GridIdx2D::new_y_x(s_inv, l));
                }
                LongAxis::Y => {
                    debug_assert!(s >= x_min);
                    debug_assert!(s <= x_max);
                    debug_assert!(l >= y_min);
                    debug_assert!(l <= y_max);
                    collected.push(GridIdx2D::new_y_x(l, s));
                    collected.push(GridIdx2D::new_y_x(l, s_inv));
                }
            }
        }

        collected
    }
}

impl SamplePoints for SpatialGridDefinition {
    type Coord = Coordinate2D;

    fn sample_outline(&self, step: usize) -> Vec<Self::Coord> {
        let px = self.grid_bounds.sample_outline(step);
        px.iter()
            .map(|gidx| {
                self.geo_transform
                    .grid_idx_to_pixel_upper_left_coordinate_2d(*gidx)
            })
            .collect()
    }

    fn sample_cross(&self, step: usize) -> Vec<Self::Coord> {
        let px = self.grid_bounds.sample_cross(step);
        px.iter()
            .map(|gidx| {
                self.geo_transform
                    .grid_idx_to_pixel_upper_left_coordinate_2d(*gidx)
            })
            .collect()
    }

    fn sample_diagonals(&self, step: usize) -> Vec<Self::Coord> {
        let px = self.grid_bounds.sample_diagonals(step);
        px.iter()
            .map(|gidx| {
                self.geo_transform
                    .grid_idx_to_pixel_upper_left_coordinate_2d(*gidx)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_sample_outline() {
        let gb = GridBoundingBox2D::new_min_max(0, 4, 0, 4).unwrap();
        let ds = gb.sample_outline(1);
        let exp = vec![
            GridIdx2D::new_y_x(0, 0),
            GridIdx2D::new_y_x(4, 0),
            GridIdx2D::new_y_x(0, 1),
            GridIdx2D::new_y_x(4, 1),
            GridIdx2D::new_y_x(0, 2),
            GridIdx2D::new_y_x(4, 2),
            GridIdx2D::new_y_x(0, 3),
            GridIdx2D::new_y_x(4, 3),
            GridIdx2D::new_y_x(0, 4),
            GridIdx2D::new_y_x(4, 4),
            GridIdx2D::new_y_x(0, 0),
            GridIdx2D::new_y_x(0, 4),
            GridIdx2D::new_y_x(1, 0),
            GridIdx2D::new_y_x(1, 4),
            GridIdx2D::new_y_x(2, 0),
            GridIdx2D::new_y_x(2, 4),
            GridIdx2D::new_y_x(3, 0),
            GridIdx2D::new_y_x(3, 4),
            GridIdx2D::new_y_x(4, 0),
            GridIdx2D::new_y_x(4, 4),
        ];
        assert_eq!(ds, exp);
    }

    #[test]
    fn test_sample_cross() {
        let gb = GridBoundingBox2D::new_min_max(0, 4, 0, 4).unwrap();
        let ds = gb.sample_cross(1);
        let exp = vec![
            GridIdx2D::new_y_x(2, 0),
            GridIdx2D::new_y_x(2, 1),
            GridIdx2D::new_y_x(2, 2),
            GridIdx2D::new_y_x(2, 3),
            GridIdx2D::new_y_x(2, 4),
            GridIdx2D::new_y_x(0, 2),
            GridIdx2D::new_y_x(1, 2),
            GridIdx2D::new_y_x(2, 2),
            GridIdx2D::new_y_x(3, 2),
            GridIdx2D::new_y_x(4, 2),
        ];
        assert_eq!(ds, exp);
    }

    #[test]
    fn test_sample_diagnals() {
        let gb = GridBoundingBox2D::new_min_max(0, 4, 0, 4).unwrap();
        let ds = gb.sample_diagonals(1);
        let exp = vec![
            GridIdx2D::new_y_x(0, 0),
            GridIdx2D::new_y_x(0, 4),
            GridIdx2D::new_y_x(1, 1),
            GridIdx2D::new_y_x(1, 3),
            GridIdx2D::new_y_x(2, 2),
            GridIdx2D::new_y_x(2, 2),
            GridIdx2D::new_y_x(3, 3),
            GridIdx2D::new_y_x(3, 1),
            GridIdx2D::new_y_x(4, 4),
            GridIdx2D::new_y_x(4, 0),
        ];
        assert_eq!(ds, exp);
    }

    #[test]
    fn test_sample_diagnals_non_symetric() {
        let gb = GridBoundingBox2D::new_min_max(0, 2, 0, 4).unwrap();
        let ds = gb.sample_diagonals(1);
        let exp = vec![
            GridIdx2D::new_y_x(0, 0),
            GridIdx2D::new_y_x(2, 0),
            GridIdx2D::new_y_x(0, 1),
            GridIdx2D::new_y_x(2, 1),
            GridIdx2D::new_y_x(1, 2),
            GridIdx2D::new_y_x(1, 2),
            GridIdx2D::new_y_x(1, 3),
            GridIdx2D::new_y_x(1, 3),
            GridIdx2D::new_y_x(2, 4),
            GridIdx2D::new_y_x(0, 4),
        ];
        assert_eq!(ds, exp);
    }
}
