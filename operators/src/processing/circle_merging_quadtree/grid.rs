use std::collections::{hash_map::Entry, HashMap};

use geoengine_datatypes::primitives::{AxisAlignedRectangle, BoundingBox2D, Circle, Coordinate2D};

use super::{circle_of_points::CircleOfPoints, circle_radius_model::CircleRadiusModel};

///  A Grid that contains non-overlapping circles.
/// It merges circles automatically upon insert if there are multiples circles in a grid cell.
#[derive(Clone, Debug)]
pub struct Grid<C: CircleRadiusModel> {
    offset: Coordinate2D,
    cell_width: f64,
    number_of_horizontal_cells: usize,
    number_of_vertical_cells: usize,
    cells: HashMap<u16, CircleOfPoints>,
    radius_model: C,
}

impl<C: CircleRadiusModel> Grid<C> {
    pub fn new(bbox: BoundingBox2D, radius_model: C) -> Self {
        let cell_width = (2. * radius_model.min_radius() + radius_model.delta()).sqrt();

        let map_width = bbox.size_x();
        let map_height = bbox.size_y();

        let number_of_horizontal_cells = (map_width / cell_width).ceil() as usize;
        let number_of_vertical_cells = (map_height / cell_width).ceil() as usize;

        let offset_x = (bbox.lower_left().x / cell_width).floor() * cell_width;
        let offset_y = (bbox.lower_left().y / cell_width).floor() * cell_width;

        Self {
            offset: Coordinate2D {
                x: offset_x,
                y: offset_y,
            },
            cell_width,
            number_of_horizontal_cells,
            number_of_vertical_cells,
            cells: HashMap::new(),
            radius_model,
        }
    }

    pub fn insert(&mut self, circle_of_points: CircleOfPoints) {
        let grid_x = ((circle_of_points.circle.x() - self.offset.x) / self.cell_width) as usize;
        let grid_y = ((circle_of_points.circle.y() - self.offset.x) / self.cell_width) as usize;

        // uses XY-order, but could also use Z-order
        let grid_pos = grid_y * self.number_of_horizontal_cells + grid_x;

        match self.cells.entry(grid_pos as u16) {
            Entry::Vacant(entry) => {
                entry.insert(circle_of_points);
            }
            Entry::Occupied(mut entry) => {
                entry.get_mut().merge(&circle_of_points, &self.radius_model);
            }
        };
    }

    pub fn insert_coordinate(&mut self, coordinate: Coordinate2D) {
        let circle = Circle::from_coordinate(&coordinate, self.radius_model.min_radius());
        self.insert(CircleOfPoints::new_with_one_point(circle))
    }

    /// Creates a draining iterator that can be used to remove all circles from the grid.
    pub fn drain(self) -> impl Iterator<Item = CircleOfPoints> {
        self.cells.into_values()
    }

    pub fn radius_model(&self) -> &C {
        &self.radius_model
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::primitives::Circle;

    use crate::processing::circle_merging_quadtree::circle_radius_model::LogScaledRadius;

    use super::*;

    #[test]
    fn test_grid() {
        let mut grid = Grid::new(
            BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            LogScaledRadius::new(2., 1.).unwrap(),
        );

        grid.insert(CircleOfPoints::new_with_one_point(Circle::new(1., 1., 1.)));
        grid.insert(CircleOfPoints::new_with_one_point(Circle::new(2., 1., 1.)));
        grid.insert(CircleOfPoints::new_with_one_point(Circle::new(6., 6., 1.)));

        assert_eq!(
            grid.drain().collect::<Vec<_>>(),
            vec![
                CircleOfPoints::new_with_one_point(Circle::new(6., 6., 1.)),
                CircleOfPoints::new(Circle::new(1.5, 1., 2.693_147_180_559_945_4), 2).unwrap(),
            ]
        );
    }
}
