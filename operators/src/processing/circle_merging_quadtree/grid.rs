use geoengine_datatypes::primitives::{AxisAlignedRectangle, BoundingBox2D, Coordinate2D};

use super::{
    circle_of_points::CircleOfPoints, circle_radius_model::CircleRadiusModel,
    hash_map::SeparateChainingHashMap,
};

/// A Grid that contains non-overlapping circles.
/// It merges circles automatically upon insert if there are multiples circles in a grid cell.
#[derive(Clone, Debug)]
pub struct Grid<C: CircleRadiusModel> {
    offset: Coordinate2D,
    cell_width: f64,
    number_of_horizontal_cells: usize,
    number_of_vertical_cells: usize,
    cells: SeparateChainingHashMap<u16, CircleOfPoints>,
    radius_model: C,
}

impl<C: CircleRadiusModel> Grid<C> {
    pub fn new(bbox: BoundingBox2D, radius_model: C) -> Self {
        // cell width s.th. every circle in the cell overlaps with every other circle in the cell
        let cell_width =
            (2. * radius_model.min_radius() + radius_model.delta()) / std::f64::consts::SQRT_2;

        let map_width = bbox.size_x();
        let map_height = bbox.size_y();

        let mut number_of_horizontal_cells = (map_width / cell_width).ceil() as usize;
        let mut number_of_vertical_cells = (map_height / cell_width).ceil() as usize;

        if (number_of_horizontal_cells * number_of_vertical_cells) > 256 * 256 {
            // cap the number of cells to fit in a u16
            number_of_horizontal_cells = number_of_horizontal_cells.max(256);
            number_of_vertical_cells = number_of_vertical_cells.max(256);
        }

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
            cells: SeparateChainingHashMap::new(),
            radius_model,
        }
    }

    pub fn insert(&mut self, circle_of_points: CircleOfPoints) {
        let grid_x = ((circle_of_points.circle.x() - self.offset.x) / self.cell_width) as usize;
        let grid_y = ((circle_of_points.circle.y() - self.offset.x) / self.cell_width) as usize;

        // uses XY-order, but could also use Z-order
        let grid_pos = grid_y * self.number_of_horizontal_cells + grid_x;

        match self.cells.entry(grid_pos as u16) {
            super::hash_map::ValueRef::Vacant(entry_pos) => {
                self.cells.insert_unchecked(entry_pos, circle_of_points);
            }
            super::hash_map::ValueRef::Occupied(matched_circle_of_points) => {
                matched_circle_of_points.merge(&circle_of_points, &self.radius_model);
            }
        }
    }

    /// Creates a draining iterator that can be used to remove all circles from the grid.
    pub fn drain(self) -> impl Iterator<Item = CircleOfPoints> {
        self.cells.into_iter()
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

        grid.insert(CircleOfPoints::new_with_one_point(
            Circle::new(1., 1., 1.),
            Default::default(),
        ));
        grid.insert(CircleOfPoints::new_with_one_point(
            Circle::new(2., 1., 1.),
            Default::default(),
        ));
        grid.insert(CircleOfPoints::new_with_one_point(
            Circle::new(6., 6., 1.),
            Default::default(),
        ));

        assert_eq!(
            grid.drain().collect::<Vec<_>>(),
            vec![
                CircleOfPoints::new(
                    Circle::new(1.5, 1., 2.693_147_180_559_945_4),
                    2,
                    Default::default(),
                )
                .unwrap(),
                CircleOfPoints::new_with_one_point(Circle::new(6., 6., 1.), Default::default(),),
            ]
        );
    }

    // TODO: test dataset with close-by points and compare with same dataset after raster vector join
}
