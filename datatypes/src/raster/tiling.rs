use crate::primitives::{
    AxisAlignedRectangle, Coordinate2D, SpatialPartition2D, SpatialPartitioned,
};

use super::{GeoTransform, GridBoundingBox2D, GridIdx, GridIdx2D, GridShape2D, GridSize};

use serde::{Deserialize, Serialize};

/// The static parameters of a `TilingStrategy`
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct TilingSpecification {
    pub origin_coordinate: Coordinate2D,
    pub tile_size_in_pixels: GridShape2D,
}

impl TilingSpecification {
    pub fn new(origin_coordinate: Coordinate2D, tile_size_in_pixels: GridShape2D) -> Self {
        Self {
            origin_coordinate,
            tile_size_in_pixels,
        }
    }

    /// create a `TilingStrategy` from self and pixel sizes
    pub fn strategy(self, x_pixel_size: f64, y_pixel_size: f64) -> TilingStrategy {
        debug_assert!(x_pixel_size > 0.0);
        debug_assert!(y_pixel_size < 0.0);

        TilingStrategy::new_with_tiling_spec(self, x_pixel_size, y_pixel_size)
    }
}

/// A provider of tile (size) information for a raster/grid
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct TilingStrategy {
    pub tile_size_in_pixels: GridShape2D,
    pub geo_transform: GeoTransform,
}

impl TilingStrategy {
    pub fn new(tile_pixel_size: GridShape2D, geo_transform: GeoTransform) -> Self {
        Self {
            tile_size_in_pixels: tile_pixel_size,
            geo_transform,
        }
    }

    pub fn new_with_tiling_spec(
        tiling_specification: TilingSpecification,
        x_pixel_size: f64,
        y_pixel_size: f64,
    ) -> Self {
        Self {
            tile_size_in_pixels: tiling_specification.tile_size_in_pixels,
            geo_transform: GeoTransform::new(
                tiling_specification.origin_coordinate,
                x_pixel_size,
                y_pixel_size,
            ),
        }
    }

    /// compute the index of the upper left pixel that is contained in the `partition`
    pub fn upper_left_pixel_idx(&self, partition: SpatialPartition2D) -> GridIdx2D {
        self.geo_transform
            .coordinate_to_grid_idx_2d(partition.upper_left())
    }

    /// compute the index of the lower right pixel that is contained in the `partition`
    pub fn lower_right_pixel_idx(&self, partition: SpatialPartition2D) -> GridIdx2D {
        // as the lower right coordinate is not included in the partition we subtract an epsilon
        // in order to not include the next pixel if the lower right coordinate is exactly on the
        // edge of the next pixel

        // choose the epsilon relative to the pixel size
        const EPSILON: f64 = 0.000_001;
        let epsilon: Coordinate2D = (
            self.geo_transform.x_pixel_size() * EPSILON,
            self.geo_transform.y_pixel_size() * EPSILON,
        )
            .into();

        // shift lower right by epsilon
        let lower_right = partition.lower_right() - epsilon;

        // ensure we don't accidentally go beyond the upper left pixel
        let lower_right = (
            lower_right.x.max(partition.upper_left().x),
            lower_right.y.min(partition.upper_left().y),
        )
            .into();

        self.geo_transform.coordinate_to_grid_idx_2d(lower_right)
    }

    pub fn pixel_idx_to_tile_idx(&self, pixel_idx: GridIdx2D) -> GridIdx2D {
        let GridIdx([y_pixel_idx, x_pixel_idx]) = pixel_idx;
        let [y_tile_size, x_tile_size] = self.tile_size_in_pixels.into_inner();
        let y_tile_idx = (y_pixel_idx as f32 / y_tile_size as f32).floor() as isize;
        let x_tile_idx = (x_pixel_idx as f32 / x_tile_size as f32).floor() as isize;
        [y_tile_idx, x_tile_idx].into()
    }

    pub fn tile_grid_box(&self, partition: SpatialPartition2D) -> GridBoundingBox2D {
        let start = self.pixel_idx_to_tile_idx(self.upper_left_pixel_idx(partition));
        let end = self.pixel_idx_to_tile_idx(self.lower_right_pixel_idx(partition));
        GridBoundingBox2D::new_unchecked(start, end)
    }

    /// generates the tile idx in \[z,y,x\] order for the tiles intersecting the bounding box
    /// the iterator moves once along the x-axis and then increases the y-axis
    pub fn tile_idx_iterator(
        &self,
        partition: SpatialPartition2D,
    ) -> impl Iterator<Item = GridIdx2D> {
        let GridIdx([upper_left_tile_y, upper_left_tile_x]) =
            self.pixel_idx_to_tile_idx(self.upper_left_pixel_idx(partition));

        let GridIdx([lower_right_tile_y, lower_right_tile_x]) =
            self.pixel_idx_to_tile_idx(self.lower_right_pixel_idx(partition));

        let y_range = upper_left_tile_y..=lower_right_tile_y;
        let x_range = upper_left_tile_x..=lower_right_tile_x;

        y_range.flat_map(move |y_tile| x_range.clone().map(move |x_tile| [y_tile, x_tile].into()))
    }

    /// generates the tile information for the tiles intersecting the bounding box
    /// the iterator moves once along the x-axis and then increases the y-axis
    pub fn tile_information_iterator(
        &self,
        partition: SpatialPartition2D,
    ) -> impl Iterator<Item = TileInformation> {
        let tile_pixel_size = self.tile_size_in_pixels;
        let geo_transform = self.geo_transform;
        self.tile_idx_iterator(partition)
            .map(move |idx| TileInformation::new(idx, tile_pixel_size, geo_transform))
    }
}

/// The `TileInformation` is used to represent the spatial position of each tile
#[derive(PartialEq, Debug, Copy, Clone, Serialize, Deserialize)]
pub struct TileInformation {
    pub tile_size_in_pixels: GridShape2D,
    pub global_tile_position: GridIdx2D,
    pub global_geo_transform: GeoTransform,
}

impl TileInformation {
    pub fn new(
        global_tile_position: GridIdx2D,
        tile_size_in_pixels: GridShape2D,
        global_geo_transform: GeoTransform,
    ) -> Self {
        Self {
            tile_size_in_pixels,
            global_tile_position,
            global_geo_transform,
        }
    }

    pub fn with_partition_and_shape(partition: SpatialPartition2D, shape: GridShape2D) -> Self {
        Self {
            tile_size_in_pixels: shape,
            global_tile_position: [0, 0].into(),
            global_geo_transform: GeoTransform::new(
                partition.upper_left(),
                partition.size_x() / shape.axis_size_x() as f64,
                -partition.size_y() / shape.axis_size_y() as f64,
            ),
        }
    }

    #[allow(clippy::unused_self)]
    pub fn local_upper_left_pixel_idx(&self) -> GridIdx2D {
        [0, 0].into()
    }

    pub fn local_lower_left_pixel_idx(&self) -> GridIdx2D {
        [self.tile_size_in_pixels.axis_size_y() as isize - 1, 0].into()
    }

    pub fn local_upper_right_pixel_idx(&self) -> GridIdx2D {
        [0, self.tile_size_in_pixels.axis_size_x() as isize - 1].into()
    }

    pub fn local_lower_right_pixel_idx(&self) -> GridIdx2D {
        let GridIdx([y, _]) = self.local_lower_left_pixel_idx();
        let GridIdx([_, x]) = self.local_upper_right_pixel_idx();
        [y, x].into()
    }

    pub fn global_tile_position(&self) -> GridIdx2D {
        self.global_tile_position
    }

    pub fn global_upper_left_pixel_idx(&self) -> GridIdx2D {
        let [tile_size_y, tile_size_x] = self.tile_size_in_pixels.into_inner();
        self.global_tile_position() * [tile_size_y as isize, tile_size_x as isize]
    }

    pub fn global_upper_right_pixel_idx(&self) -> GridIdx2D {
        self.global_upper_left_pixel_idx() + self.local_upper_right_pixel_idx()
    }

    pub fn global_lower_right_pixel_idx(&self) -> GridIdx2D {
        self.global_upper_left_pixel_idx() + self.local_lower_right_pixel_idx()
    }

    pub fn global_lower_left_pixel_idx(&self) -> GridIdx2D {
        self.global_upper_left_pixel_idx() + self.local_lower_left_pixel_idx()
    }

    pub fn tile_size_in_pixels(&self) -> GridShape2D {
        self.tile_size_in_pixels
    }

    pub fn local_to_global_pixel_idx(&self, local_pixel_position: GridIdx2D) -> GridIdx2D {
        self.global_upper_left_pixel_idx() + local_pixel_position
    }

    pub fn tile_geo_transform(&self) -> GeoTransform {
        let tile_upper_left_coord = self
            .global_geo_transform
            .grid_idx_to_upper_left_coordinate_2d(self.global_upper_left_pixel_idx());

        GeoTransform::new(
            tile_upper_left_coord,
            self.global_geo_transform.x_pixel_size(),
            self.global_geo_transform.y_pixel_size(),
        )
    }
}

impl SpatialPartitioned for TileInformation {
    fn spatial_partition(&self) -> SpatialPartition2D {
        let top_left_coord = self
            .global_geo_transform
            .grid_idx_to_upper_left_coordinate_2d(self.global_upper_left_pixel_idx());
        let lower_right_coord = self
            .global_geo_transform
            .grid_idx_to_upper_left_coordinate_2d(self.global_lower_right_pixel_idx() + 1); // we need the border of the lower right pixel.
        SpatialPartition2D::new_unchecked(top_left_coord, lower_right_coord)
    }
}

#[cfg(test)]
mod tests {
    use crate::util::test::TestDefault;

    use super::*;

    #[test]
    fn lower_right_pixel_index_edge() {
        let strat = TilingStrategy {
            tile_size_in_pixels: [600, 600].into(),
            geo_transform: GeoTransform::test_default(),
        };

        let partition = SpatialPartition2D::new((1., 1.).into(), (8., -8.).into()).unwrap();
        assert_eq!(strat.lower_right_pixel_idx(partition), [7, 7].into());

        let partition = SpatialPartition2D::new((1., 1.).into(), (8.5, -8.).into()).unwrap();
        assert_eq!(strat.lower_right_pixel_idx(partition), [7, 8].into());

        let partition = SpatialPartition2D::new((1., 1.).into(), (8., -8.5).into()).unwrap();
        assert_eq!(strat.lower_right_pixel_idx(partition), [8, 7].into());
    }

    #[test]
    fn lower_right_pixel_index_inside() {
        let strat = TilingStrategy {
            tile_size_in_pixels: [600, 600].into(),
            geo_transform: GeoTransform::test_default(),
        };

        let partition = SpatialPartition2D::new((1., 1.).into(), (7.5, -7.5).into()).unwrap();
        assert_eq!(strat.lower_right_pixel_idx(partition), [7, 7].into());
    }
}
