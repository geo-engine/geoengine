use crate::{
    primitives::{AxisAlignedRectangle, Coordinate2D, SpatialPartition2D, SpatialPartitioned},
    util::test::TestDefault,
};

use super::{
    GeoTransform, GridBoundingBox2D, GridIdx, GridIdx2D, GridShape2D, GridShapeAccess, GridSize,
};

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

impl GridShapeAccess for TilingSpecification {
    type ShapeArray = [usize; 2];

    fn grid_shape_array(&self) -> Self::ShapeArray {
        self.tile_size_in_pixels.shape_array
    }

    fn grid_shape(&self) -> GridShape2D {
        self.tile_size_in_pixels
    }
}

impl TestDefault for TilingSpecification {
    fn test_default() -> Self {
        Self {
            origin_coordinate: Coordinate2D::new(0., 0.),
            tile_size_in_pixels: GridShape2D::new([512, 512]),
        }
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

    pub fn pixel_idx_to_tile_idx(&self, pixel_idx: GridIdx2D) -> GridIdx2D {
        let GridIdx([y_pixel_idx, x_pixel_idx]) = pixel_idx;
        let [y_tile_size, x_tile_size] = self.tile_size_in_pixels.into_inner();
        let y_tile_idx = (y_pixel_idx as f64 / y_tile_size as f64).floor() as isize;
        let x_tile_idx = (x_pixel_idx as f64 / x_tile_size as f64).floor() as isize;
        [y_tile_idx, x_tile_idx].into()
    }

    pub fn tile_grid_box(&self, partition: SpatialPartition2D) -> GridBoundingBox2D {
        let start = self.pixel_idx_to_tile_idx(self.geo_transform.upper_left_pixel_idx(&partition));
        let end = self.pixel_idx_to_tile_idx(self.geo_transform.lower_right_pixel_idx(&partition));
        GridBoundingBox2D::new_unchecked(start, end)
    }

    /// generates the tile idx in \[z,y,x\] order for the tiles intersecting the bounding box
    /// the iterator moves once along the x-axis and then increases the y-axis
    pub fn tile_idx_iterator(
        &self,
        partition: SpatialPartition2D,
    ) -> impl Iterator<Item = GridIdx2D> {
        let GridIdx([upper_left_tile_y, upper_left_tile_x]) =
            self.pixel_idx_to_tile_idx(self.geo_transform.upper_left_pixel_idx(&partition));

        let GridIdx([lower_right_tile_y, lower_right_tile_x]) =
            self.pixel_idx_to_tile_idx(self.geo_transform.lower_right_pixel_idx(&partition));

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
            .grid_idx_to_pixel_upper_left_coordinate_2d(self.global_upper_left_pixel_idx());

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
            .grid_idx_to_pixel_upper_left_coordinate_2d(self.global_upper_left_pixel_idx());
        let lower_right_coord = self
            .global_geo_transform
            .grid_idx_to_pixel_upper_left_coordinate_2d(self.global_lower_right_pixel_idx() + 1); // we need the border of the lower right pixel.
        SpatialPartition2D::new_unchecked(top_left_coord, lower_right_coord)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn it_generates_only_intersected_tiles() {
        let strat = TilingStrategy {
            tile_size_in_pixels: [600, 600].into(),
            geo_transform: GeoTransform::new(
                (0., 0.).into(),
                2.095_475_792_884_826_7E-8,
                -2.095_475_792_884_826_7E-8,
            ),
        };

        let partition = SpatialPartition2D::new(
            (12.477_738_261_222_84, 43.881_293_535_232_544).into(),
            (12.477_743_625_640_87, 43.881_288_170_814_514).into(),
        )
        .unwrap();

        let tiles = strat
            .tile_information_iterator(partition)
            .collect::<Vec<_>>();

        assert_eq!(tiles.len(), 2);

        for tile in tiles {
            assert!(partition.intersects(&tile.spatial_partition()));
        }
    }
}
