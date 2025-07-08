use super::{
    GeoTransform, GridBoundingBox2D, GridIdx, GridIdx2D, GridShape2D, GridShapeAccess, GridSize,
    SpatialGridDefinition,
};
use crate::{
    primitives::{
        Coordinate2D, RasterSpatialQueryRectangle, SpatialPartition2D, SpatialPartitioned,
    },
    raster::GridBounds,
    util::test::TestDefault,
};
use serde::{Deserialize, Serialize};

/// The static parameters required to create a `TilingStrategy`
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct TilingSpecification {
    pub tile_size_in_pixels: GridShape2D,
}

impl TilingSpecification {
    pub fn new(tile_size_in_pixels: GridShape2D) -> Self {
        Self {
            tile_size_in_pixels,
        }
    }

    #[allow(clippy::unused_self)]
    pub fn tiling_origin_reference(&self) -> Coordinate2D {
        Coordinate2D::new(0., 0.)
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

impl From<TilingSpecification> for GridShape2D {
    fn from(val: TilingSpecification) -> Self {
        val.tile_size_in_pixels
    }
}

impl TestDefault for TilingSpecification {
    fn test_default() -> Self {
        Self {
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

    pub fn pixel_idx_to_tile_idx(&self, pixel_idx: GridIdx2D) -> GridIdx2D {
        let GridIdx([y_pixel_idx, x_pixel_idx]) = pixel_idx;
        let [y_tile_size, x_tile_size] = self.tile_size_in_pixels.into_inner();
        //let y_tile_idx = (y_pixel_idx as f64 / y_tile_size as f64).floor() as isize;
        //let x_tile_idx = (x_pixel_idx as f64 / x_tile_size as f64).floor() as isize;
        let y_tile_idx = num::integer::div_floor(y_pixel_idx, y_tile_size as isize);
        let x_tile_idx = num::integer::div_floor(x_pixel_idx, x_tile_size as isize);
        [y_tile_idx, x_tile_idx].into()
    }

    pub fn tile_grid_box(&self, partition: SpatialPartition2D) -> GridBoundingBox2D {
        let start = self.pixel_idx_to_tile_idx(self.geo_transform.upper_left_pixel_idx(&partition));
        let end = self.pixel_idx_to_tile_idx(self.geo_transform.lower_right_pixel_idx(&partition));
        GridBoundingBox2D::new_unchecked(start, end)
    }

    pub fn global_pixel_grid_bounds_to_tile_grid_bounds(
        &self,
        global_pixel_grid_bounds: GridBoundingBox2D,
    ) -> GridBoundingBox2D {
        let start = self.pixel_idx_to_tile_idx(global_pixel_grid_bounds.min_index());
        let end = self.pixel_idx_to_tile_idx(global_pixel_grid_bounds.max_index());
        GridBoundingBox2D::new_unchecked(start, end)
    }

    /// Transforms a tile position into a global pixel position
    pub fn tile_idx_to_global_pixel_idx(&self, tile_idx: GridIdx2D) -> GridIdx2D {
        let GridIdx([y_tile_idx, x_tile_idx]) = tile_idx;
        GridIdx::new([
            y_tile_idx * self.tile_size_in_pixels.axis_size_y() as isize,
            x_tile_idx * self.tile_size_in_pixels.axis_size_x() as isize,
        ])
    }

    /// Returns the tile grid bounds for the given `raster_spatial_query`.
    /// The query must match the tiling strategy's geo transform for now.
    ///
    /// # Panics
    /// If the query's geo transform does not match the tiling strategy's geo transform.
    ///
    pub fn raster_spatial_query_to_tiling_grid_box(
        &self,
        raster_spatial_query: &RasterSpatialQueryRectangle,
    ) -> GridBoundingBox2D {
        self.global_pixel_grid_bounds_to_tile_grid_bounds(raster_spatial_query.grid_bounds())
    }

    /// Returns an iterator over all tile indices that intersect with the given `grid_bounds`.
    pub fn tile_idx_iterator_from_grid_bounds(
        // TODO: indicate that this uses pixel bounds!
        &self,
        grid_bounds: GridBoundingBox2D,
    ) -> impl Iterator<Item = GridIdx2D> + use<> {
        let tile_bounds = self.global_pixel_grid_bounds_to_tile_grid_bounds(grid_bounds);

        let y_range = tile_bounds.y_min()..=tile_bounds.y_max();
        let x_range = tile_bounds.x_min()..=tile_bounds.x_max();

        y_range.flat_map(move |y| x_range.clone().map(move |x| [y, x].into()))
    }

    /// generates the tile information for the tiles intersecting the bounding box
    /// the iterator moves once along the x-axis and then increases the y-axis
    pub fn tile_information_iterator_from_grid_bounds(
        // TODO: indicate that this uses pixel bounds!
        &self,
        grid_bounds: GridBoundingBox2D,
    ) -> impl Iterator<Item = TileInformation> + use<> {
        let tile_pixel_size = self.tile_size_in_pixels;
        let geo_transform = self.geo_transform;
        self.tile_idx_iterator_from_grid_bounds(grid_bounds)
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

    pub fn global_pixel_bounds(&self) -> GridBoundingBox2D {
        GridBoundingBox2D::new_unchecked(
            self.global_upper_left_pixel_idx(),
            self.global_lower_right_pixel_idx(),
        )
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

    pub fn spatial_grid_definition(&self) -> SpatialGridDefinition {
        SpatialGridDefinition::new(self.global_geo_transform, self.global_pixel_bounds())
    }

    pub fn tiling_strategy(&self) -> TilingStrategy {
        TilingStrategy::new(self.tile_size_in_pixels, self.global_geo_transform)
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
    use crate::raster::GridIntersection;

    #[test]
    fn it_generates_only_intersected_tiles() {
        let origin_coordinate = (0., 0.).into();

        let geo_transform = GeoTransform::new(
            origin_coordinate,
            2.095_475_792_884_826_7E-8,
            -2.095_475_792_884_826_7E-8,
        );

        let strat = TilingStrategy {
            tile_size_in_pixels: [600, 600].into(),
            geo_transform,
        };

        let ul_idx = strat
            .geo_transform
            .coordinate_to_grid_idx_2d((12.477_738_261_222_84, 43.881_293_535_232_544).into());

        let lr_idx = strat
            .geo_transform
            .coordinate_to_grid_idx_2d((12.477_743_625_640_87, 43.881_288_170_814_514).into());

        let grid_bounds = GridBoundingBox2D::new_unchecked(ul_idx, lr_idx);

        let tiles = strat
            .tile_information_iterator_from_grid_bounds(grid_bounds)
            .collect::<Vec<_>>();

        assert_eq!(tiles.len(), 2);

        for tile in tiles {
            assert!(grid_bounds.intersects(&tile.global_pixel_bounds()));
        }
    }

    #[test]
    fn it_generates_all_interesected_tiles() {
        let strat = TilingStrategy {
            tile_size_in_pixels: [512, 512].into(),
            geo_transform: GeoTransform::new((0., -0.).into(), 10., -10.),
        };

        let bounds =
            GridBoundingBox2D::new(GridIdx2D::new([-513, -513]), GridIdx2D::new([512, 512]))
                .unwrap();

        let tiles_idxs = strat
            .tile_idx_iterator_from_grid_bounds(bounds)
            .collect::<Vec<_>>();

        assert_eq!(tiles_idxs.len(), 4 * 4);
        assert_eq!(tiles_idxs[0], [-2, -2].into());
        assert_eq!(tiles_idxs[1], [-2, -1].into());
        assert_eq!(tiles_idxs[14], [1, 0].into());
        assert_eq!(tiles_idxs[15], [1, 1].into());
    }

    #[test]
    fn tiling_tile_tile() {
        let geo_transform = GeoTransform::new(
            (-1_234_567_890., 1_234_567_890.).into(),
            0.000_033_337_4,
            -0.000_033_337_4,
        );

        let tile_pixel_size = GridShape2D::new_2d(512, 512);
        let tiling_strat = TilingStrategy::new(tile_pixel_size, geo_transform);

        let tiling_origin_reference = Coordinate2D::new(0., 0.); // This is the _currently_ fixed tiling origin reference.
        let nearest_to_tiling_origin = geo_transform.nearest_pixel_edge(tiling_origin_reference);

        let tile_idx = tiling_strat.pixel_idx_to_tile_idx(nearest_to_tiling_origin);
        let expected_near_tiling_origin_idx = GridIdx::new([72_329_138_149, 72_329_138_149]);
        assert_eq!(tile_idx, expected_near_tiling_origin_idx);

        let pixel_distance_reverse = nearest_to_tiling_origin * -1;

        let origin_pixel_tile = tiling_strat.pixel_idx_to_tile_idx(pixel_distance_reverse);
        let origin_pixel_offset =
            tiling_strat.tile_idx_to_global_pixel_idx(origin_pixel_tile) - pixel_distance_reverse;

        let expected_origin_in_tiling_based_pixels =
            GridIdx::new([-72_329_138_150, -72_329_138_150]);
        let expected_tile_offset_from_tiling = GridIdx::new([-85, -85]);
        assert_eq!(origin_pixel_tile, expected_origin_in_tiling_based_pixels);
        assert_eq!(origin_pixel_offset, expected_tile_offset_from_tiling);
    }

    #[test]
    fn pixel_idx_to_tile_idx() {
        let geo_transform = GeoTransform::new((123., 321.).into(), 1.0, -1.0);
        let tile_pixel_size = GridShape2D::new_2d(100, 100);

        let tiling_strat = TilingStrategy::new(tile_pixel_size, geo_transform);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(0, 0));
        assert_eq!(GridIdx2D::new_y_x(0, 0), pixels);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(1, 1));
        assert_eq!(GridIdx2D::new_y_x(0, 0), pixels);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(57, 57));
        assert_eq!(GridIdx2D::new_y_x(0, 0), pixels);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(100, 100));
        assert_eq!(GridIdx2D::new_y_x(1, 1), pixels);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(200, 200));
        assert_eq!(GridIdx2D::new_y_x(2, 2), pixels);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(1000, 1000));
        assert_eq!(GridIdx2D::new_y_x(10, 10), pixels);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(-57, -57));
        assert_eq!(GridIdx2D::new_y_x(-1, -1), pixels);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(-300, -300));
        assert_eq!(GridIdx2D::new_y_x(-3, -3), pixels);
    }

    #[test]
    fn tile_idx_to_pixel_idx() {
        let geo_transform = GeoTransform::new((123., 321.).into(), 1.0, -1.0);
        let tile_pixel_size = GridShape2D::new_2d(100, 100);

        let tiling_strat = TilingStrategy::new(tile_pixel_size, geo_transform);
        let pixels = tiling_strat.tile_idx_to_global_pixel_idx(GridIdx2D::new_y_x(0, 0));
        assert_eq!(GridIdx2D::new_y_x(0, 0), pixels);
        let pixels = tiling_strat.tile_idx_to_global_pixel_idx(GridIdx2D::new_y_x(1, 1));
        assert_eq!(GridIdx2D::new_y_x(100, 100), pixels);
        let pixels = tiling_strat.tile_idx_to_global_pixel_idx(GridIdx2D::new_y_x(2, 2));
        assert_eq!(GridIdx2D::new_y_x(200, 200), pixels);
        let pixels = tiling_strat.tile_idx_to_global_pixel_idx(GridIdx2D::new_y_x(3, 3));
        assert_eq!(GridIdx2D::new_y_x(300, 300), pixels);
        let pixels = tiling_strat.tile_idx_to_global_pixel_idx(GridIdx2D::new_y_x(10, 10));
        assert_eq!(GridIdx2D::new_y_x(1000, 1000), pixels);
        let pixels = tiling_strat.tile_idx_to_global_pixel_idx(GridIdx2D::new_y_x(-3, -3));
        assert_eq!(GridIdx2D::new_y_x(-300, -300), pixels);
    }
}
