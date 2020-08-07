use super::{BaseRaster, Dim, GeoTransform, GridDimension, Ix, Raster};
use crate::primitives::{BoundingBox2D, SpatialBounded, TemporalBounded, TimeInterval};
use serde::{Deserialize, Serialize};

pub type RasterTile2D<T> = RasterTile<Dim<[Ix; 2]>, T>;
pub type RasterTile3D<T> = RasterTile<Dim<[Ix; 3]>, T>;

/// A `RasterTile2D` is the main type used to iterate over tiles of 2D raster data
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RasterTile<D, T> {
    pub time: TimeInterval,
    pub tile: TileInformation,
    pub data: BaseRaster<D, T, Vec<T>>,
}

impl<D, T> RasterTile<D, T> {
    /// create a new `RasterTile2D`
    pub fn new(time: TimeInterval, tile: TileInformation, data: BaseRaster<D, T, Vec<T>>) -> Self {
        Self { time, tile, data }
    }

    /// Converts the data type of the raster tile by converting its inner raster
    pub fn convert<To>(self) -> RasterTile<D, To>
    where
        D: GridDimension,
        T: Into<To> + Copy, // TODO: find common type for pixel values,
    {
        RasterTile::new(self.time, self.tile, self.data.convert())
    }
}

/// The `TileInformation` is used to represent the spatial position of each tile
#[derive(PartialEq, Debug, Copy, Clone, Serialize, Deserialize)]
pub struct TileInformation {
    pub global_size_in_tiles: Dim<[Ix; 2]>,
    pub global_tile_position: Dim<[Ix; 2]>,
    pub global_pixel_position: Dim<[Ix; 2]>,
    pub tile_size_in_pixels: Dim<[Ix; 2]>,
    pub geo_transform: GeoTransform,
}

impl TileInformation {
    pub fn new(
        global_size_in_tiles: Dim<[Ix; 2]>,
        global_tile_position: Dim<[Ix; 2]>,
        global_pixel_position: Dim<[Ix; 2]>,
        tile_size_in_pixels: Dim<[Ix; 2]>,
        geo_transform: GeoTransform,
    ) -> Self {
        Self {
            global_size_in_tiles,
            global_tile_position,
            global_pixel_position,
            tile_size_in_pixels,
            geo_transform,
        }
    }
    pub fn global_size_in_tiles(&self) -> Dim<[Ix; 2]> {
        self.global_size_in_tiles
    }
    pub fn global_tile_position(&self) -> Dim<[Ix; 2]> {
        self.global_tile_position
    }
    pub fn global_pixel_position(&self) -> Dim<[Ix; 2]> {
        self.global_pixel_position
    }
    pub fn tile_size_in_pixels(&self) -> Dim<[Ix; 2]> {
        self.tile_size_in_pixels
    }
}

impl SpatialBounded for TileInformation {
    fn spatial_bounds(&self) -> BoundingBox2D {
        let top_left_coord = self.geo_transform.grid_2d_to_coordinate_2d((0, 0));
        let (.., tile_y_size, tile_x_size) = self.tile_size_in_pixels.as_pattern();
        let lower_right_coord = self
            .geo_transform
            .grid_2d_to_coordinate_2d((tile_y_size, tile_x_size));
        BoundingBox2D::new_upper_left_lower_right_unchecked(top_left_coord, lower_right_coord)
    }
}

impl<D, T> TemporalBounded for RasterTile<D, T> {
    fn temporal_bounds(&self) -> TimeInterval {
        self.time
    }
}

impl<D, T> SpatialBounded for RasterTile<D, T> {
    fn spatial_bounds(&self) -> BoundingBox2D {
        self.tile.spatial_bounds()
    }
}

impl<D, T> Raster<D, T, Vec<T>> for RasterTile<D, T>
where
    D: GridDimension,
    T: Copy,
{
    fn dimension(&self) -> &D {
        self.data.dimension()
    }
    fn no_data_value(&self) -> Option<T> {
        self.data.no_data_value()
    }
    fn data_container(&self) -> &Vec<T> {
        self.data.data_container()
    }
    fn geo_transform(&self) -> &GeoTransform {
        &self.tile.geo_transform
    }
}
