use super::{BaseRaster, Dim2D, Dim3D, GeoTransform, GridDimension, Raster};
use crate::primitives::{BoundingBox2D, SpatialBounded, TemporalBounded, TimeInterval};
use crate::raster::data_type::FromPrimitive;
use crate::raster::Pixel;
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};

pub type RasterTile2D<T> = RasterTile<Dim2D, T>;
pub type RasterTile3D<T> = RasterTile<Dim3D, T>;

/// A `RasterTile2D` is the main type used to iterate over tiles of 2D raster data
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RasterTile<D, T>
where
    T: Pixel,
{
    pub time: TimeInterval,
    pub tile: TileInformation,
    pub data: BaseRaster<D, T, Vec<T>>,
}

impl<D, T> RasterTile<D, T>
where
    T: Pixel,
{
    /// create a new `RasterTile2D`
    pub fn new(time: TimeInterval, tile: TileInformation, data: BaseRaster<D, T, Vec<T>>) -> Self {
        Self { time, tile, data }
    }

    /// Converts the data type of the raster tile by converting its inner raster
    pub fn convert<To>(self) -> RasterTile<D, To>
    where
        D: GridDimension,
        To: Pixel + FromPrimitive<T>,
        T: AsPrimitive<To>,
    {
        RasterTile::new(self.time, self.tile, self.data.convert())
    }
}

/// The `TileInformation` is used to represent the spatial position of each tile
#[derive(PartialEq, Debug, Copy, Clone, Serialize, Deserialize)]
pub struct TileInformation {
    pub global_size_in_tiles: Dim2D,
    pub global_tile_position: Dim2D,
    pub global_pixel_position: Dim2D,
    pub tile_size_in_pixels: Dim2D,
    pub global_geo_transform: GeoTransform,
}

impl TileInformation {
    pub fn new(
        global_size_in_tiles: Dim2D,
        global_tile_position: Dim2D,
        global_pixel_position: Dim2D,
        tile_size_in_pixels: Dim2D,
        global_geo_transform: GeoTransform,
    ) -> Self {
        Self {
            global_size_in_tiles,
            global_tile_position,
            global_pixel_position,
            tile_size_in_pixels,
            global_geo_transform,
        }
    }
    pub fn global_size_in_tiles(&self) -> Dim2D {
        self.global_size_in_tiles
    }
    pub fn global_tile_position(&self) -> Dim2D {
        self.global_tile_position
    }
    pub fn global_pixel_position_upper_left(&self) -> Dim2D {
        self.global_pixel_position
    }

    pub fn global_pixel_position_lower_right(&self) -> Dim2D {
        self.global_pixel_position_upper_left() + self.tile_size_in_pixels
    }

    pub fn tile_size_in_pixels(&self) -> Dim2D {
        self.tile_size_in_pixels
    }

    pub fn tile_pixel_position_to_global(&self, local_pixel_position: Dim2D) -> Dim2D {
        self.global_pixel_position_upper_left() + local_pixel_position
    }

    pub fn tile_geo_transform(&self) -> GeoTransform {
        let tile_upper_left_coord = self
            .global_geo_transform
            .grid_2d_to_coordinate_2d(self.global_pixel_position.as_pattern());

        GeoTransform::new(
            tile_upper_left_coord,
            self.global_geo_transform.x_pixel_size,
            self.global_geo_transform.y_pixel_size,
        )
    }
}

impl SpatialBounded for TileInformation {
    fn spatial_bounds(&self) -> BoundingBox2D {
        let top_left_coord = self
            .global_geo_transform
            .grid_2d_to_coordinate_2d(self.global_pixel_position_upper_left().as_pattern());
        let lower_right_coord = self
            .global_geo_transform
            .grid_2d_to_coordinate_2d(self.global_pixel_position_lower_right().as_pattern());
        BoundingBox2D::new_upper_left_lower_right_unchecked(top_left_coord, lower_right_coord)
    }
}

impl<D, T> TemporalBounded for RasterTile<D, T>
where
    T: Pixel,
{
    fn temporal_bounds(&self) -> TimeInterval {
        self.time
    }
}

impl<D, T> SpatialBounded for RasterTile<D, T>
where
    T: Pixel,
{
    fn spatial_bounds(&self) -> BoundingBox2D {
        self.tile.spatial_bounds()
    }
}

impl<D, T> Raster<D, T, Vec<T>> for RasterTile<D, T>
where
    D: GridDimension,
    T: Pixel,
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
        &self.tile.global_geo_transform
    }
}
