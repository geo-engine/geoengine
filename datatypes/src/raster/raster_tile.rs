use super::{
    BaseRaster, Dim2D, Dim3D, GeoTransform, GridDimension, GridIdx2D, GridIndex, GridPixelAccess,
    GridPixelAccessMut, Raster, SignedGridIdx2D, SignedGridIndex,
};
use crate::primitives::{BoundingBox2D, SpatialBounded, TemporalBounded, TimeInterval};
use crate::raster::data_type::FromPrimitive;
use crate::raster::Pixel;
use crate::util::Result;
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
    pub tile_position: SignedGridIdx2D,
    pub global_geo_transform: GeoTransform,
    pub data: BaseRaster<D, T, Vec<T>>,
}

impl<D, T> RasterTile<D, T>
where
    T: Pixel,
    D: GridDimension,
{
    /// create a new `RasterTile`
    pub fn new_with_tile_info(
        time: TimeInterval,
        tile_info: TileInformation,
        data: BaseRaster<D, T, Vec<T>>,
    ) -> Self {
        // TODO: assert, tile information xy size equals the data xy size
        Self {
            time,
            tile_position: tile_info.global_tile_position,
            global_geo_transform: tile_info.global_geo_transform,
            data,
        }
    }

    /// create a new `RasterTile`
    pub fn new(
        time: TimeInterval,
        tile_position: SignedGridIdx2D,
        global_geo_transform: GeoTransform,
        data: BaseRaster<D, T, Vec<T>>,
    ) -> Self {
        Self {
            time,
            tile_position,
            global_geo_transform,
            data,
        }
    }

    /// create a new `RasterTile`
    pub fn new_without_offset(
        time: TimeInterval,
        global_geo_transform: GeoTransform,
        data: BaseRaster<D, T, Vec<T>>,
    ) -> Self {
        Self {
            time,
            tile_position: [0, 0].into(),
            global_geo_transform,
            data,
        }
    }

    /// Converts the data type of the raster tile by converting its inner raster
    pub fn convert<To>(self) -> RasterTile<D, To>
    where
        D: GridDimension,
        To: Pixel + FromPrimitive<T>,
        T: AsPrimitive<To>,
    {
        RasterTile::new(
            self.time,
            self.tile_position,
            self.global_geo_transform,
            self.data.convert(),
        )
    }

    pub fn grid_dimension(&self) -> D {
        self.data.grid_dimension.clone()
    }

    pub fn tile_offset(&self) -> SignedGridIdx2D {
        self.tile_position
    }

    pub fn tile_information(&self) -> TileInformation {
        TileInformation::new(
            self.tile_position,
            [
                self.grid_dimension().size_of_y_axis(),
                self.grid_dimension().size_of_x_axis(),
            ]
            .into(),
            self.global_geo_transform,
        )
    }
}

/// The `TileInformation` is used to represent the spatial position of each tile
#[derive(PartialEq, Debug, Copy, Clone, Serialize, Deserialize)]
pub struct TileInformation {
    pub tile_size_in_pixels: GridIdx2D,
    pub global_tile_position: SignedGridIdx2D,
    pub global_geo_transform: GeoTransform,
}

impl TileInformation {
    pub fn new(
        global_tile_position: SignedGridIdx2D,
        tile_size_in_pixels: GridIdx2D,
        global_geo_transform: GeoTransform,
    ) -> Self {
        Self {
            global_tile_position,
            tile_size_in_pixels,
            global_geo_transform,
        }
    }

    pub fn global_tile_position(&self) -> SignedGridIdx2D {
        self.global_tile_position
    }

    pub fn global_pixel_position_upper_left(&self) -> SignedGridIdx2D {
        let [tile_y, tile_x] = self.global_tile_position.as_index_array();
        let [tile_size_y, tile_size_x] = self.tile_size_in_pixels.as_index_array();
        [tile_y * tile_size_y as isize, tile_x * tile_size_x as isize].into()
    }

    pub fn global_pixel_position_lower_right(&self) -> SignedGridIdx2D {
        let [up_left_y, up_left_x] = self.global_pixel_position_upper_left().as_index_array();
        let [size_y, size_x] = self.tile_size_in_pixels.as_index_array();
        [up_left_y + (size_y as isize), up_left_x + (size_x as isize)].into()
    }

    pub fn tile_size_in_pixels(&self) -> GridIdx2D {
        self.tile_size_in_pixels
    }

    pub fn tile_pixel_position_to_global(
        &self,
        local_pixel_position: GridIdx2D,
    ) -> SignedGridIdx2D {
        let [up_left_y, up_left_x] = self.global_pixel_position_upper_left().as_index_array();
        let [pos_y, pos_x] = local_pixel_position.as_index_array();
        [up_left_y + (pos_y as isize), up_left_x + (pos_x as isize)].into()
    }

    pub fn tile_geo_transform(&self) -> GeoTransform {
        let tile_upper_left_coord = self
            .global_geo_transform
            .signed_grid_idx_to_coordinate_2d(self.global_pixel_position_upper_left());

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
            .signed_grid_idx_to_coordinate_2d(self.global_pixel_position_upper_left());
        let lower_right_coord = self
            .global_geo_transform
            .signed_grid_idx_to_coordinate_2d(self.global_pixel_position_lower_right());
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
    D: GridDimension,
{
    fn spatial_bounds(&self) -> BoundingBox2D {
        self.tile_information().spatial_bounds()
    }
}

impl<D, T> Raster<D, T, Vec<T>> for RasterTile<D, T>
where
    D: GridDimension,
    T: Pixel,
{
    fn dimension(&self) -> D {
        self.data.grid_dimension.clone()
    }
    fn no_data_value(&self) -> Option<T> {
        self.data.no_data_value
    }
    fn data_container(&self) -> &Vec<T> {
        &self.data.data_container
    }
    fn geo_transform(&self) -> GeoTransform {
        self.tile_information().global_geo_transform
    }
}

impl<D, T, I> GridPixelAccess<T, I> for RasterTile<D, T>
where
    D: GridDimension<IndexType = I>,
    I: GridIndex<D>,
    T: Pixel,
{
    fn pixel_value_at_grid_index(&self, grid_index: &I) -> Result<T> {
        self.data.pixel_value_at_grid_index(grid_index)
    }
}

impl<D, T, I> GridPixelAccessMut<T, I> for RasterTile<D, T>
where
    D: GridDimension<IndexType = I>,
    I: GridIndex<D>,
    T: Pixel,
{
    fn set_pixel_value_at_grid_index(&mut self, grid_index: &I, value: T) -> Result<()> {
        self.data.set_pixel_value_at_grid_index(grid_index, value)
    }
}
