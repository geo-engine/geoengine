use super::{
    GeoTransform, GridArray, GridBounds, GridIdx, GridIdx2D, GridIndexAccess, GridIndexAccessMut,
    GridShape2D, GridShape3D, GridSize, GridSpaceToLinearSpace, Raster,
};
use crate::primitives::{BoundingBox2D, SpatialBounded, TemporalBounded, TimeInterval};
use crate::raster::data_type::FromPrimitive;
use crate::raster::Pixel;
use crate::util::Result;
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};

pub type RasterTile2D<T> = RasterTile<GridShape2D, T>;
pub type RasterTile3D<T> = RasterTile<GridShape3D, T>;

/// A `RasterTile2D` is the main type used to iterate over tiles of 2D raster data
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RasterTile<D, T>
where
    D: GridSize + GridSpaceToLinearSpace,
    T: Pixel,
{
    /// The `TimeInterval` where this `RasterTile is valid`.
    pub time: TimeInterval,
    /// The tile position is the position of the tile in the gird of tiles with origin at the origin of the global_geo_transform.
    /// This is NOT a pixel position inside the tile.
    pub tile_position: GridIdx2D,
    /// The global geotransform to transform pixels into geographic coordinates
    pub global_geo_transform: GeoTransform,
    /// The data of the `RasterTile` is stored as `GridArray`
    pub grid_array: GridArray<D, T>,
}

impl<D, T> RasterTile<D, T>
where
    T: Pixel,
    D: GridSize + GridSpaceToLinearSpace + Clone,
{
    /// create a new `RasterTile`
    pub fn new_with_tile_info(
        time: TimeInterval,
        tile_info: TileInformation,
        data: GridArray<D, T>,
    ) -> Self {
        // TODO: assert, tile information xy size equals the data xy size
        Self {
            time,
            tile_position: tile_info.global_tile_position,
            global_geo_transform: tile_info.global_geo_transform,
            grid_array: data,
        }
    }

    /// create a new `RasterTile`
    pub fn new(
        time: TimeInterval,
        tile_position: GridIdx2D,
        global_geo_transform: GeoTransform,
        data: GridArray<D, T>,
    ) -> Self {
        Self {
            time,
            tile_position,
            global_geo_transform,
            grid_array: data,
        }
    }

    /// create a new `RasterTile`
    pub fn new_without_offset(
        time: TimeInterval,
        global_geo_transform: GeoTransform,
        data: GridArray<D, T>,
    ) -> Self {
        Self {
            time,
            tile_position: [0, 0].into(),
            global_geo_transform,
            grid_array: data,
        }
    }

    /// Converts the data type of the raster tile by converting its inner raster
    pub fn convert<To>(self) -> RasterTile<D, To>
    where
        D: GridSize + GridSpaceToLinearSpace,
        To: Pixel + FromPrimitive<T>,
        T: AsPrimitive<To>,
    {
        RasterTile::new(
            self.time,
            self.tile_position,
            self.global_geo_transform,
            self.grid_array.convert_dtype(),
        )
    }

    pub fn grid_dimension(&self) -> D {
        self.grid_array.shape.clone()
    }

    pub fn tile_offset(&self) -> GridIdx2D {
        self.tile_position
    }

    pub fn tile_information(&self) -> TileInformation {
        TileInformation::new(
            self.tile_position,
            [
                self.grid_array.shape.axis_size_y(),
                self.grid_array.shape.axis_size_x(),
            ]
            .into(),
            self.global_geo_transform,
        )
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
            global_tile_position,
            tile_size_in_pixels,
            global_geo_transform,
        }
    }

    #[allow(clippy::unused_self)]
    pub fn local_upper_left_idx(&self) -> GridIdx2D {
        [0, 0].into()
    }

    pub fn local_lower_left_idx(&self) -> GridIdx2D {
        [self.tile_size_in_pixels.axis_size_y() as isize - 1, 0].into()
    }

    pub fn local_upper_right_idx(&self) -> GridIdx2D {
        [0, self.tile_size_in_pixels.axis_size_x() as isize - 1].into()
    }

    pub fn local_lower_right_idx(&self) -> GridIdx2D {
        let GridIdx([y, _]) = self.local_lower_left_idx();
        let GridIdx([_, x]) = self.local_upper_right_idx();
        [y, x].into()
    }

    pub fn global_tile_position(&self) -> GridIdx2D {
        self.global_tile_position
    }

    pub fn global_upper_left_idx(&self) -> GridIdx2D {
        let [tile_size_y, tile_size_x] = self.tile_size_in_pixels.into_inner();
        self.global_tile_position() * [tile_size_y as isize, tile_size_x as isize]
    }

    pub fn global_upper_right_idx(&self) -> GridIdx2D {
        self.global_upper_left_idx() + self.local_upper_right_idx()
    }

    pub fn global_lower_right_idx(&self) -> GridIdx2D {
        self.global_upper_left_idx() + self.local_lower_right_idx()
    }

    pub fn global_lower_left_idx(&self) -> GridIdx2D {
        self.global_upper_left_idx() + self.local_lower_left_idx()
    }

    pub fn tile_size_in_pixels(&self) -> GridShape2D {
        self.tile_size_in_pixels
    }

    pub fn local_to_global_idx(&self, local_pixel_position: GridIdx2D) -> GridIdx2D {
        self.global_upper_left_idx() + local_pixel_position
    }

    pub fn tile_geo_transform(&self) -> GeoTransform {
        let tile_upper_left_coord = self
            .global_geo_transform
            .grid_idx_to_coordinate_2d(self.global_upper_left_idx());

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
            .grid_idx_to_coordinate_2d(self.global_upper_left_idx());
        let lower_right_coord = self
            .global_geo_transform
            .grid_idx_to_coordinate_2d(self.global_lower_right_idx() + 1); // we need the border of the lower right pixel.
        BoundingBox2D::new_upper_left_lower_right_unchecked(top_left_coord, lower_right_coord)
    }
}

impl<D, T> TemporalBounded for RasterTile<D, T>
where
    D: GridSize + GridSpaceToLinearSpace,
    T: Pixel,
{
    fn temporal_bounds(&self) -> TimeInterval {
        self.time
    }
}

impl<D, T> SpatialBounded for RasterTile<D, T>
where
    T: Pixel,
    D: GridSize + GridSpaceToLinearSpace + Clone,
{
    fn spatial_bounds(&self) -> BoundingBox2D {
        self.tile_information().spatial_bounds()
    }
}

impl<D, T> Raster<D, T, Vec<T>> for RasterTile<D, T>
where
    D: GridSize + GridSpaceToLinearSpace + Clone,
    T: Pixel,
{
    fn dimension(&self) -> D {
        self.grid_array.shape.clone()
    }
    fn no_data_value(&self) -> Option<T> {
        self.grid_array.no_data_value
    }
    fn data_container(&self) -> &Vec<T> {
        self.grid_array.inner_ref()
    }
    fn geo_transform(&self) -> GeoTransform {
        self.tile_information().global_geo_transform
    }
}

impl<T, D, I, A> GridIndexAccess<T, I> for RasterTile<D, T>
where
    D: GridSize + GridSpaceToLinearSpace<IndexArray = A> + GridBounds<IndexArray = A>,
    I: Into<GridIdx<A>>,
    A: AsRef<[isize]> + Into<GridIdx<A>> + Clone,
    T: Pixel,
{
    fn get_at_grid_index(&self, grid_index: I) -> Result<T> {
        self.grid_array.get_at_grid_index(grid_index.into())
    }

    fn get_at_grid_index_unchecked(&self, grid_index: I) -> T {
        self.grid_array.get_at_grid_index_unchecked(grid_index)
    }
}

impl<T, D, I, A> GridIndexAccessMut<T, I> for RasterTile<D, T>
where
    D: GridSize + GridSpaceToLinearSpace<IndexArray = A> + GridBounds<IndexArray = A>,
    I: Into<GridIdx<A>>,
    A: AsRef<[isize]> + Into<GridIdx<A>> + Clone,
    T: Pixel,
{
    fn set_at_grid_index(&mut self, grid_index: I, value: T) -> Result<()> {
        self.grid_array.set_at_grid_index(grid_index, value)
    }

    fn set_at_grid_index_unchecked(&mut self, grid_index: I, value: T) {
        self.grid_array
            .set_at_grid_index_unchecked(grid_index, value)
    }
}

#[cfg(test)]
mod tests {
    use crate::primitives::Coordinate2D;

    use super::*;

    #[test]
    fn tile_information_new() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_geo_transform, GeoTransform::default());
        assert_eq!(ti.global_tile_position, GridIdx([0, 0]));
        assert_eq!(ti.tile_size_in_pixels, GridShape2D::from([100, 100]));
    }

    #[test]
    fn tile_information_global_tile_position() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_tile_position(), GridIdx([0, 0]));
    }

    #[test]
    fn tile_information_local_upper_left() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.local_upper_left_idx(), GridIdx([0, 0]));
    }

    #[test]
    fn tile_information_local_lower_left() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.local_lower_left_idx(), GridIdx([99, 0]));
    }

    #[test]
    fn tile_information_local_upper_right() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.local_upper_right_idx(), GridIdx([0, 99]));
    }

    #[test]
    fn tile_information_local_lower_right() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.local_lower_right_idx(), GridIdx([99, 99]));
    }

    #[test]
    fn tile_information_global_upper_left_idx() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_upper_left_idx(), GridIdx([0, 0]));
    }

    #[test]
    fn tile_information_global_upper_left_idx_2_3() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_upper_left_idx(), GridIdx([-200, 3000]));
    }

    #[test]
    fn tile_information_global_upper_right_idx() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_upper_right_idx(), GridIdx([0, 99]));
    }

    #[test]
    fn tile_information_global_upper_right_idx_2_3() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_upper_right_idx(), GridIdx([-200, 3999]));
    }

    #[test]
    fn tile_information_global_lower_right_idx() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_lower_right_idx(), GridIdx([99, 99]));
    }

    #[test]
    fn tile_information_global_lower_right_idx_2_3() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_lower_right_idx(), GridIdx([-101, 3999]));
    }

    #[test]
    fn tile_information_global_lower_left_idx() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_lower_left_idx(), GridIdx([99, 0]));
    }

    #[test]
    fn tile_information_global_lower_left_idx_2_3() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_lower_left_idx(), GridIdx([-101, 3000]));
    }

    #[test]
    fn tile_information_local_to_global_idx_0_0() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.local_to_global_idx(GridIdx([25, 75])), GridIdx([25, 75]));
    }

    #[test]
    fn tile_information_local_to_global_idx_2_3() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::default(),
        );
        assert_eq!(
            ti.local_to_global_idx(GridIdx([25, 75])),
            GridIdx([-175, 3075])
        );
    }

    #[test]
    fn tile_information_spatial_bounds() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::default(),
        );
        assert_eq!(
            ti.spatial_bounds(),
            BoundingBox2D::new_upper_left_lower_right_unchecked(
                Coordinate2D::new(3000., 200.),
                Coordinate2D::new(4000., 100.)
            )
        );
    }

    #[test]
    fn tile_information_spatial_bounds_geotransform() {
        let ti = TileInformation::new(
            GridIdx([2, 3]),
            GridShape2D::from([10, 10]),
            GeoTransform::new_with_coordinate_x_y(-180., 0.1, 90., -0.1),
        );
        assert_eq!(
            ti.spatial_bounds(),
            BoundingBox2D::new_upper_left_lower_right_unchecked(
                Coordinate2D::new(-177., 88.),
                Coordinate2D::new(-176., 87.)
            )
        );
    }
}
