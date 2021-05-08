use super::{
    grid_or_empty::GridOrEmpty, GeoTransform, GeoTransformAccess, Grid, GridBounds, GridIdx2D,
    GridIndexAccess, GridIndexAccessMut, GridShape, GridShape2D, GridShape3D, GridShapeAccess,
    GridSize, GridSpaceToLinearSpace, NoDataValue, Raster, TileInformation,
};
use crate::primitives::{
    BoundingBox2D, Coordinate2D, SpatialBounded, TemporalBounded, TimeInterval,
};
use crate::raster::data_type::FromPrimitive;
use crate::raster::{CoordinatePixelAccess, Pixel};
use crate::util::Result;
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};

pub type RasterTile<D, T> = BaseTile<GridOrEmpty<D, T>>;
pub type RasterTile2D<T> = RasterTile<GridShape2D, T>;
pub type RasterTile3D<T> = RasterTile<GridShape3D, T>;

pub type MaterializedRasterTile<D, T> = BaseTile<Grid<D, T>>;
pub type MaterializedRasterTile2D<T> = MaterializedRasterTile<GridShape2D, T>;
pub type MaterializedRasterTile3D<T> = MaterializedRasterTile<GridShape3D, T>;

/// A `RasterTile2D` is the main type used to iterate over tiles of 2D raster data
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BaseTile<G> {
    /// The `TimeInterval` where this `RasterTile is valid`.
    pub time: TimeInterval,
    /// The tile position is the position of the tile in the gird of tiles with origin at the origin of the global_geo_transform.
    /// This is NOT a pixel position inside the tile.
    pub tile_position: GridIdx2D,
    /// The global geotransform to transform pixels into geographic coordinates
    pub global_geo_transform: GeoTransform,
    /// The data of the `RasterTile` is stored as `Grid` or `NoDataGrid`. `GridOrEmpty` allows a combination of both.
    pub grid_array: G,
}

impl<G> BaseTile<G>
where
    G: GridSize,
{
    pub fn tile_offset(&self) -> GridIdx2D {
        self.tile_position
    }

    pub fn tile_information(&self) -> TileInformation {
        TileInformation::new(
            self.tile_position,
            [self.grid_array.axis_size_y(), self.grid_array.axis_size_x()].into(),
            self.global_geo_transform,
        )
    }

    /// Use this geo transform to transform `Coordinate2D` into local grid indices and vice versa.
    #[inline]
    pub fn tile_geo_transform(&self) -> GeoTransform {
        let global_upper_left_idx = self.tile_position
            * [
                self.grid_array.axis_size_y() as isize,
                self.grid_array.axis_size_x() as isize,
            ];

        let tile_upper_left_coord = self
            .global_geo_transform
            .grid_idx_to_upper_left_coordinate_2d(global_upper_left_idx);

        GeoTransform::new(
            tile_upper_left_coord,
            self.global_geo_transform.x_pixel_size,
            self.global_geo_transform.y_pixel_size,
        )
    }
}

impl<D, T> BaseTile<GridOrEmpty<D, T>>
where
    T: Pixel,
    D: GridSize + Clone,
{
    /// create a new `RasterTile`
    pub fn new_with_tile_info(
        time: TimeInterval,
        tile_info: TileInformation,
        data: Grid<D, T>,
    ) -> Self {
        // TODO: assert, tile information xy size equals the data xy size
        Self {
            time,
            tile_position: tile_info.global_tile_position,
            global_geo_transform: tile_info.global_geo_transform,
            grid_array: GridOrEmpty::Grid(data),
        }
    }

    /// create a new `RasterTile`
    pub fn new(
        time: TimeInterval,
        tile_position: GridIdx2D,
        global_geo_transform: GeoTransform,
        data: GridOrEmpty<D, T>,
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
        data: Grid<D, T>,
    ) -> Self {
        Self {
            time,
            tile_position: [0, 0].into(),
            global_geo_transform,
            grid_array: GridOrEmpty::Grid(data),
        }
    }

    /// Converts the data type of the raster tile by converting its inner raster
    pub fn convert<To>(self) -> BaseTile<GridOrEmpty<D, To>>
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

    /// Convert the tile into a materialized tile.
    pub fn into_materialized_tile(self) -> MaterializedRasterTile<D, T> {
        MaterializedRasterTile {
            grid_array: self.grid_array.into_materialized_grid(),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
        }
    }
}

impl<G> TemporalBounded for BaseTile<G> {
    fn temporal_bounds(&self) -> TimeInterval {
        self.time
    }
}

impl<G> SpatialBounded for BaseTile<G>
where
    G: GridSize,
{
    fn spatial_bounds(&self) -> BoundingBox2D {
        self.tile_information().spatial_bounds()
    }
}

impl<D, T, G> Raster<D, T, G> for BaseTile<G>
where
    D: GridSize + GridBounds + Clone,
    T: Pixel,
    G: GridIndexAccess<D::IndexArray, T>,
    Self:
        SpatialBounded + NoDataValue<NoDataType = T> + GridShapeAccess<ShapeArray = D::ShapeArray>,
{
    fn data_container(&self) -> &G {
        &self.grid_array
    }
}

impl<T, G, I> GridIndexAccess<T, I> for BaseTile<G>
where
    G: GridIndexAccess<T, I>,
    T: Pixel,
{
    fn get_at_grid_index(&self, grid_index: I) -> Result<T> {
        self.grid_array.get_at_grid_index(grid_index)
    }

    fn get_at_grid_index_unchecked(&self, grid_index: I) -> T {
        self.grid_array.get_at_grid_index_unchecked(grid_index)
    }
}

impl<T, G, I> GridIndexAccessMut<T, I> for BaseTile<G>
where
    G: GridIndexAccessMut<T, I>,
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

impl<G, P> CoordinatePixelAccess<P> for BaseTile<G>
where
    G: GridSize + Clone,
    P: Pixel,
    Self: GridIndexAccess<P, GridIdx2D>,
{
    fn pixel_value_at_coord(&self, coordinate: Coordinate2D) -> Result<P> {
        // TODO: benchmark the impact of creating the `GeoTransform`s

        let grid_index = self
            .tile_geo_transform()
            .coordinate_to_grid_idx_2d(coordinate);

        self.get_at_grid_index(grid_index)
    }

    fn pixel_value_at_coord_unchecked(&self, coordinate: Coordinate2D) -> P {
        let grid_index = self
            .tile_geo_transform()
            .coordinate_to_grid_idx_2d(coordinate);

        self.get_at_grid_index_unchecked(grid_index)
    }
}

impl<G> NoDataValue for BaseTile<G>
where
    G: NoDataValue,
{
    type NoDataType = G::NoDataType;

    fn no_data_value(&self) -> Option<Self::NoDataType> {
        self.grid_array.no_data_value()
    }
}

impl<G, A> GridShapeAccess for BaseTile<G>
where
    G: GridShapeAccess<ShapeArray = A>,
    A: AsRef<[usize]> + Into<GridShape<A>>,
{
    type ShapeArray = A;

    fn grid_shape_array(&self) -> Self::ShapeArray {
        self.grid_array.grid_shape_array()
    }
}

impl<G> GeoTransformAccess for BaseTile<G> {
    fn geo_transform(&self) -> GeoTransform {
        self.global_geo_transform
    }
}

impl<D, T> From<MaterializedRasterTile<D, T>> for RasterTile<D, T>
where
    T: Clone,
{
    fn from(mat_tile: MaterializedRasterTile<D, T>) -> Self {
        RasterTile {
            grid_array: mat_tile.grid_array.into(),
            global_geo_transform: mat_tile.global_geo_transform,
            tile_position: mat_tile.tile_position,
            time: mat_tile.time,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::primitives::Coordinate2D;

    use super::*;
    use crate::raster::{Grid2D, GridIdx};

    #[test]
    fn coordinate_pixel_access() {
        fn validate_coordinate<C: Into<Coordinate2D> + Copy>(
            raster_tile: &RasterTile2D<i32>,
            coordinate: C,
        ) {
            let coordinate: Coordinate2D = coordinate.into();

            let tile_geo_transform = raster_tile.tile_information().tile_geo_transform();

            let value_a = raster_tile.pixel_value_at_coord(coordinate);

            let value_b = raster_tile
                .get_at_grid_index(tile_geo_transform.coordinate_to_grid_idx_2d(coordinate));

            match (value_a, value_b) {
                (Ok(a), Ok(b)) => assert_eq!(a, b),
                (Err(e1), Err(e2)) => assert_eq!(format!("{:?}", e1), format!("{:?}", e2)),
                (Err(e), _) | (_, Err(e)) => panic!("{}", e.to_string()),
            };
        }

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: Default::default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None).unwrap(),
        );

        validate_coordinate(&raster_tile, (0.0, 0.0));
        validate_coordinate(&raster_tile, (1.0, 0.0));

        assert_eq!(
            raster_tile.pixel_value_at_coord_unchecked((0.0, 0.0).into()),
            1
        );
        assert_eq!(
            raster_tile.pixel_value_at_coord_unchecked((1.0, 0.0).into()),
            2
        );

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: Default::default(),
                global_tile_position: [1, 1].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None).unwrap(),
        );

        validate_coordinate(&raster_tile, (0.0, 0.0));
        validate_coordinate(&raster_tile, (2.0, -3.0));
        validate_coordinate(&raster_tile, (3.0, -3.0));

        assert_eq!(
            raster_tile.pixel_value_at_coord_unchecked((2.0, -3.0).into()),
            1
        );
        assert_eq!(
            raster_tile.pixel_value_at_coord_unchecked((3.0, -3.0).into()),
            2
        );
    }

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
        assert_eq!(ti.local_upper_left_pixel_idx(), GridIdx([0, 0]));
    }

    #[test]
    fn tile_information_local_lower_left() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.local_lower_left_pixel_idx(), GridIdx([99, 0]));
    }

    #[test]
    fn tile_information_local_upper_right() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.local_upper_right_pixel_idx(), GridIdx([0, 99]));
    }

    #[test]
    fn tile_information_local_lower_right() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.local_lower_right_pixel_idx(), GridIdx([99, 99]));
    }

    #[test]
    fn tile_information_global_upper_left_idx() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_upper_left_pixel_idx(), GridIdx([0, 0]));
    }

    #[test]
    fn tile_information_global_upper_left_idx_2_3() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_upper_left_pixel_idx(), GridIdx([-200, 3000]));
    }

    #[test]
    fn tile_information_global_upper_right_idx() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_upper_right_pixel_idx(), GridIdx([0, 99]));
    }

    #[test]
    fn tile_information_global_upper_right_idx_2_3() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_upper_right_pixel_idx(), GridIdx([-200, 3999]));
    }

    #[test]
    fn tile_information_global_lower_right_idx() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_lower_right_pixel_idx(), GridIdx([99, 99]));
    }

    #[test]
    fn tile_information_global_lower_right_idx_2_3() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_lower_right_pixel_idx(), GridIdx([-101, 3999]));
    }

    #[test]
    fn tile_information_global_lower_left_idx() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_lower_left_pixel_idx(), GridIdx([99, 0]));
    }

    #[test]
    fn tile_information_global_lower_left_idx_2_3() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::default(),
        );
        assert_eq!(ti.global_lower_left_pixel_idx(), GridIdx([-101, 3000]));
    }

    #[test]
    fn tile_information_local_to_global_idx_0_0() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::default(),
        );
        assert_eq!(
            ti.local_to_global_pixel_idx(GridIdx([25, 75])),
            GridIdx([25, 75])
        );
    }

    #[test]
    fn tile_information_local_to_global_idx_2_3() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::default(),
        );
        assert_eq!(
            ti.local_to_global_pixel_idx(GridIdx([25, 75])),
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
