use super::masked_grid::MaskedGrid;
use super::{
    grid_or_empty::GridOrEmpty, GeoTransform, GeoTransformAccess, GridBounds, GridIdx2D,
    GridIndexAccess, GridShape, GridShape2D, GridShape3D, GridShapeAccess, GridSize, Raster,
    TileInformation,
};
use super::{GridIndexAccessMut, RasterProperties};
use crate::primitives::{
    SpatialBounded, SpatialPartition2D, SpatialPartitioned, TemporalBounded, TimeInterval,
};
use crate::raster::Pixel;
use crate::util::Result;
use serde::{Deserialize, Serialize};

/// A `RasterTile` is a `BaseTile` of raster data where the data is represented by `GridOrEmpty`.
pub type RasterTile<D, T> = BaseTile<GridOrEmpty<D, T>>;
/// A `RasterTile2D` is a `BaseTile` of 2-dimensional raster data where the data is represented by `GridOrEmpty`.
pub type RasterTile2D<T> = RasterTile<GridShape2D, T>;
/// A `RasterTile3D` is a `BaseTile` of 3-dimensional raster data where the data is represented by `GridOrEmpty`.
pub type RasterTile3D<T> = RasterTile<GridShape3D, T>;

/// A `MaterializedRasterTile` is a `BaseTile` of raster data where the data is represented by `Grid`. It implements mutable access to pixels.
pub type MaterializedRasterTile<D, T> = BaseTile<MaskedGrid<D, T>>;
/// A `MaterializedRasterTile2D` is a 2-dimensional `BaseTile` of raster data where the data is represented by `Grid`. It implements mutable access to pixels.
pub type MaterializedRasterTile2D<T> = MaterializedRasterTile<GridShape2D, T>;
/// A `MaterializedRasterTile3D` is a 3-dimensional `BaseTile` of raster data where the data is represented by `Grid`. It implements mutable access to pixels.
pub type MaterializedRasterTile3D<T> = MaterializedRasterTile<GridShape3D, T>;

/// A `BaseTile` is the main type used to iterate over tiles of raster data
/// The data of the `RasterTile` is stored as `Grid` or `NoDataGrid`. The enum `GridOrEmpty` allows a combination of both.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BaseTile<G> {
    /// The `TimeInterval` where this tile is valid.
    pub time: TimeInterval,
    /// The tile position is the position of the tile in the gird of tiles with origin at the origin of the global_geo_transform.
    /// This is NOT a pixel position inside the tile.
    pub tile_position: GridIdx2D,
    /// The global geotransform to transform pixels into geographic coordinates
    pub global_geo_transform: GeoTransform,
    /// The pixels of the tile are stored as `Grid` or, in case they are all no-data as `NoDataGrid`.
    /// The enum `GridOrEmpty` allows a combination of both.
    pub grid_array: G,
    /// Metadata for the `BaseTile`
    pub properties: RasterProperties,
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
            .grid_idx_to_pixel_upper_left_coordinate_2d(global_upper_left_idx);

        GeoTransform::new(
            tile_upper_left_coord,
            self.global_geo_transform.x_pixel_size(),
            self.global_geo_transform.y_pixel_size(),
        )
    }
}

impl<D, T> BaseTile<GridOrEmpty<D, T>>
where
    T: Pixel,
    D: GridSize + Clone + PartialEq,
{
    /// create a new `RasterTile`
    pub fn new_with_tile_info(
        time: TimeInterval,
        tile_info: TileInformation,
        data: GridOrEmpty<D, T>,
    ) -> Self
    where
        D: GridSize,
    {
        debug_assert_eq!(
            tile_info.tile_size_in_pixels.axis_size_x(),
            data.shape_ref().axis_size_x()
        );

        debug_assert_eq!(
            tile_info.tile_size_in_pixels.axis_size_y(),
            data.shape_ref().axis_size_y()
        );

        debug_assert_eq!(
            tile_info.tile_size_in_pixels.number_of_elements(),
            data.shape_ref().number_of_elements()
        );

        Self {
            time,
            tile_position: tile_info.global_tile_position,
            global_geo_transform: tile_info.global_geo_transform,
            grid_array: data,
            properties: Default::default(),
        }
    }

    /// create a new `RasterTile`
    pub fn new_with_tile_info_and_properties(
        time: TimeInterval,
        tile_info: TileInformation,
        data: GridOrEmpty<D, T>,
        properties: RasterProperties,
    ) -> Self {
        debug_assert_eq!(
            tile_info.tile_size_in_pixels.axis_size_x(),
            data.shape_ref().axis_size_x()
        );

        debug_assert_eq!(
            tile_info.tile_size_in_pixels.axis_size_y(),
            data.shape_ref().axis_size_y()
        );

        debug_assert_eq!(
            tile_info.tile_size_in_pixels.number_of_elements(),
            data.shape_ref().number_of_elements()
        );

        Self {
            time,
            tile_position: tile_info.global_tile_position,
            global_geo_transform: tile_info.global_geo_transform,
            grid_array: data,
            properties,
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
            properties: RasterProperties::default(),
        }
    }

    /// create a new `RasterTile`
    pub fn new_with_properties(
        time: TimeInterval,
        tile_position: GridIdx2D,
        global_geo_transform: GeoTransform,
        data: GridOrEmpty<D, T>,
        properties: RasterProperties,
    ) -> Self {
        Self {
            time,
            tile_position,
            global_geo_transform,
            grid_array: data,
            properties,
        }
    }

    /// create a new `RasterTile`
    pub fn new_without_offset<G>(
        time: TimeInterval,
        global_geo_transform: GeoTransform,
        data: G,
    ) -> Self
    where
        G: Into<GridOrEmpty<D, T>>,
    {
        Self {
            time,
            tile_position: [0, 0].into(),
            global_geo_transform,
            grid_array: data.into(),
            properties: RasterProperties::default(),
        }
    }

    /// Returns true if the grid is a `NoDataGrid`
    pub fn is_empty(&self) -> bool {
        self.grid_array.is_empty()
    }

    /// Convert the tile into a materialized tile.
    pub fn into_materialized_tile(self) -> MaterializedRasterTile<D, T> {
        MaterializedRasterTile {
            grid_array: self.grid_array.into_materialized_masked_grid(),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
        }
    }

    pub fn materialize(&mut self) {
        match self.grid_array {
            GridOrEmpty::Grid(_) => {}
            GridOrEmpty::Empty(_) => {
                self.grid_array = self
                    .grid_array
                    .clone()
                    .into_materialized_masked_grid()
                    .into();
            }
        }
    }
}

impl<G> TemporalBounded for BaseTile<G> {
    fn temporal_bounds(&self) -> TimeInterval {
        self.time
    }
}

impl<G> SpatialPartitioned for BaseTile<G>
where
    G: GridSize,
{
    fn spatial_partition(&self) -> SpatialPartition2D {
        self.tile_information().spatial_partition()
    }
}

impl<D, T, G> Raster<D, T> for BaseTile<G>
where
    D: GridSize + GridBounds + Clone,
    T: Pixel,
    G: GridIndexAccess<D::IndexArray, T>,
    Self: SpatialBounded + GridShapeAccess<ShapeArray = D::ShapeArray>,
{
    type DataContainer = G;

    fn data_container(&self) -> &G {
        &self.grid_array
    }
}

impl<T, G, I> GridIndexAccess<Option<T>, I> for BaseTile<G>
where
    G: GridIndexAccess<Option<T>, I>,
    T: Pixel,
{
    fn get_at_grid_index(&self, grid_index: I) -> Result<Option<T>> {
        self.grid_array.get_at_grid_index(grid_index)
    }

    fn get_at_grid_index_unchecked(&self, grid_index: I) -> Option<T> {
        self.grid_array.get_at_grid_index_unchecked(grid_index)
    }
}

impl<T, G, I> GridIndexAccessMut<Option<T>, I> for BaseTile<G>
where
    G: GridIndexAccessMut<Option<T>, I>,
    T: Pixel,
{
    fn set_at_grid_index(&mut self, grid_index: I, value: Option<T>) -> Result<()> {
        self.grid_array.set_at_grid_index(grid_index, value)
    }

    fn set_at_grid_index_unchecked(&mut self, grid_index: I, value: Option<T>) {
        self.grid_array
            .set_at_grid_index_unchecked(grid_index, value);
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
            properties: mat_tile.properties,
        }
    }
}

/// Pretty printer for raster tiles with 2D ASCII grids
pub fn display_raster_tile_2d<P: Pixel + std::fmt::Debug>(
    raster_tile_2d: &RasterTile2D<P>,
) -> impl std::fmt::Debug + '_ {
    struct DebugTile<'a, P>(&'a RasterTile2D<P>);

    impl<P: Pixel> std::fmt::Debug for DebugTile<'_, P> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let tile = self.0;
            let mut fmt = f.debug_struct(stringify!(RasterTile2D));
            fmt.field("time", &tile.time);
            fmt.field("tile_position", &tile.tile_position);
            fmt.field("global_geo_transform", &tile.global_geo_transform);
            fmt.field("properties", &tile.properties);

            let grid = if let Some(grid) = tile.grid_array.as_masked_grid() {
                let values: Vec<String> = grid
                    .masked_element_ref_iterator()
                    .map(|v| v.map_or('_'.to_string(), |v| format!("{:?}", v)))
                    .collect();
                let max_digits = values.iter().map(String::len).max().unwrap_or(0);

                let mut s = vec![String::new()];

                let last_value_index = values.len() - 1;
                for (i, value) in values.into_iter().enumerate() {
                    let str_ref = s.last_mut().unwrap();

                    str_ref.push_str(&format!("{value:max_digits$}"));

                    let is_new_line = (i + 1) % grid.grid_shape().axis_size_x() == 0;
                    if is_new_line && i < last_value_index {
                        s.push(String::new());
                    } else {
                        str_ref.push(' ');
                    }
                }

                s
            } else {
                vec!["empty".to_string()]
            };

            fmt.field("grid", &grid);

            fmt.finish()
        }
    }

    DebugTile(raster_tile_2d)
}

#[cfg(test)]
mod tests {
    use crate::{primitives::Coordinate2D, util::test::TestDefault};

    use super::*;
    use crate::raster::GridIdx;

    #[test]
    fn tile_information_new() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_geo_transform, GeoTransform::test_default());
        assert_eq!(ti.global_tile_position, GridIdx([0, 0]));
        assert_eq!(ti.tile_size_in_pixels, GridShape2D::from([100, 100]));
    }

    #[test]
    fn tile_information_global_tile_position() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_tile_position(), GridIdx([0, 0]));
    }

    #[test]
    fn tile_information_local_upper_left() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.local_upper_left_pixel_idx(), GridIdx([0, 0]));
    }

    #[test]
    fn tile_information_local_lower_left() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.local_lower_left_pixel_idx(), GridIdx([99, 0]));
    }

    #[test]
    fn tile_information_local_upper_right() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.local_upper_right_pixel_idx(), GridIdx([0, 99]));
    }

    #[test]
    fn tile_information_local_lower_right() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.local_lower_right_pixel_idx(), GridIdx([99, 99]));
    }

    #[test]
    fn tile_information_global_upper_left_idx() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_upper_left_pixel_idx(), GridIdx([0, 0]));
    }

    #[test]
    fn tile_information_global_upper_left_idx_2_3() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_upper_left_pixel_idx(), GridIdx([-200, 3000]));
    }

    #[test]
    fn tile_information_global_upper_right_idx() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_upper_right_pixel_idx(), GridIdx([0, 99]));
    }

    #[test]
    fn tile_information_global_upper_right_idx_2_3() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_upper_right_pixel_idx(), GridIdx([-200, 3999]));
    }

    #[test]
    fn tile_information_global_lower_right_idx() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_lower_right_pixel_idx(), GridIdx([99, 99]));
    }

    #[test]
    fn tile_information_global_lower_right_idx_2_3() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_lower_right_pixel_idx(), GridIdx([-101, 3999]));
    }

    #[test]
    fn tile_information_global_lower_left_idx() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_lower_left_pixel_idx(), GridIdx([99, 0]));
    }

    #[test]
    fn tile_information_global_lower_left_idx_2_3() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_lower_left_pixel_idx(), GridIdx([-101, 3000]));
    }

    #[test]
    fn tile_information_local_to_global_idx_0_0() {
        let ti = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::from([100, 100]),
            GeoTransform::test_default(),
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
            GeoTransform::test_default(),
        );
        assert_eq!(
            ti.local_to_global_pixel_idx(GridIdx([25, 75])),
            GridIdx([-175, 3075])
        );
    }

    #[test]
    fn tile_information_spatial_partition() {
        let ti = TileInformation::new(
            GridIdx([-2, 3]),
            GridShape2D::from([100, 1000]),
            GeoTransform::test_default(),
        );
        assert_eq!(
            ti.spatial_partition(),
            SpatialPartition2D::new_unchecked(
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
            ti.spatial_partition(),
            SpatialPartition2D::new_unchecked(
                Coordinate2D::new(-177., 88.),
                Coordinate2D::new(-176., 87.)
            )
        );
    }
}
