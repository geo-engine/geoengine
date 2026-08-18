use super::{
    GeoTransform, GridBoundingBox2D, GridIdx, GridIdx2D, GridShape2D, GridShapeAccess, GridSize,
    SpatialGridDefinition,
};
use crate::{
    primitives::{Coordinate2D, SpatialPartition2D, SpatialPartitioned},
    raster::{GridBounds, GridIdx2DIter},
    util::test::TestDefault,
};
use serde::{Deserialize, Serialize};

/// A tile size in pixels — distinguishes tile dimensions from pixel coordinates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TileSize(pub GridShape2D);

impl TileSize {
    pub fn new(y_size: usize, x_size: usize) -> Self {
        Self(GridShape2D::new_2d(y_size, x_size))
    }

    /// Default tile size (512×512) used when tile size is unknown
    /// (e.g. deserializing from database).
    pub fn default_512() -> Self {
        Self::new(512, 512)
    }

    pub fn axis_size_y(&self) -> usize {
        self.0.axis_size_y()
    }

    pub fn axis_size_x(&self) -> usize {
        self.0.axis_size_x()
    }

    pub fn into_inner(self) -> [usize; 2] {
        self.0.into_inner()
    }
}

impl GridSize for TileSize {
    type ShapeArray = [usize; 2];
    const NDIM: usize = 2;

    fn axis_size(&self) -> Self::ShapeArray {
        self.0.axis_size()
    }
}

impl std::fmt::Display for TileSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let [y, x] = self.0.into_inner();
        write!(f, "{y}x{x}")
    }
}

impl From<[usize; 2]> for TileSize {
    fn from(val: [usize; 2]) -> Self {
        Self(GridShape2D::from(val))
    }
}

impl From<TileSize> for GridShape2D {
    fn from(val: TileSize) -> Self {
        val.0
    }
}

/// A tile index — distinguishes tile positions from pixel positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TileIdx(pub GridIdx2D);

impl TileIdx {
    pub fn new_y_x(y: isize, x: isize) -> Self {
        Self(GridIdx2D::new_y_x(y, x))
    }
}

impl From<TileIdx> for GridIdx2D {
    fn from(val: TileIdx) -> Self {
        val.0
    }
}

impl From<[isize; 2]> for TileIdx {
    fn from(val: [isize; 2]) -> Self {
        Self(GridIdx2D::from(val))
    }
}

/// Tile-space bounding box — distinguishes from pixel-space `GridBoundingBox2D`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TileBounds(pub GridBoundingBox2D);

impl TileBounds {
    pub fn min_index(&self) -> TileIdx {
        TileIdx(self.0.min_index())
    }

    pub fn max_index(&self) -> TileIdx {
        TileIdx(self.0.max_index())
    }

    pub fn num_tiles(&self) -> usize {
        let TileIdx(GridIdx([uy, ux])) = self.min_index();
        let TileIdx(GridIdx([ly, lx])) = self.max_index();
        ((ly - uy + 1) * (lx - ux + 1)) as usize
    }
}

/// Iterator over tile indices. Wraps `GridIdx2DIter` to yield `TileIdx`.
#[derive(Clone, Debug)]
pub struct TileIdx2DIter(GridIdx2DIter);

impl TileIdx2DIter {
    pub fn new(bounds: &TileBounds) -> Self {
        Self(GridIdx2DIter::new(&bounds.0))
    }

    pub fn reset(&mut self) {
        self.0.reset();
    }
}

impl Iterator for TileIdx2DIter {
    type Item = TileIdx;

    fn next(&mut self) -> Option<TileIdx> {
        self.0.next().map(TileIdx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

/// The static parameters required to create a `TilingStrategy`
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub struct TilingSpecification {
    #[serde(alias = "tile_size")]
    pub tile_size: TileSize,
    pub origin: Coordinate2D,
}

impl TilingSpecification {
    /// Create a `TilingSpecification` with an explicit origin.
    /// The origin should typically be derived from the dataset's geo-transform origin.
    pub fn new(tile_size: TileSize, origin: Coordinate2D) -> Self {
        Self { tile_size, origin }
    }

    /// Convenience constructor using `(0, 0)` as the tiling origin.
    ///
    /// **Warning:** `(0, 0)` is almost always wrong for real data.
    /// Prefer `TilingSpecification::new(tile_size, dataset_origin)` and only
    /// use this when the origin genuinely does not matter (e.g. unit tests
    /// with mock data that has origin `(0, 0)`).
    pub fn with_zero_origin(tile_size: TileSize) -> Self {
        Self {
            tile_size,
            origin: Coordinate2D::new(0., 0.),
        }
    }

    pub fn tiling_origin_reference(&self) -> Coordinate2D {
        self.origin
    }
}

impl GridShapeAccess for TilingSpecification {
    type ShapeArray = [usize; 2];

    fn grid_shape_array(&self) -> Self::ShapeArray {
        self.tile_size.0.shape_array
    }

    fn grid_shape(&self) -> GridShape2D {
        self.tile_size.0
    }
}

impl From<TilingSpecification> for GridShape2D {
    fn from(val: TilingSpecification) -> Self {
        val.tile_size.0
    }
}

impl TestDefault for TilingSpecification {
    fn test_default() -> Self {
        Self::with_zero_origin(TileSize(GridShape2D::new([512, 512])))
    }
}

/// A provider of tile (size) information for a raster/grid
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct TilingStrategy {
    #[serde(alias = "tile_size")]
    pub tile_size: TileSize,
    pub geo_transform: GeoTransform,
}

impl TilingStrategy {
    pub fn new(tile_size: TileSize, geo_transform: GeoTransform) -> Self {
        Self {
            tile_size,
            geo_transform,
        }
    }

    pub fn pixel_idx_to_tile_idx(&self, pixel_idx: GridIdx2D) -> TileIdx {
        let GridIdx([y_pixel_idx, x_pixel_idx]) = pixel_idx;
        let [y_tile_size, x_tile_size] = self.tile_size.into_inner();
        let y_tile_idx = num::integer::div_floor(y_pixel_idx, y_tile_size as isize);
        let x_tile_idx = num::integer::div_floor(x_pixel_idx, x_tile_size as isize);
        TileIdx([y_tile_idx, x_tile_idx].into())
    }

    pub fn tile_grid_box(&self, partition: SpatialPartition2D) -> TileBounds {
        let start = self.pixel_idx_to_tile_idx(self.geo_transform.upper_left_pixel_idx(&partition));
        let end = self.pixel_idx_to_tile_idx(self.geo_transform.lower_right_pixel_idx(&partition));
        TileBounds(GridBoundingBox2D::new_unchecked(start.0, end.0))
    }

    pub fn num_tiles_intersecting_partition(&self, partition: SpatialPartition2D) -> usize {
        let grid_bounds = self.geo_transform.spatial_to_grid_bounds(&partition);
        self.num_tiles_intersecting_grid_bounds(grid_bounds)
    }

    pub fn num_tiles_intersecting_grid_bounds(&self, grid_bounds: GridBoundingBox2D) -> usize {
        let tile_bounds = self.global_pixel_grid_bounds_to_tile_grid_bounds(grid_bounds);
        tile_bounds.num_tiles()
    }

    pub fn global_pixel_grid_bounds_to_tile_grid_bounds(
        &self,
        global_pixel_grid_bounds: GridBoundingBox2D,
    ) -> TileBounds {
        let start = self.pixel_idx_to_tile_idx(global_pixel_grid_bounds.min_index());
        let end = self.pixel_idx_to_tile_idx(global_pixel_grid_bounds.max_index());
        TileBounds(GridBoundingBox2D::new_unchecked(start.0, end.0))
    }

    /// Transforms a tile position into a global pixel position
    pub fn tile_idx_to_global_pixel_idx(&self, tile_idx: TileIdx) -> GridIdx2D {
        let TileIdx(GridIdx([y_tile_idx, x_tile_idx])) = tile_idx;
        GridIdx::new([
            y_tile_idx * self.tile_size.axis_size_y() as isize,
            x_tile_idx * self.tile_size.axis_size_x() as isize,
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
        raster_spatial_query: GridBoundingBox2D,
    ) -> TileBounds {
        self.global_pixel_grid_bounds_to_tile_grid_bounds(raster_spatial_query)
    }

    /// Returns an iterator over all tile indices that intersect with the given `grid_bounds`.
    pub fn tile_idx_iterator_from_grid_bounds(
        // TODO: indicate that this uses pixel bounds!
        &self,
        grid_bounds: GridBoundingBox2D,
    ) -> TileIdx2DIter {
        let tile_bounds = self.global_pixel_grid_bounds_to_tile_grid_bounds(grid_bounds);
        TileIdx2DIter::new(&tile_bounds)
    }

    /// generates the tile information for the tiles intersecting the bounding box
    /// the iterator moves once along the x-axis and then increases the y-axis
    pub fn tile_information_iterator_from_pixel_bounds(
        // TODO: indicate that this uses pixel bounds!
        &self,
        grid_bounds: GridBoundingBox2D,
    ) -> TileInformationIter {
        TileInformationIter::new_with_pixel_bounds(*self, &grid_bounds)
    }
}

/// The `TileInformation` is used to represent the spatial position of each tile
#[derive(PartialEq, Debug, Copy, Clone, Serialize, Deserialize)]
pub struct TileInformation {
    #[serde(alias = "tile_size")]
    pub tile_size: TileSize,
    #[serde(alias = "tile_position")]
    pub tile_position: TileIdx,
    pub global_geo_transform: GeoTransform,
}

impl TileInformation {
    pub fn new(
        tile_position: TileIdx,
        tile_size: TileSize,
        global_geo_transform: GeoTransform,
    ) -> Self {
        Self {
            tile_size,
            tile_position,
            global_geo_transform,
        }
    }

    #[allow(clippy::unused_self)]
    pub fn local_upper_left_pixel_idx(&self) -> GridIdx2D {
        [0, 0].into()
    }

    pub fn local_lower_left_pixel_idx(&self) -> GridIdx2D {
        [self.tile_size.axis_size_y() as isize - 1, 0].into()
    }

    pub fn local_upper_right_pixel_idx(&self) -> GridIdx2D {
        [0, self.tile_size.axis_size_x() as isize - 1].into()
    }

    pub fn local_lower_right_pixel_idx(&self) -> GridIdx2D {
        let GridIdx([y, _]) = self.local_lower_left_pixel_idx();
        let GridIdx([_, x]) = self.local_upper_right_pixel_idx();
        [y, x].into()
    }

    pub fn tile_position(&self) -> TileIdx {
        self.tile_position
    }

    pub fn global_upper_left_pixel_idx(&self) -> GridIdx2D {
        let [tile_size_y, tile_size_x] = self.tile_size.into_inner();
        self.tile_position().0 * [tile_size_y as isize, tile_size_x as isize]
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

    pub fn tile_size(&self) -> TileSize {
        self.tile_size
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
        TilingStrategy::new(self.tile_size, self.global_geo_transform)
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

#[derive(Clone, Debug)]
pub struct TileInformationIter {
    tile_idx_iter: TileIdx2DIter,
    tiling_strategy: TilingStrategy,
}

impl TileInformationIter {
    pub fn new_with_pixel_bounds(
        tiling_strategy: TilingStrategy,
        pixel_bounds: &GridBoundingBox2D,
    ) -> Self {
        let tile_idx_iter = tiling_strategy.tile_idx_iterator_from_grid_bounds(*pixel_bounds);

        Self {
            tile_idx_iter,
            tiling_strategy,
        }
    }

    /// Access the used `TilingStategy`.
    pub fn tiling_strategy(&self) -> TilingStrategy {
        self.tiling_strategy
    }

    pub fn reset(&mut self) {
        self.tile_idx_iter.reset();
    }
}

impl Iterator for TileInformationIter {
    type Item = TileInformation;

    fn next(&mut self) -> Option<Self::Item> {
        self.tile_idx_iter.next().map(|idx| {
            TileInformation::new(
                idx,
                self.tiling_strategy.tile_size,
                self.tiling_strategy.geo_transform,
            )
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.tile_idx_iter.size_hint()
    }
}

/// A grid aligned to a tiling specification, used for tile addressing.
///
/// `TilingGrid` stores the aligned geo-transform, pixel bounds, and tile size.
/// It can convert between pixel coordinates and tile indices.
///
/// Construct via `TilingGrid::from_spatial_grid(grid, tile_size)` or
/// `TilingGrid::from_spatial_grid_with_origin(grid, origin, tile_size)`.
///
/// The grid's own origin is used by `from_spatial_grid`. The explicit-origin
/// constructor keeps the same spatial extent and resolution while changing
/// the pixel coordinate origin.
///
/// Unlike [`SpatialGridDefinition`] (which represents raw pixels), a `TilingGrid`
/// knows its tile size.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct TilingGrid {
    /// Geo-transform mapping pixel indices to spatial coordinates.
    pub geo_transform: GeoTransform,
    /// Pixel bounds of the grid.
    pub pixel_bounds: GridBoundingBox2D,
    /// The size of each tile in pixels.
    #[serde(alias = "tile_size")]
    pub tile_size: TileSize,
}

impl TilingGrid {
    /// Construct from a spatial grid and tile size.
    ///
    /// # Panics
    ///
    /// Panics if the spatial grid is not aligned with its own origin.
    pub fn from_spatial_grid(grid: SpatialGridDefinition, tile_size: TileSize) -> Self {
        Self::from_spatial_grid_with_origin(grid, grid.geo_transform().origin_coordinate, tile_size)
            .expect("a spatial grid is aligned with its own origin")
    }

    /// Construct a tiling grid with an explicit pixel-aligned origin.
    ///
    /// The returned grid covers the same spatial extent and keeps the source
    /// resolution. `None` means the origin is not aligned to the source grid
    /// or the tile size is invalid.
    pub fn from_spatial_grid_with_origin(
        grid: SpatialGridDefinition,
        origin: Coordinate2D,
        tile_size: TileSize,
    ) -> Option<Self> {
        if tile_size.axis_size_y() == 0 || tile_size.axis_size_x() == 0 {
            return None;
        }

        let source_geo_transform = grid.geo_transform();
        let geo_transform = GeoTransform::new(
            origin,
            source_geo_transform.x_pixel_size(),
            source_geo_transform.y_pixel_size(),
        );

        if !source_geo_transform.is_compatible_grid(geo_transform) {
            return None;
        }

        Some(Self {
            pixel_bounds: geo_transform.spatial_to_grid_bounds(&grid.spatial_partition()),
            geo_transform,
            tile_size,
        })
    }

    /// Create a [`TilingStrategy`] for tile index computation.
    pub fn tiling_strategy(&self) -> TilingStrategy {
        TilingStrategy {
            geo_transform: self.geo_transform,
            tile_size: self.tile_size,
        }
    }

    /// Convert to a [`SpatialGridDefinition`] (pixel grid).
    pub fn to_spatial_grid(self) -> SpatialGridDefinition {
        SpatialGridDefinition::new(self.geo_transform, self.pixel_bounds)
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
            tile_size: TileSize([600, 600].into()),
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
            .tile_information_iterator_from_pixel_bounds(grid_bounds)
            .collect::<Vec<_>>();

        assert_eq!(tiles.len(), 2);

        for tile in tiles {
            assert!(grid_bounds.intersects(&tile.global_pixel_bounds()));
        }
    }

    #[test]
    fn it_generates_all_interesected_tiles() {
        let strat = TilingStrategy {
            tile_size: TileSize([512, 512].into()),
            geo_transform: GeoTransform::new((0., -0.).into(), 10., -10.),
        };

        let bounds =
            GridBoundingBox2D::new(GridIdx2D::new([-513, -513]), GridIdx2D::new([512, 512]))
                .unwrap();

        let tiles_idxs = strat
            .tile_idx_iterator_from_grid_bounds(bounds)
            .collect::<Vec<_>>();

        assert_eq!(tiles_idxs.len(), 4 * 4);
        assert_eq!(tiles_idxs[0], TileIdx::new_y_x(-2, -2));
        assert_eq!(tiles_idxs[1], TileIdx::new_y_x(-2, -1));
        assert_eq!(tiles_idxs[14], TileIdx::new_y_x(1, 0));
        assert_eq!(tiles_idxs[15], TileIdx::new_y_x(1, 1));
    }

    #[test]
    fn tiling_tile_tile() {
        let geo_transform = GeoTransform::new(
            (-1_234_567_890., 1_234_567_890.).into(),
            0.000_033_337_4,
            -0.000_033_337_4,
        );

        let tile_pixel_size = TileSize(GridShape2D::new_2d(512, 512));
        let tiling_strat = TilingStrategy::new(tile_pixel_size, geo_transform);

        let tiling_origin_reference = Coordinate2D::new(0., 0.); // This is the _currently_ fixed tiling origin reference.
        let nearest_to_tiling_origin = geo_transform.nearest_pixel_edge(tiling_origin_reference);

        let tile_idx = tiling_strat.pixel_idx_to_tile_idx(nearest_to_tiling_origin);
        let expected_near_tiling_origin_idx = TileIdx::new_y_x(72_329_138_149, 72_329_138_149);
        assert_eq!(tile_idx, expected_near_tiling_origin_idx);

        let pixel_distance_reverse = nearest_to_tiling_origin * -1;

        let origin_pixel_tile = tiling_strat.pixel_idx_to_tile_idx(pixel_distance_reverse);
        let origin_pixel_offset =
            tiling_strat.tile_idx_to_global_pixel_idx(origin_pixel_tile) - pixel_distance_reverse;

        let expected_origin_in_tiling_based_pixels =
            TileIdx::new_y_x(-72_329_138_150, -72_329_138_150);
        let expected_tile_offset_from_tiling = GridIdx::new([-85, -85]);
        assert_eq!(origin_pixel_tile, expected_origin_in_tiling_based_pixels);
        assert_eq!(origin_pixel_offset, expected_tile_offset_from_tiling);
    }

    #[test]
    fn pixel_idx_to_tile_idx() {
        let geo_transform = GeoTransform::new((123., 321.).into(), 1.0, -1.0);
        let tile_pixel_size = TileSize(GridShape2D::new_2d(100, 100));

        let tiling_strat = TilingStrategy::new(tile_pixel_size, geo_transform);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(0, 0));
        assert_eq!(TileIdx::new_y_x(0, 0), pixels);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(1, 1));
        assert_eq!(TileIdx::new_y_x(0, 0), pixels);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(57, 57));
        assert_eq!(TileIdx::new_y_x(0, 0), pixels);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(100, 100));
        assert_eq!(TileIdx::new_y_x(1, 1), pixels);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(200, 200));
        assert_eq!(TileIdx::new_y_x(2, 2), pixels);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(1000, 1000));
        assert_eq!(TileIdx::new_y_x(10, 10), pixels);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(-57, -57));
        assert_eq!(TileIdx::new_y_x(-1, -1), pixels);
        let pixels = tiling_strat.pixel_idx_to_tile_idx(GridIdx2D::new_y_x(-300, -300));
        assert_eq!(TileIdx::new_y_x(-3, -3), pixels);
    }

    #[test]
    fn tile_idx_to_pixel_idx() {
        let geo_transform = GeoTransform::new((123., 321.).into(), 1.0, -1.0);
        let tile_pixel_size = TileSize(GridShape2D::new_2d(100, 100));

        let tiling_strat = TilingStrategy::new(tile_pixel_size, geo_transform);
        let pixels = tiling_strat.tile_idx_to_global_pixel_idx(TileIdx::new_y_x(0, 0));
        assert_eq!(GridIdx2D::new_y_x(0, 0), pixels);
        let pixels = tiling_strat.tile_idx_to_global_pixel_idx(TileIdx::new_y_x(1, 1));
        assert_eq!(GridIdx2D::new_y_x(100, 100), pixels);
        let pixels = tiling_strat.tile_idx_to_global_pixel_idx(TileIdx::new_y_x(2, 2));
        assert_eq!(GridIdx2D::new_y_x(200, 200), pixels);
        let pixels = tiling_strat.tile_idx_to_global_pixel_idx(TileIdx::new_y_x(3, 3));
        assert_eq!(GridIdx2D::new_y_x(300, 300), pixels);
        let pixels = tiling_strat.tile_idx_to_global_pixel_idx(TileIdx::new_y_x(10, 10));
        assert_eq!(GridIdx2D::new_y_x(1000, 1000), pixels);
        let pixels = tiling_strat.tile_idx_to_global_pixel_idx(TileIdx::new_y_x(-3, -3));
        assert_eq!(GridIdx2D::new_y_x(-300, -300), pixels);
    }

    #[test]
    fn tiling_specification_with_origin() {
        let spec = TilingSpecification::with_zero_origin(TileSize(GridShape2D::new_2d(512, 512)));
        assert_eq!(spec.origin, Coordinate2D::new(0., 0.));

        let custom_origin = Coordinate2D::new(12.5, -3.7);
        let spec_with_origin = TilingSpecification::new(spec.tile_size, custom_origin);
        assert_eq!(spec_with_origin.origin, custom_origin);
        assert_eq!(spec_with_origin.tile_size, spec.tile_size);
    }

    #[test]
    fn tiling_strategy_with_custom_origin() {
        let origin = Coordinate2D::new(10., -10.);
        let geo_transform = GeoTransform::new(origin, 1.0, -1.0);
        let tile_pixel_size = TileSize(GridShape2D::new_2d(100, 100));
        let strat = TilingStrategy::new(tile_pixel_size, geo_transform);

        // The pixel at the tiling origin (10, -10) maps to pixel idx (0, 0)
        let origin_pixel = geo_transform.coordinate_to_grid_idx_2d(origin);
        assert_eq!(origin_pixel, GridIdx2D::new_y_x(0, 0));

        // Tile (0,0) starts at global pixel (0,0)
        let tile_start = strat.tile_idx_to_global_pixel_idx(TileIdx::new_y_x(0, 0));
        assert_eq!(tile_start, GridIdx2D::new_y_x(0, 0));
    }

    #[test]
    fn tiling_grid_definition_with_custom_origin() {
        let origin = Coordinate2D::new(100., -200.);
        let spec = TilingSpecification::new(TileSize(GridShape2D::new_2d(512, 512)), origin);

        let geo_transform = GeoTransform::new(origin, 30., -30.);

        // Verify that TilingStrategy built from this spec uses the custom origin
        let strat = TilingStrategy::new(spec.tile_size, geo_transform);
        assert_eq!(strat.geo_transform.origin_coordinate, origin);
        assert_eq!(strat.tile_size, TileSize(GridShape2D::new_2d(512, 512)));
    }

    #[test]
    fn tiling_grid_with_explicit_origin_preserves_extent() {
        let source = SpatialGridDefinition::new(
            GeoTransform::new((100., -200.).into(), 30., -30.),
            GridBoundingBox2D::new_min_max(0, 9, 0, 9).unwrap(),
        );
        let tiling_grid = TilingGrid::from_spatial_grid_with_origin(
            source,
            (70., -170.).into(),
            TileSize::new(4, 4),
        )
        .unwrap();

        assert_eq!(
            tiling_grid.to_spatial_grid().spatial_partition(),
            source.spatial_partition()
        );
        assert_eq!(
            tiling_grid.geo_transform.origin_coordinate,
            (70., -170.).into()
        );
        assert_eq!(
            tiling_grid.pixel_bounds,
            GridBoundingBox2D::new_min_max(1, 10, 1, 10).unwrap()
        );
    }

    #[test]
    fn tiling_grid_rejects_unaligned_origin() {
        let source = SpatialGridDefinition::new(
            GeoTransform::new((100., -200.).into(), 30., -30.),
            GridBoundingBox2D::new_min_max(0, 9, 0, 9).unwrap(),
        );

        assert!(
            TilingGrid::from_spatial_grid_with_origin(
                source,
                (70.5, -170.).into(),
                TileSize::new(4, 4),
            )
            .is_none()
        );
    }

    #[test]
    fn tiling_grid_rejects_zero_tile_size() {
        let source = SpatialGridDefinition::new(
            GeoTransform::test_default(),
            GridBoundingBox2D::new_min_max(0, 1, 0, 1).unwrap(),
        );

        assert!(
            TilingGrid::from_spatial_grid_with_origin(
                source,
                source.geo_transform().origin_coordinate,
                TileSize::new(0, 4),
            )
            .is_none()
        );
    }
}
