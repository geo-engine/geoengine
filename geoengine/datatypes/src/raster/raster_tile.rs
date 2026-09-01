use super::masked_grid::MaskedGrid;
use super::{
    BoundedGrid, ChangeGridBounds, EmptyGrid2D, GridBlit, GridBoundingBox2D, GridIdx, GridIdx2D,
    GridIndexAccessMut, RasterProperties, SpatialGridDefinition,
};
use super::{
    GeoTransform, GeoTransformAccess, GridBounds, GridIndexAccess, GridShape, GridShape2D,
    GridShape3D, GridShapeAccess, GridSize, Raster, TileIdx, TileInformation, TileOverlap,
    TileSize, grid_or_empty::GridOrEmpty,
};
use crate::error::Error;
use crate::primitives::CacheHint;
use crate::primitives::{
    SpatialBounded, SpatialPartition2D, SpatialPartitioned, SpatialResolution, TemporalBounded,
    TimeInterval,
};
use crate::raster::Pixel;
use crate::util::{ByteSize, Result};
use float_cmp::approx_eq;
use serde::{Deserialize, Serialize};
use std::fmt::Write;

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
    /// The tile position is the position of the tile's *core* in the grid of tiles
    /// with origin at the origin of the `global_geo_transform`.
    /// This is NOT a pixel position inside the tile.
    pub tile_position: TileIdx,
    // the band of the tile, relevant for multi-band raster
    pub band: u32,
    /// The overlap halo of this tile around its core region.
    ///
    /// With non-zero overlap, the data grid extends beyond the core on every
    /// side. The core's upper left pixel is derived as
    /// `tile_position × core_size` with `core_size = grid shape − 2×overlap`.
    /// Coverage and query intersection are always defined by cores.
    #[serde(default)]
    pub overlap: TileOverlap,
    /// The global geotransform to transform pixels into geographic coordinates
    pub global_geo_transform: GeoTransform,
    /// The pixels of the tile are stored as `Grid` or, in case they are all no-data as `NoDataGrid`.
    /// The enum `GridOrEmpty` allows a combination of both.
    pub grid_array: G,
    /// Metadata for the `BaseTile`
    pub properties: RasterProperties,
    /// Indicate how long the tile may be cached, if `None` the tile may be cached indefinitely.
    pub cache_hint: CacheHint,
}

impl<G> BaseTile<G>
where
    G: GridSize,
{
    pub fn tile_offset(&self) -> TileIdx {
        self.tile_position
    }

    /// The axis sizes `[y, x]` of the tile's core region: the grid shape minus
    /// twice the overlap halo.
    pub fn core_axis_size(&self) -> [usize; 2] {
        let y = self.grid_array.axis_size_y() - 2 * self.overlap.axis_size_y();
        let x = self.grid_array.axis_size_x() - 2 * self.overlap.axis_size_x();
        [y, x]
    }

    /// The global pixel index of the upper left pixel of the tile's *core*.
    pub fn global_core_upper_left_pixel_idx(&self) -> GridIdx2D {
        let [core_size_y, core_size_x] = self.core_axis_size();
        self.tile_position.grid_idx() * [core_size_y as isize, core_size_x as isize]
    }

    /// The global pixel index of the upper left pixel of the tile's actual data.
    /// This is the core anchor shifted by the overlap halo.
    pub fn global_data_upper_left_pixel_idx(&self) -> GridIdx2D {
        self.global_core_upper_left_pixel_idx()
            - GridIdx([
                self.overlap.axis_size_y() as isize,
                self.overlap.axis_size_x() as isize,
            ])
    }

    /// The bounds of the tile's *core* (coverage) in global pixel coordinates.
    ///
    /// Core-anchored convention: local index `(0, 0)` is the core's upper-left pixel
    /// and the overlap halo lies at negative indices. See [`Self::core_geo_transform`]
    /// for the matching geo transform and [`Self::total_pixel_bounds`] for the bounds
    /// that include the halo.
    pub fn core_pixel_bounds(&self) -> GridBoundingBox2D {
        let min = self.global_core_upper_left_pixel_idx();
        let [core_y, core_x] = self.core_axis_size();
        GridBoundingBox2D::new_unchecked(min, min + [core_y as isize - 1, core_x as isize - 1])
    }

    /// The bounds of the tile's *total* data in global pixel coordinates: the core
    /// expanded by the overlap halo on every side. Equals [`Self::bounding_box`].
    pub fn total_pixel_bounds(&self) -> GridBoundingBox2D {
        let min = self.global_data_upper_left_pixel_idx();
        let [full_y, full_x] = [self.grid_array.axis_size_y(), self.grid_array.axis_size_x()];
        GridBoundingBox2D::new_unchecked(min, min + [full_y as isize - 1, full_x as isize - 1])
    }

    /// The bounds of the tile's *core* in **core-anchored local** pixel coordinates:
    /// `(0, 0)` is the core's upper-left pixel. To address a pixel in the stored
    /// `grid_array` (whose origin is the data corner), add [`Self::overlap_offset`] to a
    /// core-anchored local index.
    pub fn local_core_pixel_bounds(&self) -> GridBoundingBox2D {
        let [core_y, core_x] = self.core_axis_size();
        GridBoundingBox2D::new_unchecked(
            GridIdx2D::from([0, 0]),
            GridIdx2D::from([core_y as isize - 1, core_x as isize - 1]),
        )
    }

    /// The bounds of the tile's *total* data in **core-anchored local** pixel coordinates:
    /// the core expanded by the overlap halo, which lives at negative indices down to
    /// `(-overlap_y, -overlap_x)`.
    ///
    /// This is the valid range to check when accessing the tile's pixels by local index —
    /// the grid dimensions ([`GridSize::axis_size`]) alone do not capture the negative halo
    /// minimum. To address a pixel in the stored `grid_array`, add [`Self::overlap_offset`]
    /// to a core-anchored local index.
    pub fn local_total_pixel_bounds(&self) -> GridBoundingBox2D {
        let [oy, ox] = [self.overlap.axis_size_y(), self.overlap.axis_size_x()];
        let [core_y, core_x] = self.core_axis_size();
        GridBoundingBox2D::new_unchecked(
            GridIdx2D::from([-(oy as isize), -(ox as isize)]),
            GridIdx2D::from([
                core_y as isize - 1 + oy as isize,
                core_x as isize - 1 + ox as isize,
            ]),
        )
    }

    /// Reconstructs the `TileInformation` of this tile, including its overlap.
    pub fn tile_information(&self) -> TileInformation {
        let [y, x] = self.core_axis_size();
        TileInformation::new_with_overlap(
            self.tile_position,
            TileSize::new_y_x(y, x),
            self.global_geo_transform,
            self.overlap,
        )
    }

    pub fn global_pixel_spatial_grid_definition(&self) -> SpatialGridDefinition {
        let global_upper_left_idx = self.global_data_upper_left_pixel_idx();

        SpatialGridDefinition::new(
            self.global_geo_transform,
            GridBoundingBox2D::new_unchecked(
                global_upper_left_idx,
                global_upper_left_idx
                    + [
                        self.grid_array.axis_size_y() as isize,
                        self.grid_array.axis_size_x() as isize,
                    ],
            ),
        )
    }

    /// The tile's canonical geo transform, anchored at the tile's *core* upper-left
    /// pixel. The overlap halo is addressed with negative grid indices: local index
    /// `(0, 0)` is the core's upper-left pixel, and the halo extends into negative
    /// indices down to `(-overlap_y, -overlap_x)`.
    ///
    /// To map a coordinate onto the *stored* grid (whose `(0, 0)` is the data
    /// corner, i.e. the core anchor shifted by the overlap), add
    /// [`Self::overlap_offset`] to the core index obtained from this transform.
    #[inline]
    pub fn core_geo_transform(&self) -> GeoTransform {
        let core_upper_left_coord = self
            .global_geo_transform
            .grid_idx_to_pixel_upper_left_coordinate_2d(self.global_core_upper_left_pixel_idx());

        GeoTransform::new(
            core_upper_left_coord,
            self.global_geo_transform.x_pixel_size(),
            self.global_geo_transform.y_pixel_size(),
        )
    }

    /// The grid-index offset from the core anchor to the stored data grid origin.
    ///
    /// The stored grid's `(0, 0)` (the data corner) lies `overlap` pixels before the
    /// core anchor, so a core-referenced index `i` maps to the stored index
    /// `i + overlap_offset()`. For the halo this stored index is the one read from
    /// the grid; conversely a stored index `j` maps to the core index
    /// `j - overlap_offset()` (negative for halo pixels).
    #[inline]
    pub fn overlap_offset(&self) -> GridIdx2D {
        GridIdx2D::from([
            self.overlap.axis_size_y() as isize,
            self.overlap.axis_size_x() as isize,
        ])
    }

    pub fn spatial_resolution(&self) -> SpatialResolution {
        self.global_geo_transform.spatial_resolution()
    }
}

impl<G> ByteSize for BaseTile<G>
where
    G: ByteSize,
{
    fn heap_byte_size(&self) -> usize {
        self.grid_array.heap_byte_size() + self.properties.heap_byte_size()
    }
}

/// A way to compare two `BaseTile` ignoring the `CacheHint` and only considering the actual data.
pub trait TilesEqualIgnoringCacheHint<G: PartialEq> {
    fn tiles_equal_ignoring_cache_hint(&self, other: &dyn IterableBaseTile<G>) -> bool;
}

/// Allow comparing Iterables of `BaseTile` ignoring the `CacheHint` and only considering the actual data.
pub trait IterableBaseTile<G> {
    fn iter_tiles(&self) -> Box<dyn Iterator<Item = &BaseTile<G>> + '_>;
}

struct SingleBaseTileIter<'a, G> {
    tile: Option<&'a BaseTile<G>>,
}

impl<'a, G> Iterator for SingleBaseTileIter<'a, G> {
    type Item = &'a BaseTile<G>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tile.take()
    }
}

impl<G: PartialEq> IterableBaseTile<G> for BaseTile<G> {
    fn iter_tiles(&self) -> Box<dyn Iterator<Item = &BaseTile<G>> + '_> {
        Box::new(SingleBaseTileIter { tile: Some(self) })
    }
}

impl<G: PartialEq> IterableBaseTile<G> for Vec<BaseTile<G>> {
    fn iter_tiles(&self) -> Box<dyn Iterator<Item = &BaseTile<G>> + '_> {
        Box::new(self.iter())
    }
}

impl<G: PartialEq, const N: usize> IterableBaseTile<G> for [BaseTile<G>; N] {
    fn iter_tiles(&self) -> Box<dyn Iterator<Item = &BaseTile<G>> + '_> {
        Box::new(self.iter())
    }
}

impl<G: PartialEq, I: IterableBaseTile<G>> TilesEqualIgnoringCacheHint<G> for I
where
    G: GridSize,
{
    fn tiles_equal_ignoring_cache_hint(&self, other: &dyn IterableBaseTile<G>) -> bool {
        let mut iter_self = self.iter_tiles();
        let mut iter_other = other.iter_tiles();
        loop {
            match (iter_self.next(), iter_other.next()) {
                (Some(a), Some(b)) => {
                    if a.time != b.time {
                        return false;
                    }
                    if a.tile_position != b.tile_position {
                        return false;
                    }
                    if a.band != b.band {
                        return false;
                    }
                    if !approx_eq!(GeoTransform, a.global_geo_transform, b.global_geo_transform) {
                        return false;
                    }
                    if a.global_geo_transform != b.global_geo_transform {
                        return false;
                    }
                    if a.properties != b.properties {
                        return false;
                    }
                    if a.grid_array != b.grid_array {
                        return false;
                    }
                }
                // both iterators are exhausted
                (None, None) => return true,
                // one iterator is exhausted, the other is not, so they are not equal
                _ => return false,
            }
        }
    }
}

impl<D, T> BaseTile<GridOrEmpty<D, T>>
where
    T: Pixel,
    D: GridSize + Clone + PartialEq,
{
    /// create a new `RasterTile` from tile information, inheriting its overlap
    pub fn new_with_tile_info(
        time: TimeInterval,
        tile_info: TileInformation,
        band: u32,
        data: GridOrEmpty<D, T>,
        cache_hint: CacheHint,
    ) -> Self
    where
        D: GridSize,
    {
        debug_assert_eq!(
            tile_info.tile_size.axis_size_x() + 2 * tile_info.overlap.axis_size_x(),
            data.shape_ref().axis_size_x()
        );

        debug_assert_eq!(
            tile_info.tile_size.axis_size_y() + 2 * tile_info.overlap.axis_size_y(),
            data.shape_ref().axis_size_y()
        );

        Self {
            time,
            tile_position: tile_info.tile_position,
            overlap: tile_info.overlap,
            band,
            global_geo_transform: tile_info.global_geo_transform,
            grid_array: data,
            properties: Default::default(),
            cache_hint,
        }
    }

    /// create a new `RasterTile` from tile information and properties, inheriting its overlap
    pub fn new_with_tile_info_and_properties(
        time: TimeInterval,
        tile_info: TileInformation,
        band: u32,
        data: GridOrEmpty<D, T>,
        properties: RasterProperties,
        cache_hint: CacheHint,
    ) -> Self {
        debug_assert_eq!(
            tile_info.tile_size.axis_size_x() + 2 * tile_info.overlap.axis_size_x(),
            data.shape_ref().axis_size_x()
        );

        debug_assert_eq!(
            tile_info.tile_size.axis_size_y() + 2 * tile_info.overlap.axis_size_y(),
            data.shape_ref().axis_size_y()
        );

        Self {
            time,
            tile_position: tile_info.tile_position,
            overlap: tile_info.overlap,
            band,
            global_geo_transform: tile_info.global_geo_transform,
            grid_array: data,
            properties,
            cache_hint,
        }
    }

    /// create a new `RasterTile` without overlap
    pub fn new(
        time: TimeInterval,
        tile_position: TileIdx,
        band: u32,
        global_geo_transform: GeoTransform,
        data: GridOrEmpty<D, T>,
        cache_hint: CacheHint,
    ) -> Self {
        Self {
            time,
            tile_position,
            overlap: TileOverlap::zero(),
            band,
            global_geo_transform,
            grid_array: data,
            properties: RasterProperties::default(),
            cache_hint,
        }
    }

    /// create a new `RasterTile` without overlap
    pub fn new_with_properties(
        time: TimeInterval,
        tile_position: TileIdx,
        band: u32,
        global_geo_transform: GeoTransform,
        data: GridOrEmpty<D, T>,
        properties: RasterProperties,
        cache_hint: CacheHint,
    ) -> Self {
        Self {
            time,
            tile_position,
            overlap: TileOverlap::zero(),
            band,
            global_geo_transform,
            grid_array: data,
            properties,
            cache_hint,
        }
    }

    /// create a new `RasterTile`
    pub fn new_without_offset<G>(
        time: TimeInterval,
        global_geo_transform: GeoTransform,
        data: G,
        cache_hint: CacheHint,
    ) -> Self
    where
        G: Into<GridOrEmpty<D, T>>,
    {
        Self {
            time,
            tile_position: TileIdx::new_y_x(0, 0),
            overlap: TileOverlap::zero(),
            band: 0,
            global_geo_transform,
            grid_array: data.into(),
            properties: RasterProperties::default(),
            cache_hint,
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
            overlap: self.overlap,
            band: 0,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
            cache_hint: self.cache_hint.clone_with_current_datetime(),
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

impl<T> RasterTile2D<T>
where
    T: Pixel,
{
    /// Converts the tile into a grid with the global pixel bounds of the tile.
    ///
    /// # Panics
    /// Only if the tile was invalid before...
    ///
    pub fn into_inner_positioned_grid(self) -> GridOrEmpty<GridBoundingBox2D, T> {
        let b = self.bounding_box();
        let g = self.grid_array;
        g.set_grid_bounds(b).expect("tile was valid before")
    }

    /// Crops the tile's overlap halo, reducing its overlap by `amount` pixels
    /// on every side.
    ///
    /// The core region and the tile's georeference are unaffected: cropping
    /// only removes halo pixels (and no-data-fills nothing). Requesting more
    /// than the available overlap is an error.
    ///
    /// # Panics
    /// Never in practice: re-bounding a valid grid keeps it valid.
    pub fn crop_overlap(self, amount: TileOverlap) -> Result<Self> {
        if amount.y > self.overlap.y || amount.x > self.overlap.x {
            return Err(Error::NotEnoughTileOverlap {
                requested: amount,
                available: self.overlap,
            });
        }

        if amount.is_zero() {
            return Ok(self);
        }

        let Self {
            time,
            tile_position,
            overlap,
            band,
            global_geo_transform,
            grid_array,
            properties,
            cache_hint,
        } = self;
        let remaining_overlap = TileOverlap::new(overlap.y - amount.y, overlap.x - amount.x);

        // work on the globally positioned grid to reuse intersection-based blitting
        let data_bounds = grid_array.bounding_box();
        let positioned = grid_array
            .set_grid_bounds(data_bounds)
            .expect("tile was valid before");

        let cropped_bounds = GridBoundingBox2D::new(
            data_bounds.min_index()
                + GridIdx([amount.axis_size_y() as isize, amount.axis_size_x() as isize]),
            data_bounds.max_index()
                - GridIdx([amount.axis_size_y() as isize, amount.axis_size_x() as isize]),
        )?;

        let cropped_shape = cropped_bounds.grid_shape();

        let mut target =
            GridOrEmpty::from(EmptyGrid2D::new(cropped_shape).set_grid_bounds(cropped_bounds)?);
        target.grid_blit_from(&positioned);

        Ok(Self {
            time,
            tile_position,
            overlap: remaining_overlap,
            band,
            global_geo_transform,
            grid_array: target.unbounded(),
            properties,
            cache_hint,
        })
    }
}

impl<T> BoundedGrid for RasterTile2D<T>
where
    T: Pixel,
{
    type IndexArray = [isize; 2];

    fn bounding_box(&self) -> GridBoundingBox2D {
        // The bounding box covers the tile's total data (core + overlap halo).
        self.total_pixel_bounds()
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
            overlap: mat_tile.overlap,
            band: mat_tile.band,
            time: mat_tile.time,
            properties: mat_tile.properties,
            cache_hint: mat_tile.cache_hint,
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
                    .map(|v| v.map_or('_'.to_string(), |v| format!("{v:?}")))
                    .collect();
                let max_digits = values.iter().map(String::len).max().unwrap_or(0);

                let mut s = vec![String::new()];

                let last_value_index = values.len() - 1;
                for (i, value) in values.into_iter().enumerate() {
                    let str_ref = s
                        .last_mut()
                        .expect("it shouldn't be empty since it was populated before the loop");

                    let _ = write!(str_ref, "{value:max_digits$}");

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
    use crate::raster::TileSize;
    use crate::{primitives::Coordinate2D, util::test::TestDefault};

    use super::*;
    use crate::raster::{Grid2D, GridIdx};
    #[test]
    fn overlapped_tile_bounding_box_covers_data() {
        let geo_transform = GeoTransform::test_default();
        let overlap = TileOverlap::new(1, 2);
        let core_size = GridShape2D::new_2d(4, 4);
        let padded_size = GridShape2D::new_2d(
            core_size.axis_size_y() + 2 * overlap.axis_size_y(),
            core_size.axis_size_x() + 2 * overlap.axis_size_x(),
        );
        let tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation::new_with_overlap(
                TileIdx::new_y_x(2, 3),
                TileSize::from(core_size),
                geo_transform,
                overlap,
            ),
            0,
            GridOrEmpty::from(Grid2D::new_filled(padded_size, 7_u8)),
            CacheHint::default(),
        );

        // the bounding box covers the *data*: the core anchor shifted by the halo
        let expected_min = [2 * 4 - 1, 3 * 4 - 2];
        assert_eq!(tile.bounding_box().min_index(), GridIdx(expected_min));
        assert_eq!(
            tile.bounding_box().max_index(),
            GridIdx([expected_min[0] + 5, expected_min[1] + 7])
        );

        // reconstructed tile information preserves the core geometry
        let info = tile.tile_information();
        assert_eq!(info.overlap, overlap);
        assert_eq!(info.tile_size, TileSize::from(core_size));
        assert_eq!(
            info.core_pixel_bounds(),
            GridBoundingBox2D::new_unchecked(
                GridIdx([2 * 4, 3 * 4]),
                GridIdx([2 * 4 + 3, 3 * 4 + 3])
            )
        );
    }

    #[test]
    fn core_and_total_pixel_bounds_are_consistent() {
        let geo_transform = GeoTransform::test_default();
        let overlap = TileOverlap::new(1, 2);
        let core_size = GridShape2D::new_2d(4, 4);
        let padded_size = GridShape2D::new_2d(
            core_size.axis_size_y() + 2 * overlap.axis_size_y(),
            core_size.axis_size_x() + 2 * overlap.axis_size_x(),
        );
        let tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation::new_with_overlap(
                TileIdx::new_y_x(2, 3),
                TileSize::from(core_size),
                geo_transform,
                overlap,
            ),
            0,
            GridOrEmpty::from(Grid2D::new_filled(padded_size, 7_u8)),
            CacheHint::default(),
        );

        // core: [tile_position * core_size, + core_size - 1]
        let core = GridBoundingBox2D::new_unchecked(
            GridIdx([2 * 4, 3 * 4]),
            GridIdx([2 * 4 + 3, 3 * 4 + 3]),
        );
        // total: core expanded by the halo on every side
        let total = GridBoundingBox2D::new_unchecked(
            GridIdx([2 * 4 - 1, 3 * 4 - 2]),
            GridIdx([2 * 4 + 3 + 1, 3 * 4 + 3 + 2]),
        );

        assert_eq!(tile.core_pixel_bounds(), core);
        assert_eq!(tile.total_pixel_bounds(), total);
        // bounding_box is exactly the total bounds
        assert_eq!(tile.bounding_box(), total);

        // matches the reconstructed TileInformation (single source of truth)
        let info = tile.tile_information();
        assert_eq!(tile.core_pixel_bounds(), info.core_pixel_bounds());
        assert_eq!(tile.total_pixel_bounds(), info.total_pixel_bounds());

        // core-anchored convention: total upper-left = core upper-left - overlap
        let overlap_idx = GridIdx([
            overlap.axis_size_y() as isize,
            overlap.axis_size_x() as isize,
        ]);
        assert_eq!(
            tile.total_pixel_bounds().min_index(),
            tile.core_pixel_bounds().min_index() - overlap_idx
        );
    }

    #[test]
    fn local_pixel_bounds_are_core_anchored() {
        let geo_transform = GeoTransform::test_default();
        let overlap = TileOverlap::new(1, 2);
        let core_size = GridShape2D::new_2d(4, 4);
        let padded_size = GridShape2D::new_2d(
            core_size.axis_size_y() + 2 * overlap.axis_size_y(),
            core_size.axis_size_x() + 2 * overlap.axis_size_x(),
        );
        let tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation::new_with_overlap(
                TileIdx::new_y_x(2, 3),
                TileSize::from(core_size),
                geo_transform,
                overlap,
            ),
            0,
            GridOrEmpty::from(Grid2D::new_filled(padded_size, 7_u8)),
            CacheHint::default(),
        );

        // core: (0, 0) is the core's upper-left pixel
        assert_eq!(
            tile.local_core_pixel_bounds(),
            GridBoundingBox2D::new_unchecked(GridIdx([0, 0]), GridIdx([3, 3]))
        );
        // total: the halo lives at negative indices down to (-overlap_y, -overlap_x)
        assert_eq!(
            tile.local_total_pixel_bounds(),
            GridBoundingBox2D::new_unchecked(GridIdx([-1, -2]), GridIdx([4, 5]))
        );

        // total is the core expanded by the overlap on every side
        let overlap_idx = GridIdx([
            overlap.axis_size_y() as isize,
            overlap.axis_size_x() as isize,
        ]);
        let core = tile.local_core_pixel_bounds();
        let total = tile.local_total_pixel_bounds();
        assert_eq!(total.min_index(), core.min_index() - overlap_idx);
        assert_eq!(total.max_index(), core.max_index() + overlap_idx);

        // a core-anchored local index maps onto the global bounds via the core anchor:
        // local + global_core_upper_left == the matching global bound
        let core_ul = tile.global_core_upper_left_pixel_idx();
        assert_eq!(
            tile.local_core_pixel_bounds().min_index() + core_ul,
            tile.core_pixel_bounds().min_index()
        );
        assert_eq!(
            tile.local_total_pixel_bounds().min_index() + core_ul,
            tile.total_pixel_bounds().min_index()
        );
    }

    #[test]
    fn crop_full_overlap_restores_core_tile() {
        let geo_transform = GeoTransform::test_default();
        let core_size = GridShape2D::new_2d(2, 2);
        let overlap = TileOverlap::new(1, 1);

        // padded data: 9s are halo, inner values 1..=4 form the core
        let padded = Grid2D::new(
            GridShape2D::new_2d(4, 4),
            vec![9, 9, 9, 9, 9, 1, 2, 9, 9, 3, 4, 9, 9, 9, 9, 9],
        )
        .unwrap();

        let padded_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation::new_with_overlap(
                TileIdx::new_y_x(0, 0),
                TileSize::from(core_size),
                geo_transform,
                overlap,
            ),
            0,
            GridOrEmpty::from(padded),
            CacheHint::default(),
        );

        let cropped = padded_tile.crop_overlap(overlap).unwrap();

        assert_eq!(cropped.overlap, TileOverlap::zero());
        assert_eq!(cropped.grid_array.shape_ref(), &core_size);

        // the core values survived the crop and sit at their local positions
        assert_eq!(
            cropped.get_at_grid_index(GridIdx2D::new_y_x(0, 0)).unwrap(),
            Some(1)
        );
        assert_eq!(
            cropped.get_at_grid_index(GridIdx2D::new_y_x(1, 1)).unwrap(),
            Some(4)
        );
    }

    #[test]
    fn crop_partial_overlap_keeps_remainder() {
        let geo_transform = GeoTransform::test_default();
        let core_size = GridShape2D::new_2d(2, 2);
        let overlap = TileOverlap::new(2, 2);
        let padded_size = GridShape2D::new_2d(6, 6);

        let tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation::new_with_overlap(
                TileIdx::new_y_x(0, 0),
                TileSize::from(core_size),
                geo_transform,
                overlap,
            ),
            0,
            GridOrEmpty::from(Grid2D::new_filled(padded_size, 1_u8)),
            CacheHint::default(),
        );

        let cropped = tile.crop_overlap(TileOverlap::new(1, 1)).unwrap();
        assert_eq!(cropped.overlap, TileOverlap::new(1, 1));
        assert_eq!(cropped.grid_array.shape_ref(), &GridShape2D::new_2d(4, 4));
    }

    #[test]
    fn crop_rejects_insufficient_overlap() {
        let geo_transform = GeoTransform::test_default();
        let tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation::new_with_overlap(
                TileIdx::new_y_x(0, 0),
                TileSize::new_y_x(4, 4),
                geo_transform,
                TileOverlap::new(1, 1),
            ),
            0,
            GridOrEmpty::from(Grid2D::new_filled(GridShape2D::new_2d(6, 6), 1_u8)),
            CacheHint::default(),
        );

        let err = tile
            .crop_overlap(TileOverlap::new(2, 1))
            .expect_err("must fail");
        assert!(err.to_string().contains("Not enough tile overlap"));
    }

    #[test]
    fn legacy_tile_serialization_defaults_to_zero_overlap() {
        #[derive(Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct LegacyTile {
            time: TimeInterval,
            tile_position: TileIdx,
            band: u32,
            global_geo_transform: GeoTransform,
            grid_array: GridOrEmpty<GridShape2D, u8>,
            properties: RasterProperties,
            cache_hint: CacheHint,
        }

        let legacy = LegacyTile {
            time: TimeInterval::default(),
            tile_position: TileIdx::new_y_x(1, 2),
            band: 0,
            global_geo_transform: GeoTransform::test_default(),
            grid_array: GridOrEmpty::from(Grid2D::new_filled(GridShape2D::new_2d(1, 1), 5_u8)),
            properties: RasterProperties::default(),
            cache_hint: CacheHint::default(),
        };
        let json = serde_json::to_string(&legacy).unwrap();

        let deserialized: RasterTile2D<u8> = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.overlap, TileOverlap::zero());
    }

    #[test]
    fn tile_information_new() {
        let ti = TileInformation::new(
            TileIdx::new_y_x(0, 0),
            TileSize::new_y_x(100, 100),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_geo_transform, GeoTransform::test_default());
        assert_eq!(ti.tile_position, TileIdx::new_y_x(0, 0));
        assert_eq!(ti.tile_size, TileSize::new_y_x(100, 100));
    }

    #[test]
    fn tile_information_tile_position() {
        let ti = TileInformation::new(
            TileIdx::new_y_x(0, 0),
            TileSize::new_y_x(100, 100),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.tile_position(), TileIdx::new_y_x(0, 0));
    }

    #[test]
    fn tile_information_local_upper_left() {
        let ti = TileInformation::new(
            TileIdx::new_y_x(0, 0),
            TileSize::new_y_x(100, 100),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.local_upper_left_pixel_idx(), GridIdx([0, 0]));
    }

    #[test]
    fn tile_information_local_lower_left() {
        let ti = TileInformation::new(
            TileIdx::new_y_x(0, 0),
            TileSize::new_y_x(100, 100),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.local_lower_left_pixel_idx(), GridIdx([99, 0]));
    }

    #[test]
    fn tile_information_local_upper_right() {
        let ti = TileInformation::new(
            TileIdx::new_y_x(0, 0),
            TileSize::new_y_x(100, 100),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.local_upper_right_pixel_idx(), GridIdx([0, 99]));
    }

    #[test]
    fn tile_information_local_lower_right() {
        let ti = TileInformation::new(
            TileIdx::new_y_x(0, 0),
            TileSize::new_y_x(100, 100),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.local_lower_right_pixel_idx(), GridIdx([99, 99]));
    }

    #[test]
    fn tile_information_global_upper_left_idx() {
        let ti = TileInformation::new(
            TileIdx::new_y_x(0, 0),
            TileSize::new_y_x(100, 100),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_upper_left_pixel_idx(), GridIdx([0, 0]));
    }

    #[test]
    fn tile_information_global_upper_left_idx_2_3() {
        let ti = TileInformation::new(
            TileIdx::new_y_x(-2, 3),
            TileSize::new_y_x(100, 1000),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_upper_left_pixel_idx(), GridIdx([-200, 3000]));
    }

    #[test]
    fn tile_information_global_upper_right_idx() {
        let ti = TileInformation::new(
            TileIdx::new_y_x(0, 0),
            TileSize::new_y_x(100, 100),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_upper_right_pixel_idx(), GridIdx([0, 99]));
    }

    #[test]
    fn tile_information_global_upper_right_idx_2_3() {
        let ti = TileInformation::new(
            TileIdx::new_y_x(-2, 3),
            TileSize::new_y_x(100, 1000),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_upper_right_pixel_idx(), GridIdx([-200, 3999]));
    }

    #[test]
    fn tile_information_global_lower_right_idx() {
        let ti = TileInformation::new(
            TileIdx::new_y_x(0, 0),
            TileSize::new_y_x(100, 100),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_lower_right_pixel_idx(), GridIdx([99, 99]));
    }

    #[test]
    fn tile_information_global_lower_right_idx_2_3() {
        let ti = TileInformation::new(
            TileIdx::new_y_x(-2, 3),
            TileSize::new_y_x(100, 1000),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_lower_right_pixel_idx(), GridIdx([-101, 3999]));
    }

    #[test]
    fn tile_information_global_lower_left_idx() {
        let ti = TileInformation::new(
            TileIdx::new_y_x(0, 0),
            TileSize::new_y_x(100, 100),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_lower_left_pixel_idx(), GridIdx([99, 0]));
    }

    #[test]
    fn tile_information_global_lower_left_idx_2_3() {
        let ti = TileInformation::new(
            TileIdx::new_y_x(-2, 3),
            TileSize::new_y_x(100, 1000),
            GeoTransform::test_default(),
        );
        assert_eq!(ti.global_lower_left_pixel_idx(), GridIdx([-101, 3000]));
    }

    #[test]
    fn tile_information_local_to_global_idx_0_0() {
        let ti = TileInformation::new(
            TileIdx::new_y_x(0, 0),
            TileSize::new_y_x(100, 100),
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
            TileIdx::new_y_x(-2, 3),
            TileSize::new_y_x(100, 1000),
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
            TileIdx::new_y_x(-2, 3),
            TileSize::new_y_x(100, 1000),
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
            TileIdx::new_y_x(2, 3),
            TileSize::new_y_x(10, 10),
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
