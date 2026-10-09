# Shared raster grid behavior adopted by STAC

STAC now uses the shared `GeoTransform` conversions and one-pixel
`TileInformationIter` for query membership and cell geometry. The following
existing behaviors are therefore also STAC behavior; corresponding precision
regressions in `grid.rs` remain ignored until the shared raster APIs are
addressed.

1. `GeoTransform::coordinate_to_grid_idx_2d` computes
   `floor((coordinate - origin) / pixel_size)` directly
   ([`geo_transform.rs`](geoengine/datatypes/src/raster/geo_transform.rs#L121-L140)).
   For the geographic extent `(-180, -90) .. (180, 90)` and 513 target cells,
   the generated first x edge is `-168.76097026101968`; its inverse quotient
   is `0.9999999999999993`, so a point at that edge can select column 0 rather
   than column 1.

2. `GeoTransform::lower_right_pixel_idx` moves the lower-right coordinate
   inward by `1e-6` of a pixel before converting it
   ([`geo_transform.rs`](geoengine/datatypes/src/raster/geo_transform.rs#L170-L191)).
   With unit pixels, a query from x `0` to `1.0000005` overlaps column 1, but
   the inward shift moves its upper bound to `0.9999995`; conversion selects
   only column 0. Tiny overlaps near an upper query edge can therefore be
   omitted.

3. `GridIdx2DIter::size_hint` always returns the element count of its original
   bounds ([`grid_bounds.rs`](geoengine/datatypes/src/raster/grid_bounds.rs#L522-L545)),
   even after iteration has advanced. `TileInformationIter` forwards this hint
   unchanged ([`tiling.rs`](geoengine/datatypes/src/raster/tiling.rs#L294-L309)).
   STAC's standard iterator composition inherits that size hint.

4. `for_extent` trusts the shared conversion after validating finite positive
   inputs and representable indices. It does not reject a cell size smaller
   than the coordinate spacing at a large origin. For example, the extent
   `(1e16, 0) .. (1e16 + 2, 1)` with the default target produces cell steps
   smaller than the 2-unit spacing of representable x coordinates there, so
   neighboring raster edges can collapse to the same coordinate. The ignored
   `it_rejects_extent_beyond_shared_coordinate_precision` test records the
   earlier rejection expectation until shared precision handling is addressed.

The shared grid uses half-open spatial bounds for positive-area queries. A
point exactly on the grid's outer right or bottom edge can map to an index
outside the grid and return no cells. Internal point and edge membership also
follows the shared coordinate-to-index conversion.

`StacGridCell` and the `StacGridCells` type alias remain because existing
`loading_info` callers consume a combined cell index and bounding box and clone
the iterator for each time step. Cell geometry and traversal come directly
from the shared one-pixel tiling APIs; no STAC-specific edge correction is
applied.
