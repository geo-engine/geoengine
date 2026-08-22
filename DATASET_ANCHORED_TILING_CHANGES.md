# Dataset-Anchored Tiling Changes

## Review Summary

This change replaces the implicit global `(0, 0)` raster tiling assumption with
explicit dataset, source, and output tiling grids. The goal is to keep physical
GDAL file access unchanged while allowing logical GeoEngine tiles to use a
dataset-specific origin.

The central invariant is:

```text
physical GDAL file grid != GeoEngine source grid != GeoEngine output tiling grid
```

The grids are related by explicit coordinate conversion and pixel blitting.
The implementation does not resample data merely because tile origins differ.

## Scope And Non-Goals

### In scope

- Explicit tile size and tile-index types.
- Explicit source and output tiling grids.
- Dataset-anchored origins.
- Pixel-aligned, blit-only retile operations.
- Raster stack normalization across compatible grids.
- Conversion from external OGC TMS bounds to the optimized source grid.
- WMS and WCS request-grid conversion.
- Multi-file GDAL reads into logical GeoEngine tiles.
- Numeric spatial tests and intentional golden updates.

### Not in scope

- GDAL retry policy changes.
- GDAL worker protocol changes.
- GDAL range-request changes.
- Resampling as part of `ReTile`.
- Replacing the legacy multi-file expected-tile test helper.
- Making external Sentinel integration tests reliable in this change.

## Grid Model

### Physical GDAL grid

Each physical file keeps its own:

- width and height;
- GDAL GeoTransform;
- physical pixel bounds;
- band and read parameters;
- open options, retry settings, and no-data behavior.

`GdalDatasetParameters::spatial_grid_definition()` remains the source of this
physical grid. The reader uses it when converting a logical tile request into a
physical GDAL read window.

### GeoEngine source grid

The source descriptor owns the logical dataset grid:

- common resolution;
- common spatial extent;
- deterministic origin;
- source pixel bounds;
- logical source tile size.

This grid is independent of the origin and dimensions of any one physical
file.

### GeoEngine tiling grid

`TilingGrid` combines:

- a logical GeoTransform;
- logical pixel bounds;
- a `TileSize`.

It is used by operators and API handlers when converting between pixel bounds,
tile indices, and spatial bounds.

## Data Type Changes

### `TileSize`

`TileSize` wraps `GridShape2D` so tile dimensions cannot be confused with an
arbitrary pixel-grid shape. It provides construction, axis access, conversion,
serialization, and the standard 512-pixel fallback used for metadata that does
not specify a source tile size.

### `TileIdx`

`TileIdx` wraps `GridIdx2D` to distinguish tile coordinates from pixel
coordinates. Tile-index conversion uses floor division, so negative pixel
coordinates map to the correct negative tile index.

### `TileBounds` and tile iteration

Tile-space bounds and tile iterators are kept distinct from pixel-space bounds.
`TilingStrategy` now returns tile-aware values and `TileInformationIter` emits
`TileInformation` with typed tile positions.

### `TilingSpecification`

`TilingSpecification` now contains:

- `tile_size: TileSize`;
- `origin: Coordinate2D`.

`with_zero_origin` remains available for mocks and test fixtures that
intentionally use a zero origin. Runtime source grids use the dataset or
request origin instead.

### `TileInformation`

Tile information now stores typed `TileSize` and `TileIdx` values. Existing
serialized field names are accepted through serde aliases where needed for
compatibility with persisted or exchanged data.

## GDAL Source Changes

### Single-file GDAL source

The single-file source now keeps the physical GDAL grid separate from the
logical result tiling grid. Read-window calculation still uses the physical
file GeoTransform and dimensions.

The following behavior remains unchanged:

- file discovery;
- temporal selection;
- GDAL open/config options;
- HTTP range requests;
- retry counts and timing;
- overview selection;
- axis flipping;
- no-data handling;
- worker process messages;
- cache/read identity.

### Multi-file, multi-band GDAL source

`MultiBandGdalSource` already performs the required physical-to-logical
adaptation. For each requested logical tile it:

1. selects all physical files overlapping the tile;
2. computes a read advise in each file's physical grid;
3. reads the physical window through the GDAL worker;
4. blits each returned grid into the logical tile;
5. returns the logical tile with the requested tile position and GeoTransform.

No additional source-side mosaic compositor is introduced. The physical file
grid remains authoritative for GDAL reads; the logical tile grid remains
authoritative for the returned GeoEngine tile.

## `ReTile`

`ReTile` is the explicit operator for changing logical tile layouts without
resampling. It supports:

- an optional output tile size;
- an optional output origin;
- preservation of pixel resolution;
- pixel-aligned origin validation;
- no-data filling outside the input extent;
- output metadata describing the target `TilingGrid`.

The operator queries input data for each output tile and copies pixels into the
target layout through the raster subquery machinery. It is used for explicit
retile requests and for normalization before stacking.

Invalid tile dimensions and incompatible origins produce explicit errors
instead of silently producing a malformed grid.

## `RasterStacker`

`RasterStacker` now normalizes compatible inputs before combining them. It
selects a common logical grid and uses `ReTile` when inputs differ in aligned
origin, tile size, or extent.

The stacker rejects incompatible resolutions and non-pixel-aligned origins.
Resampling remains an explicit separate operation rather than an implicit side
effect of stacking.

The public/API model exposes an optional output origin so callers can request a
deterministic stack grid. Existing workflows retain their default behavior by
leaving the option unset.

## OGC TMS Changes

The OGC tile handler now keeps the external TMS grid separate from the source
grid.

For each request it:

1. resolves the TMS provider once;
2. validates the matrix, row, and column against the TMS matrix dimensions;
3. obtains the requested TMS tile spatial bounds from matrix metadata;
4. converts those spatial bounds into source pixel bounds;
5. limits the query window to the requested TMS tile dimensions;
6. assembles a response using the TMS tile width and height.

The provider is not rebuilt after operator optimization. Rebuilding it could
renumber matrix IDs or select a different resolution.

The handler does not derive the public TMS tile bounds from raw GDAL bounds or
from an individual physical file transform.

## WMS And WCS Changes

WMS and WCS retain request coordinates as the API authority. They construct a
target logical grid for the request and convert that grid into the wrapped
operator's source grid.

This prevents request results from depending on the physical origin of one
GDAL file and keeps no-data padding at request boundaries explicit.

## Metadata And API Consumers

The source result descriptor now carries logical tile-size information rather
than treating physical GDAL parameters as the owner of global tiling.

Metadata providers and API adapters were updated to construct
`SpatialGridDescriptor` and `TilingGrid` values explicitly. This includes:

- dataset creation from workflows;
- dataset and layer handlers;
- OGC, WMS, and WCS handlers;
- raster stacker API models;
- processing graph serialization/deserialization;
- STAC and external provider metadata;
- CLI and example source construction;
- mock raster sources and test utilities.

## Test And Fixture Changes

### Numeric tests

Tests cover:

- zero and non-zero origins;
- negative tile indices;
- tile-to-pixel and pixel-to-tile conversion;
- zero tile-size rejection;
- unaligned-origin rejection;
- explicit retile origins;
- stacker alignment and mismatch cases;
- custom TMS matrix origin and resolution;
- TMS Y extents and partial source coverage.

### Image fixtures

Six OGC tile PNGs were intentionally regenerated:

- `natural_earth_rgb_1_0_1.png`;
- `ndvi_0_0_0.png`;
- `ndvi_2_1_4.png`;
- `ndvi_3857_0_0_0.png`;
- `ndvi_native_3857_0_0_0.png`;
- `ndvi_webmercator_0_0_0.png`.

Two WMS PNGs were intentionally regenerated:

- `get_map_ndvi.png`;
- `raster_small.png`.

The changed pixels represent dataset-anchored spatial partitioning. Numeric
TMS checks establish that the content remains inside the requested geographic
tile bounds before the goldens are accepted.

The MODIS projected raster fixture and GDAL/multi-file raster fixtures were
left unchanged.

## Review Fixes

Applied during post-implementation review:

- **B1**: `ReTileAccu::into_tile` now returns `Err(InvalidOperatorSpec)` instead of
  `expect` when the accumulator has no tile data.
- **D1+D2**: Removed unused `_tiling_grid` parameter from `tile_spatial_bounds`
  trait method and both implementations. Removed dead `source_tiling_grid`
  computation in the OGC tile handler.
- **D3**: Removed unused `_tiling_specification` parameter from
  `raster_stream_to_multiband_geotiff_bytes`.
- **D4**: Restored `#[allow(dead_code)]` on `tile_grid_bbox` (test-only usage).
- **D6**: Removed 5 no-op `#[serde(alias)]` annotations from `tiling.rs`.
- **D7**: Removed stale TODOs; renamed `grid_bounds` → `pixel_bounds` in
  `tile_idx_iterator_from_grid_bounds` and `tile_information_iterator_from_pixel_bounds`.
- **E1+E2**: Replaced 10 duplicated match arms in `raster_stacker.rs` with a
  `stacker_arm!` macro (~90 lines → ~25 lines).
- **E4+B5**: Replaced silent `unwrap_or(u32::MAX)` with `expect("tile size must
  fit in u32")` in the OGC tile handler.

## Verification

The following focused suites pass:

| Area | Result |
| --- | ---: |
| `geoengine-operators` (full, skip external) | 575 passed |
| `geoengine-services` (full, skip external) | 461 passed |
| `geoengine-datatypes` (full) | 739 passed |
| Workspace check, all targets, zero warnings | passed |

## Accepted Test Debt And External Limits

Six multi-file mosaic tests compare source output against a legacy helper that
treats each physical file as one complete logical tile. That helper does not
model `MultiBandGdalSource`'s multi-file blitting, so those failures are
accepted for this change and are not a reason to alter production GDAL source
behavior.

The full services suite exceeded the available runtime while external
Sentinel retry and migration tests were active. The isolated
`query_data_with_failing_requests` Sentinel test also exceeded five minutes.
These are external integration-test limitations, separate from the focused
dataset-anchored tiling verification.

## Files And Subsystems

The implementation is distributed across these existing subsystem areas:

- `datatypes/src/raster/`: grid, tile, raster descriptor, and conversion types;
- `operators/src/source/gdal_source/`: single-file source metadata and reads;
- `operators/src/source/multi_band_gdal_source/`: multi-file tile assembly;
- `operators/src/source/gdal_worker_process/`: physical GDAL parameters, read
  windows, and worker tests;
- `operators/src/processing/retile/`: explicit grid conversion operator;
- `operators/src/processing/raster_stacker.rs`: normalized multi-input stacks;
- `operators/src/adapters/`: query and stacker adapters;
- `operators/src/engine/`: result descriptors and execution tiling metadata;
- `services/src/api/handlers/ogc/`: TMS metadata and tile requests;
- `services/src/api/handlers/wms.rs`: WMS request-grid conversion;
- `services/src/api/handlers/wcs.rs`: WCS request-grid conversion;
- `services/src/api/model/`: public processing graph and operator models;
- `services/src/datasets/`: workflow, provider, and metadata consumers;
- `test_data/ogc/tiles/` and `test_data/wms/`: intentional image goldens;
- `PLAN_DATASET_ANCHORED_TILING.md`: execution plan and checkpoint.

No commit is included in this worktree change.
