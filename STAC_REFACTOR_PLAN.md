# STAC cleanup review and implementation plan

Review the current uncommitted backend changes and preserve the projection-aware
grid, Moka request sharing, cancellation survival, retries, pagination, regular
time axes, extraction, API schema, and database behavior.

1. Inline `cache/tests.rs` into `cache.rs` and remove the empty `cache/` directory.
   Move `grid_request_tests.rs` HTTP/loading tests into `loading_info.rs`; move
   its provider configuration/database test into `mod.rs`. Keep tests beside
   production logic. Introduce a `#[cfg(test)]` helper module only if fixtures
   are actually shared by multiple modules.
2. Replace the STAC positional constructor chain with a definition-based
   constructor using the existing `StacDataProviderDefinition`. Update callers
   and remove all STAC `clippy::too_many_arguments` allowances. Keep validation
   and asynchronous authentication initialization explicit.
3. Share regular-step validation/traversal without eagerly allocating steps on
   tile queries. Remove the redundant `fill_regular_steps` wrapper and avoid
   unused time-step accumulation in the new cell search. Simplify the new tile
   comparator/deduplication with named helpers and preserve its identity rules.
4. Add Rust documentation to complex new helpers only. Describe grid rounding,
   edge/point membership, lazy traversal, full-cell searches, cache admission,
   shared errors, and cancellation where appropriate. Correct stale WGS84 grid
   comments. Narrow visibility exposed solely for separate test modules. Add
   empty lines between validation, setup, traversal, and result assembly.
5. Review the implementation against the pre-refactor snapshot. Run all STAC
   tests, relevant migration checks, `cargo fmt --all -- --check`,
   `git diff --check`, and services library/test Clippy with `-D warnings`.
   Coordinate Cargo builds with `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2`.

GPT-6 Luna performs the actual refactoring; the root agent reviews and verifies.
Keep unrelated working-tree changes and generated API/client artifacts intact.

## Completed

GPT-6 Luna implemented the cleanup; root reviewed the result and requested the
final import, allocation, documentation, and spacing fixes. Cached cell results
now contain tile files only; the provider-defined regular time axis is assembled
once per query. All 93 original STAC tests remain present and pass.

Validation passed:

- All 93 STAC tests.
- Full migration chain and migration/current-schema equivalence checks.
- Services library/test Clippy with `-D warnings`.
- Workspace formatting and whitespace checks.

Database checks used an isolated temporary PostgreSQL instance with the required
extensions; that instance has been stopped. Existing tracked changes outside
the STAC modules remain exactly as they were at the start of this review.

## Follow-up: redundant wrappers and comparison traits

GPT-6 Luna removed the single-field `StacQueryResult` wrapper and the separate
normalization, subset, and comparison helpers. Cached results now use
`Arc<Vec<TileFile>>`; each query filters its own tile vector with `retain` before
sorting and deduplicating. A private borrowed `StacTileIdentity` derives `Eq`
and `Ord`, using the same fields for both operations and treating signed zero
consistently. Shared geometry and time-interval trait semantics stay unchanged.

Regular time-step callers now construct the iterator directly, and WGS84 bounds
use the existing bounding-box intersection operation. Cache cancellation,
shared errors, and admission behavior remain covered by the existing tests.

Validation passed: all 95 STAC tests (the original 93 plus two identity
regressions), services library/test Clippy with `-D warnings`, workspace
formatting, and whitespace checks.

## Completed: shared index and traversal types

The current follow-up uses `GridIdx2D` for cell/cache indices,
`GridBoundingBox2D` for grid dimensions and query ranges, and `GridIdx2DIter`
for traversal. All 96 STAC tests, services library/test Clippy with
`-D warnings`, formatting, and whitespace checks pass.

## Proposed: model search cells as a coarse raster

This section is a plan; the additional raster geometry refactor is not yet
implemented.

### Reviewed raster code

- [RasterTile2D and BaseTile](geoengine/datatypes/src/raster/raster_tile.rs):
  geometry comes from `tile_information()`. The tile also carries pixel data,
  time, band, properties, and cache hints.
- [TileInformation and TilingStrategy](geoengine/datatypes/src/raster/tiling.rs):
  tile footprints are derived from a global `GeoTransform` and pixel bounds.
- [SpatialGridDefinition](geoengine/datatypes/src/raster/grid_spatial.rs):
  represents a raster's transform and inclusive grid bounds without pixel data.
- [GeoTransform](geoengine/datatypes/src/raster/geo_transform.rs): provides
  coordinate/index conversion and `grid_to_spatial_bounds`.
- [GridBoundingBox2D and GridIdx2DIter](geoengine/datatypes/src/raster/grid_bounds.rs):
  represent and traverse a selected integer grid range.
- The multi-band GDAL source uses these geometry types to plan tiles before
  loading their raster data.

### Recommended representation

Keep the persisted `StacGrid` configuration. Represent its derived geometry as:

```rust
struct ProjectedStacGrid {
    extent: BoundingBox2D,
    raster: SpatialGridDefinition,
}
```

One coarse raster pixel is one square STAC search cell. Use
`GeoTransform::new(extent.upper_left(), side, -side)` and standard raster
`[row, column]` indices. The native area-of-use extent stays separate because
the raster's terminal cells can extend beyond it.

Use `GeoTransform::grid_to_spatial_bounds` on a one-cell `GridBoundingBox2D`
to obtain each full cell footprint, then `BoundingBox2D::intersection` to
clip the HTTP footprint to the native extent. This reuses the geometry behind
`RasterTile2D` without constructing data tiles. A one-pixel `TilingStrategy`
and `TileInformationIter` are unnecessary if the direct grid conversion is
shorter.

### Implementation steps

1. Characterize raster conversions with the STAC edge cases before replacing
   selection logic. The current inverse conversion uses quotient/floor, and
   `lower_right_pixel_idx` subtracts a fixed one-millionth of a pixel.
   Neither is an exact replacement for STAC's canonical-edge comparisons.
   For example, with world extent and target 513, the first generated x edge
   converts back to `0.9999999999999993`, which floors to cell 0. With unit
   pixels, an upper bound of `1.0000005` intersects cell 1 but the inward
   epsilon excludes it.
2. Build the `SpatialGridDefinition` with the existing target-area calculation
   and checked finite/range/precision validation. Count rows against the actual
   negative-y transform boundaries; reversing the old positive-y calculation
   can change floating-point rounding. Keep only STAC-specific configuration
   and extent validation here.
3. Add a small precision-aware coordinate conversion on `GeoTransform` if the
   characterization tests confirm it is necessary. Reuse the existing inverse
   index as a candidate and the forward-generated edges to correct rounding.
   Check finite inputs and index arithmetic. Leave existing raster conversion
   methods unchanged initially. Do not copy STAC's binary search into another
   module or introduce configurable edge-policy machinery.
4. Reduce query selection to native-extent intersection, corrected raster
   coordinate conversion, and shared grid-bound intersection. Preserve the
   positive-area rule that a query ending exactly on an edge excludes the
   following cell; keep point and outer-endpoint handling explicit. Avoid the
   fixed inward epsilon, which can discard a real narrow intersection.
5. Remove `StacGridCell` and `StacGridCells` if callers can use the selected
   `GridBoundingBox2D`/`GridIdx2DIter` directly and derive footprints through the
   shared transform. Remove the separate `side` field and `boundary` arithmetic
   once the transform is the sole source of cell geometry. Keep no iterator
   adapter that merely forwards shared functionality. Fix the shared iterator's
   size hint to describe remaining indices, with checked arithmetic and an
   unknown upper bound when the count cannot be represented; currently it
   reports the original full range even after consumption.
6. Validate shared conversion behavior and the STAC request/cache integration.
   Cover irrational sides, exact edges, queries immediately to either side of
   an edge, horizontal and vertical points, maximum endpoints, empty queries,
   terminal padding, projected CRSs, and huge-grid lazy traversal. Run focused
   datatype/operator tests when a shared helper changes, all STAC tests,
   Clippy, formatting, and whitespace checks.

### Geometry convention decision

Recommend adopting the normal raster upper-left origin and southward rows.
This changes internal cell indices, traversal order, and terminal padding
from north to south. On non-integral extents it also changes search-cell
footprints, so it must not be treated as an index-only rename. A point on an
internal horizontal edge naturally belongs to the southern cell under raster
conventions, whereas the current STAC convention selects the northern cell.
Document and test this deliberate convention change while retaining point
query support and complete native-extent coverage. Cache keys are internal
and caches are recreated with the provider; no persisted configuration or
API type needs to change.

The acceptance criterion is less total custom code and one source of raster
geometry. A small, tested boundary adapter is preferable to silently losing
search coverage or forcing unrelated raster data/tiling abstractions.
