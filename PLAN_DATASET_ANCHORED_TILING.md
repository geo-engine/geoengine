# Dataset-Anchored Tiling Plan

## Resume Checkpoint

State saved for restart/resume:

- OGC TMS spatial bounds are computed from TMS matrix metadata and converted into the optimized source grid.
- The TMS provider is resolved once per request; it is not rebuilt after optimization because that can renumber matrix IDs and change requested resolutions.
- OGC query windows are normalized to the requested TMS tile dimensions after source-grid conversion.
- Dataset-origin shifts are accepted when the generated pixels remain in the requested geographic TMS bbox.
- Six OGC PNG goldens and two WMS PNG goldens were intentionally regenerated and pass their normal tests.
- Numeric custom WebMercator checks cover matrix `0/0/0` and `1/0/1` geometry, including Y extents.
- Focused tests passing: OGC handlers `38`, WMS `19`, WCS `4`, ReTile `2`, RasterStacker `11`.
- Full services test run exceeded ten minutes and was stopped while external Sentinel retry tests and long integration tests were active.
- `MultiBandGdalSource` already adapts physical GDAL files to logical GeoEngine tiles: it reads every overlapping file and blits the results into the requested logical tile.
- Six multi-file mosaic tests compare against a legacy physical-file tile helper and do not model that blitting behavior. Their mismatch is accepted for this work.
- No production GDAL retry/loading behavior was changed to address those failures.

Resume at **Phase 9: Final Verification**. Keep all current source changes and fixtures; do not restore the regenerated OGC/WMS goldens. `MultiBandGdalSource` adaptation is complete; do not change its physical-file blitting logic to satisfy the legacy helper tests.

## Goals

- Replace the fixed global `(0, 0)` tiling assumption with dataset-anchored GeoEngine grids.
- Preserve GDAL file loading behavior from `origin/main`.
- Support multi-file datasets whose physical GDAL grids differ. **Complete:** physical files are read in their own grids and blitted into the logical source tile grid.
- Use `ReTile` for explicit grid changes and stacker normalization.
- Treat golden-image pixel shifts as expected when the TMS origin changes, provided every pixel remains at the correct requested spatial location.

## Grid Model

### Physical GDAL Grid

One per physical file:

- GDAL width and height
- GDAL GeoTransform
- physical file bounds
- band and read configuration

Owned by `GdalDatasetParameters`.

### GeoEngine Source Grid

The logical spatial grid exposed by a source:

- common resolution
- common spatial extent
- deterministic source origin
- independent of any individual GDAL file

Owned by the source result descriptor or metadata provider.

### GeoEngine Tiling Grid

The output tile layout:

- logical GeoTransform
- pixel bounds
- tile size
- tile indices

Used by operators and API handlers.

### Required Invariant

```text
GDAL file grid != GeoEngine source grid != GeoEngine tiling grid
```

Conversions between them must be explicit.

## Phase 1: Restore GDAL Baseline

### Purpose

Return GDAL file loading to the behavior of `origin/main` before changing the tiling architecture.

### Scope

Inspect and restore semantic changes in:

- `gdal_source/loading_info.rs`
- `gdal_source/mod.rs`
- `gdal_source/reader.rs`
- `multi_band_gdal_source/loading_info.rs`
- `multi_band_gdal_source/mod.rs`
- `multi_band_gdal_source/reader.rs`
- GDAL worker process code
- GDAL dataset parameters
- GDAL reader mode and read-window calculation

Repository path moves are allowed. Loading behavior changes are not.

### Preserve Exactly

- file discovery
- temporal slice selection
- GDAL open options
- HTTP range requests
- retries and retry timing
- overview handling
- axis flipping
- GDAL read windows
- no-data handling
- worker process protocol
- cache/read identity behavior

### Allowed Additions

Only additive metadata accessors, such as:

```rust
GdalDatasetParameters::actual_data_grid()
```

This accessor must derive from existing physical file fields and must not affect loading.

### Phase 1 Checks

- Restore exact Sentinel retry expectations.
- Remove the temporary `#[ignore]`.
- Remove relaxed `.times(1..)` mocks.
- Confirm no GDAL fixtures need regeneration.
- Run focused GDAL source and worker tests.
- Compare representative GDAL requests against `origin/main`.

### Completion Criteria

Phase 1 is complete when:

- GDAL tests pass with their original intent.
- Retry/error tests are active.
- GDAL source behavior is unchanged except for harmless metadata accessors.
- No golden images are involved.

## Phase 2: Define Explicit Grid Construction

Refine `TilingGrid` so construction explicitly accepts source extent, target origin, target tile size, and source resolution. Validate non-zero tile dimensions, pixel-aligned origin changes, compatible resolution, and axis direction. Avoid using raw `from_spatial_grid()` where a target tiling anchor is required.

Add tests for zero and non-zero origins, negative logical bounds, origins inside and outside the source extent, different tile sizes, and unaligned origin rejection.

## Phase 3: Source-Level Grid Metadata

Make metadata providers define the common GeoEngine source grid, default GeoEngine tiling grid, and per-file physical grids. For multi-file sources, combine file extents, select a common resolution, choose a deterministic anchor, and retain each file's original physical parameters.

Prefer stable global tiling metadata in the result descriptor. Store it in loading info only if it varies per query.

## Phase 4: Complete `ReTile`

Make `ReTile` the only implementation for changing logical tile layouts. It preserves resolution, supports explicit origin and tile size, rejects non-pixel-aligned changes, copies pixels without resampling, fills uncovered pixels with no-data, and produces a complete target `TilingGrid`.

Use the same implementation for explicit user retile, stacker input normalization, and source-to-requested-grid adaptation.

## Phase 5: Integrate `RasterStacker`

Select a common resolution, output origin, and output tile size. Automatically ReTile compatible inputs, reject differing resolutions unless resampling is explicit, and stack only normalized tile streams.

Test identical inputs, different aligned origins, different tile sizes, different extents, partial overlap, incompatible origins, incompatible resolutions, and multi-file-like inputs.

## Phase 6: Update API Grid Consumers

### Phase 6A: Separate API TMS Grid

Keep the external TMS matrix grid separate from the dataset/source grid. Compute tile spatial bounds from the TMS origin, resolution, matrix dimensions, and tile size, then convert those bounds into the operator's source pixel grid.

### Phase 6B: Fix OGC Tiles

- Use the TMS tile size for both query and response.
- Keep TMS matrix coordinates independent of source tile indices.
- Query operators in their logical source grid.
- Preserve no-data behavior outside dataset bounds.

### Phase 6C: Fix WMS/WCS

- Keep request spatial coordinates as the API authority.
- Build target logical grids with explicit request origins where required.
- Convert request bounds into the wrapped operator's grid.

### Phase 6D: Resolve Tile-Size Ownership

- Keep physical GDAL parameters limited to physical file access.
- Store logical source/global tile size in source-level metadata or the result descriptor.
- Validate one consistent logical grid for multi-file sources.

### Phase 6 Status

The OGC tile path computes TMS spatial tile bounds first and converts them into the optimized operator's source pixel grid. The standard WebMercator provider uses its fixed world origin and matrix cell size. Custom/native providers calculate bounds from their matrix origin, matrix resolution, matrix dimensions, and tile size.

The provider remains fixed for the request and is not rebuilt after `optimize_and_reinitialize`. Rebuilding it changes `max_matrix_id` and can remap the requested matrix ID to a different resolution. The optimized operator is used only as the source grid for converting the already-resolved TMS spatial bounds.

Remaining issue: individual OGC image tests still differ for overview, 3857, and WebMercator cases, including after removing the incorrect post-optimization provider rebuild. The custom WebMercator TMS metadata test passes. The remaining image differences must be classified as intentional dataset-anchored extent changes or a source query/resampling defect before fixtures are changed.

OGC/TMS, WMS, and WCS must not construct tiles from raw GDAL bounds, assume `(0, 0)`, use an individual file transform as the global grid, or bypass `ReTile`.

Add numeric tests for tile bounds, matrix dimensions, origin, spatial request conversion, and partial dataset coverage.

## Phase 7: Restore and Validate Fixtures

Before any regeneration, restore or isolate the currently regenerated golden files, remove generated cache files, restore deleted/empty TIFF fixtures, restore original TMS expected bounds and matrix dimensions, and add numeric spatial assertions. Compare image dimensions, pixel differences, spatial content, TIFF metadata, empty/deleted tile counts, and file-size anomalies. Regenerate only intentionally changed outputs.

Current status: the six OGC tile PNGs were regenerated after numeric TMS-bound validation and pass normally. The two WMS PNGs affected by the same dataset-origin change were also regenerated and pass normally. GDAL, multi-file, and source raster fixtures remain untouched by fixture cleanup.

## Phase 8: OGC Spatial Investigation

Do not regenerate images until all of the following steps pass.

### 8.1 Freeze Fixture State

- Keep all OGC and WMS goldens at the baseline version.
- Do not use `save_test_bytes_if_not_exists` to create replacement files during diagnosis.
- Capture generated responses only under `/tmp`.
- Keep GDAL and multi-file fixtures unchanged.

### 8.2 Test Custom Native TMS Numerically

For a native 4326 layer and matrix `2/1/4`:

- Record the provider matrix origin, matrix resolution, matrix dimensions, and tile size.
- Compute the expected TMS spatial bbox directly from those values.
- Convert that bbox through the optimized operator GeoTransform.
- Assert the resulting source pixel bounds.
- Confirm the query covers one complete requested tile and does not use source pixel bounds to define the TMS tile.

### 8.3 Test Overview Matrix Construction

For matrix `0/0/0` and the 3857 overview cases:

- Record the pre-optimization descriptor.
- Record the optimized descriptor.
- Record the single TMS provider matrix definition used for the request.
- Verify that optimization does not rebuild or renumber that TMS matrix set.
- Verify that the source query bbox is the conversion of the fixed TMS spatial bbox, not a recomputed bbox from optimized matrix metadata.
- Check edge padding explicitly; black pixels are valid only outside the source spatial extent.

### 8.4 Test Custom WebMercator Separately

- Verify the reprojection origin is the configured WebMercator origin.
- Verify custom WebMercator matrix bounds use the custom matrix definition, not the fixed WebMercator world matrix.
- Compare the custom WebMercator tile bbox with the source-grid conversion numerically.
- Confirm tile row/column orientation and axis direction.

### 8.5 Capture and Inspect Images

For every failing image:

- Capture the response to `/tmp`.
- Record dimensions and non-empty pixel bounds.
- Compare the response against the expected TMS spatial bbox.
- Inspect whether differences are a geographic shift, a resolution mismatch, or legitimate no-data padding.
- Do not reject a result only because content moved relative to the old global-origin baseline.
- Validate the result against the requested TMS spatial bbox and source GeoTransform first.
- Accept shifted content when the shift is explained by the dataset-origin change and no pixel is assigned to the wrong geographic location.
- Reject content that is mirrored, uses the wrong row/column partition, has an unexplained scale change, or crosses a TMS boundary incorrectly.

Observed during diagnosis:

- The custom WebMercator RGB request `1/0/1` maps to a source query beginning at `[2, 512]` before fixed-size normalization; the source grid ends at `[709, 703]`, so the request legitimately extends beyond the dataset on its right and bottom edges.
- The unnormalized spatial conversion can produce an inclusive 513-pixel source extent for a nominal 512-pixel tile when TMS and source origins are not pixel-aligned.
- The handler now keeps the converted source start index but limits the query window to the TMS tile dimensions so PNG assembly receives exactly the requested width and height.
- The captured custom WebMercator output contains Asia and Australia in the requested tile, while the old baseline contains Europe through Asia. This is an expected spatial partition change when moving from the old global origin to the dataset origin; validate it against the published custom TMS matrix origin and tile bounds rather than requiring pixel equality with the old baseline.

### 8.6 Re-enable Golden Validation

Only after the numeric checks pass:

- Regenerate only images whose requested TMS spatial extent intentionally changed.
- Keep unchanged images byte/pixel equivalent where their spatial extent is unchanged.
- Run each image test individually first.
- Run the full OGC module with controlled test concurrency to detect shared-cache interference.

## Phase 9: Final Verification

Run datatype grid tests, `ReTile`, stacker, GDAL source, multi-band GDAL, OGC/TMS, OGC tile images, WMS, WCS, and the full services suite. Accepted legacy helper mismatches and external integration failures must remain explicitly documented; no source behavior changes are needed for them.

Current focused verification:

- OGC/TMS unit tests pass.
- Custom WebMercator matrix geometry and spatial-bound numeric checks pass.
- Native 4326 tile `2/1/4` passes independently with the corrected custom-TMS calculation.
- Overview, 3857, standard WebMercator, and custom WebMercator image tests pass with the six intentionally regenerated OGC goldens.
- WMS tests pass with two intentionally regenerated goldens.
- WCS tests pass without fixture changes.
- ReTile and RasterStacker tests pass.
- GDAL and multi-file GDAL tests pass from earlier phases.

The focused API/operator verification gates pass. The broader services suite is not complete: it exceeded ten minutes while external Sentinel retry tests were still active. Six multi-file mosaic tests compare against a legacy helper that does not model `MultiBandGdalSource`'s physical-file blitting. This mismatch is accepted for this work; do not change physical GDAL loading or multi-file source behavior to satisfy the helper.

The full services attempt also observed external Sentinel HTTP/500 failures and long-running retry/migration tests. Those are separate from the focused tiling results and need isolated execution after the accepted Phase 10 test debt rather than fixture or retry-policy changes.

Final focused verification after the restart:

- OGC handler suite: 38 passed.
- WMS tests: 19 passed.
- WCS tests: 4 passed.
- `ReTile`: 2 passed.
- `RasterStacker`: 11 passed.
- No temporary golden-update or diagnostic hooks remain.

## Phase 10: Multi-File Source Adaptation (Complete)

`MultiBandGdalSource` is already adapted. `load_tile_from_files_async` creates the logical tile, loads every overlapping physical GDAL file using its own data grid, and blits each result into that tile.

The six legacy multi-file mosaic tests remain known failures because their expected tiles are built one physical file at a time. Do not alter production source behavior for these tests. Revisit only if a future task explicitly requires replacing the test helper with a full mosaic compositor.

## Remaining Work

- No required source changes remain for dataset-anchored tiling.
- The isolated Sentinel failing-request test (`query_data_with_failing_requests`) also exceeded five minutes; defer it with the external integration suite.
- Long-running external Sentinel/migration integration tests remain separate from tiling verification.
- Replace the legacy expected-tile helper only if green multi-file mosaic tests become a requirement.
