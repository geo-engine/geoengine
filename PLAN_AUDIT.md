# PLAN_AUDIT — Branch `feature/retile-and-dataset-origins` (vs `ge/main`)

Audit of the branch diff for **crud / readability** and **GIS-correctness** violations in
the Rust tiling/grid/origin code. Scope: GIS-critical clusters (Rust operators + services
conversions). Deliverable: this report + the applied fixes.

- **Diff base:** `ge/main` (remote `ge` = `https://github.com/geo-engine/geoengine.git`), merge-base `753c65196`.
- **Axis convention (confirmed):** `GridIdx2D.0 = [y, x]` → `.y()` = index 0, `.x()` = index 1.
  `TileSize(y_size, x_size)` → `.axis_size_y()` = index 0, `.axis_size_x()` = index 1.
  `TilingStrategy.raster_spatial_query_to_tiling_grid_box` (tiling.rs:161) is the canonical
  `min_pixel_idx = [ty*ysize, tx*xsize]` mapping; `TileIdx::from_global_pixel_idx` mirrors it.

---

## Findings & status

### 1. P1 (GIS-correctness bug) — axis swap in `CustomNativeTMS::tile_grid_bbox` — **FIXED**
`tms_spec.rs:369-371`. The max pixel index was computed with `GridIdx2D::new([x_size, y_size])`
(i.e. `[y=x_size, x=y_size]`) — the axes were swapped. Harmless for the square 512×512 native
tiles, but wrong for non-square tiles (a native-TMS tile of 4×8 produced a 7×3 extent instead of
3×7).
- **Fix:** use `GridIdx2D::new_y_x(tile_size.axis_size_y(), tile_size.axis_size_x())`.
- **Extracted** the body into a free, pure fn `tile_grid_bbox(tiling_grid, matrix, row, col)`
  so it is unit-testable without a DB; the trait method is now a one-line delegate.
- **Regression test added:** `tile_grid_bbox_uses_axis_correct_tile_size_for_non_square_tiles`
  (non-square 4×8 tile; asserts per-axis extent == `tile_size - 1`, i.e. y=3, x=7). Fails on the
  old swapped code; passes now.

### 2. P2 (redundancy) — `calculate_tiles_for_zoom_levels` duplicated the per-zoom body — **FIXED**
`tms_spec.rs` (~40 lines). The function inlined the same math as `calculate_tiles_for_zoom_level`.
- **Fix:** `calculate_tiles_for_zoom_levels` now does
  `(0..=max_zoom_level).map(|z| calculate_tiles_for_zoom_level(...)).collect()`. Behavior-identical.

### 3. P3 (wrapper noise) — `CustomWebMercatorTMS` — **RECLASSIFIED: no change**
Initially flagged as a wrapper duplicating `CustomNativeTMS`. Re-audited: it is a *thin newtype*
(`TileMatrixSetId + TilingSpecification`) whose methods are trivially delegating (1-line bodies, no
logic). That is the idiomatic Rust pattern. Collapsing into a shared struct would add a
`Native | WebMercator` enum and make the two `TILE_MATRIX_SET_ID` constants less obvious — worse
readability for no correctness gain. **Left as-is.**

### 4. P4 (raw accessors / naming) — **FIXED**
- `tms_spec.rs` `calculate_number_of_zoom_levels`: renamed the misleading param `tile_size`
  (a `TilingSpecification`) to `tiling_specification`; replaced raw `.tile_size.0.x()/.y()` with
  `.tile_size.axis_size_x()/.axis_size_y()`; renamed locals to `grid_size_x/grid_size_y`.
- `operators/.../neighborhood_aggregate/tile_sub_query.rs:274-275`: replaced
  `tiling_strategy.tile_size.0.y()/.x()` with `.axis_size_y()/.axis_size_x()` (same values;
  removes the raw tuple access).

### 5. P5 (param hygiene) — `CustomNativeTMS::calculate_tiles_for_zoom_levels` `tile_matrix` — **RECLASSIFIED: no change**
The `tile_matrix` param is required by the trait signature and is legitimately used in the
`OgcApiError::TileMatrixNotFound` message. Not a redundant param. **Left as-is.**

### 6. Pre-existing (NOT introduced by this branch) — STAC `[y,x]` swap — **noted, out of scope**
`services/src/datasets/external/stac/loading_info.rs:496-497`:
```
tile_grid: [raster_width - 1, raster_height - 1]
```
is written as `[y, x]`, so for non-square STAC rasters the width/height are transposed (the
`-1` is correct; the order is not). This line is **unchanged from `ge/main`** — the branch only
added `tile_size: None` next to it and updated a test. Correct fix (separate task):
`[raster_height - 1, raster_width - 1]`. Tracked here so it isn't lost.

---

## Verified correct (no change needed)
- **`datatypes/src/raster/tiling.rs`** — all index math is `[y,x]`-consistent; `TileSize`/`TileIdx`
  accessors, `raster_spatial_query_to_tiling_grid_box`, `pixel_idx_to_tile_idx`,
  `tile_idx_to_global_pixel_idx`, `calculate_max_pixel_idx`, `tile_count_for_bounding_box` all correct.
- **`grid_blit_valid_only`** (`grid_blit.rs:265`) — aligns source/destination by global-pixel-bounds
  intersection (no manual `row*height+col`); skips NoData via validity mask; NoData fill on
  `!dest_valid`; `[y,x]` consistent. Correct.
- **`tiles.rs:440-478`** caller of `grid_blit_valid_only` — `dest_start_index`/`source_start_index`
  from global-bounds corners; `source_validity`/`source_data` indexed `[y][x]`. Correct.
- **`multi_band_gdal_source/**`** — uses the fixed `grid_blit_valid_only` + `tile_idx_to_global_pixel_idx`.
- **`processing.rs`** `TryFrom<ResultDescriptor>` — `grid_bounds()`/`tiling_grid()`/`tile_size` wired
  correctly; `raster_tiling_grid`/`raster_tile_size`/`raster_grid_bounds` helpers consistent.
- **Reprojection / downsample / interpolation / rasterization / temporal_aggregation /
  raster_vector_join / meteosat** — all use the type-safe `grid_to_spatial_bounds` /
  `tiling_strategy()` accessors; no raw tuple math. Clean.
- **`tiles.rs:606`** `TileGridBounds` — pure coordinate-transform wrapper, no index math. Clean.

---

## Fix sequence applied
1. `tms_spec.rs` — axis-swap fix + free-fn extraction + non-square regression test (P1).
2. `tms_spec.rs` — dedupe `calculate_tiles_for_zoom_levels` (P2).
3. `tms_spec.rs` — `TileSize` accessors + param rename in `calculate_number_of_zoom_levels` (P4).
4. `operators/.../neighborhood_aggregate/tile_sub_query.rs` — `TileSize` accessors (P4).
5. Reclassified #3 and #5 as no-change (documented above).

## Verification
- `cargo check -p geoengine-services -p geoengine-operators` — clean, **0 warnings**.
- New regression test `tile_grid_bbox_uses_axis_correct_tile_size_for_non_square_tiles` — **pass**.
- Full `tms_spec::tests` module (14 tests; Postgres-backed OGC TMS: tiles per zoom, pixel bounds for
  4326/3857/native/webmercator) — **14/14 pass** (exercises the dedup + extraction + axis fix).
- `neighborhood` operators tests (17) — **17/17 pass** (exercises the accessor refactor).

### Open / deferred
- **STAC `[y,x]` swap** at `loading_info.rs:496-497` (pre-existing) — fix in a follow-up:
  change to `[raster_height - 1, raster_width - 1]` and add a non-square STAC raster test.
