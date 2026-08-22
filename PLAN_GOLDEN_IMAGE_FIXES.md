# Plan — Fix golden images / GIS-correctness for `feature/retile-and-dataset-origins`

Branch: `feature/retile-and-dataset-origins`, WIP `ee9129e57` on base `753c65196`.
Scope: **whole branch** (242 files: ~100 `.rs`, 108 TIFs, 9 PNGs, `openapi.json`, 2 docs).

## Directive (user)
- Primary: **adapt the tests, not the images.** If no other tiling area is used, images must NOT change.
- Exception: if the tiling grid **origin moved by a fraction of a tile size**, tile boundaries genuinely change → regeneration acceptable.
- Review lenses: (a) GIS correctness, (b) code readability.

## Root cause (CONFIRMED)

The branch's feature = **dataset-anchored tiling**: the tiling grid origin is now the
**source origin** (no snapping) instead of snapped to the tiling origin reference.

- **OLD** `TilingSpatialGridDefinition::tiling_spatial_grid_definition()`
  (`grid_spatial.rs:304`): `element_grid_definition.with_moved_origin_to_nearest_grid_edge(tiling_specification.tiling_origin_reference())`.
  For `with_zero_origin`, reference = (0,0). Source origin (-180,90), pixel (0.1,-0.1)
  → nearest grid edge to (0,0) = **(0,0)**.
- **NEW** `TilingGrid::from_spatial_grid` (`tiling.rs:467`): uses
  `grid.geo_transform().origin_coordinate` directly = **(-180, 90)**. No snapping.
- `result_descriptor.tiling_grid_definition()` (now no-param, `result_descriptor.rs:195`)
  returns `TilingGrid::from_spatial_grid(self.spatial_grid, self.tile_size)`.

**Origin shift** = (0,0) − (-180,90) = (180, −90) spatial = **(1800, 900) pixels** —
an **exact integer** pixel shift at 0.1 px/px.

Consequence rules:
- **Pixel-space query** (WMS, png_from_stream): shift query by exact (+1800, +900) px
  → same spatial area → image IDENTICAL → **adapt query, keep image**.
- **Tile-index query** (OGC tiles): tile boundaries moved by (1800/512 × 900/512) =
  **3.515 × 1.758 tiles = FRACTION** → tile covers different area → **regenerate** (exception).

## Test-data classification

| Test data | Query type | Origin shift | Action |
|-----------|-----------|--------------|--------|
| `test_data/ogc/tiles/*.png` (6) | tile index | fraction (3.5×1.8) | **Regenerate** (exception) + fix f64 bug |
| `test_data/wms/get_map_ndvi.png` | pixel | — | **Regenerate** (user approved) |
| `test_data/wms/raster_small.png` | pixel | exact px | **Adapt query, keep image** |
| `test_data/raster/png/png_from_stream.png` | pixel | exact px | **Adapt query, keep image** |
| `test_data/raster/multi_tile/results/*.tif` (108) | full-extent → tiles | TBD whole/fraction | Verify consistency; regenerate if correct source |

## Code fixes

### 1. OGC tiles f64 round-trip bug — `services/src/api/handlers/ogc/tiles.rs` (~line 450)
- NEW (buggy): `tms_spec.tile_spatial_bounds(...)` → `source_grid.geo_transform().spatial_to_grid_bounds(...)` (f64) + add `(tile_width-1, tile_height-1)`. Imprecise.
- OLD (correct): `tms_spec.tile_grid_bbox(&result_descriptor.tiling_grid_definition(tiling_specification), matrix, row, col)?` — exact integer via `tile_idx_to_global_pixel_idx`.
- **Fix**: restore `tile_grid_bbox` call, passing `&result_descriptor.tiling_grid_definition()` (new no-param, source-anchored grid). `tile_grid_bbox` still exists in `tms_spec.rs` (~line 388), exact-integer, takes `&TilingGrid`.

### 2. `png_from_stream` pixel query — `operators/src/util/raster_stream_to_png.rs:391`
- OLD: `GridBoundingBox2D::new([-800, -100], [-199, 499])` (min=[x,y], max=[x,y]).
- NEW (shift +1800 x, +900 y): `GridBoundingBox2D::new([1000, 800], [1601, 1399])`.
- **Verify by running the test** (image must be byte-identical to existing golden).

### 3. WMS pixel query — `services/src/api/handlers/wms.rs` (`png_from_stream_non_full`, ~line 747 tiling spec 600×600)
- Shift the hardcoded pixel query by (+1800, +900). Read exact OLD bounds, apply shift, verify image identical.

### 4. multi_tile TIFs (108) — `operators/src/source/multi_band_gdal_source/mod.rs` (~line 1061)
- Test queries full extent, compares to golden TIFs loaded with the SAME source-anchored
  `tiling_grid_definition()`. Internally consistent.
- **Verify**: run tests (pass = consistent). Spot-check 1–2 TIFs for geographic
  correctness (tile x0_y0 = top-left quarter). If source correct → expected regeneration.
- Determine whole-vs-fraction for the multi_tile dataset grid to label expected vs regression.

## Code readability review (TODO during execution)
- `tms_spec.rs`: `tile_size_in_pixels` → `tile_size` rename consistency; `#[allow(dead_code)]` on `tile_grid_bbox` (remove if now called).
- `tiles.rs`: ensure no leftover f64 helper (`tile_spatial_bounds`) if unused.
- Confirm no dead code / duplicated origin math.

## Verification
1. `cargo build` (workspace root `geoengine/`), zero warnings.
2. `cargo test -p geoengine-operators --lib -- --skip external::` (575 pass).
3. `cargo test -p geoengine-services -- --skip external::` (461 pass).
4. `cargo test -p geoengine-datatypes` (739 pass).
5. For adapted pixel queries: confirm golden PNG **unchanged** (git diff empty on image).
6. For regenerated images: confirm they reflect source-anchored grid (spot-check).
7. Postgres on port 5432. External slow tests (`datasets::external::*`) hang ~15 min — always `--skip external::`.

## Work state
- Done: all source changes (Phases 1–8, 10), ReTile operator, review fixes B1/D1–D7/E1/E2/E4/B5.
- Active: this plan. Next: apply code fixes 1–3, regenerate/verify images per table, readability pass, full test run.
- Blocked: none (plan mode exited).

## Relevant files
- `geoengine/services/src/api/handlers/ogc/tiles.rs` — OGC handler + f64 bug + 6 tests (lines 796–1037).
- `geoengine/services/src/api/handlers/ogc/tms_spec.rs` — `tile_grid_bbox` (~388), `tile_spatial_bounds` (~704), CustomNativeTMS (320), `calculate_tiles_for_zoom_level` (921).
- `geoengine/services/src/api/handlers/wms.rs` — WMS handler, tiling spec 600×600 (747).
- `geoengine/operators/src/util/raster_stream_to_png.rs` — `png_from_stream` (379), query (391).
- `geoengine/operators/src/source/multi_band_gdal_source/mod.rs` — multi_tile tests (1040–1110).
- `geoengine/datatypes/src/raster/tiling.rs` — `TilingGrid::from_spatial_grid` (467), `TilingSpecification` (143), TestDefault = `with_zero_origin(512)` (186).
- `geoengine/datatypes/src/raster/grid_spatial.rs` — OLD `TilingSpatialGridDefinition` (288, base only).
- `geoengine/operators/src/engine/result_descriptor.rs` — `tiling_grid_definition()` (195, no-param).
- `geoengine/services/src/util/tests.rs` — 4326 layer (316–319): origin (-180,90), pixel (0.1,-0.1), TileSize::default_512().
- `geoengine/services/src/config.rs` — TilingSpecification config (171–183).
- `geoengine/test_data/ogc/tiles/`, `test_data/wms/`, `test_data/raster/png/`, `test_data/raster/multi_tile/results/`.
