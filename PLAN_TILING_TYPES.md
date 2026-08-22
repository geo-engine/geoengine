# Plan: Enforce Tiling-Specific Types + TilingStrategy tile→spatial

**Branch:** `feature/retile-and-dataset-origins`
**Date:** 2026-08-20

## Problem
`TileSize`, `TileIdx`, and `TileBounds` newtypes exist but are inconsistently applied. Raw `GridShape2D`, `[usize; 2]`, and `GridIdx2D` are used in tiling contexts where the semantic wrapper would prevent bugs. Additionally, there is no direct `TileIdx → Coordinate2D` path — the conversion must be manually chained through `TilingStrategy::tile_idx_to_global_pixel_idx` then `GeoTransform`.

## Principles
- **GIS correctness**: tile coordinates and pixel coordinates are different coordinate spaces. The type system must enforce this boundary.
- **Readable code**: a function that takes `TileIdx` clearly operates in tile space; a function that takes `GridIdx2D` operates in pixel space.
- **No unnecessary abstraction**: newtypes compose *around* `GeoTransform` without modifying it.

---

## Phase 1: `TileMatrixSetProvider::tile_size()` trait → `TileSize` ✅

**File:** `services/src/api/handlers/ogc/tms_spec.rs`

Changed trait method `tile_size()` return type from `GridShape2D` → `TileSize`. Updated all 4 implementations (CustomNativeTMS, CustomWebMercatorTMS, WebMercatorQuadTMS, TypedTileMatrixSetProvider). Updated `tiles.rs` caller (`.x()` → `.axis_size_x()`). Added `TileSize` import to `tms_spec.rs`.

---

## Phase 2: `TilingStrategy::tile_idx_to_spatial` + `spatial_to_tile_idx` ✅

**File:** `datatypes/src/raster/tiling.rs`

Added `tile_idx_to_spatial(TileIdx) → Coordinate2D` and `spatial_to_tile_idx(Coordinate2D) → TileIdx` methods to `TilingStrategy`.

---

## Phase 3: `ReTileParams.tile_size` → `TileSize` ✅

**File:** `operators/src/processing/retile/mod.rs` + `services/src/api/model/processing_graphs/processing.rs`

Changed `tile_size` from `Option<[usize; 2]>` → `Option<TileSize>`. Updated `optimize()` and `_initialize()` methods. Updated services `TryFrom` conversion to use `TileSize::from`. Updated test assertion. Wire format break (user-approved).

---

## Phase 4: `MockRasterSourceError` → `TileSize` ✅

**File:** `operators/src/mock/mock_raster_source.rs`

Renamed error variant fields (`tiling_specification_yx` → `tiling_specification`, `tile_size_yx` → `tile_size`). Changed types `GridShape2D` → `TileSize`. Updated helper function return type. Updated both call sites. Moved unused `GridShape2D`/`GridSize` imports to test module.

---

## Phase 5: Test code — ~50 occurrences ✅

Replaced `let tile_size = GridShape2D::new_2d(y, x)` → `TileSize::new(y, x)` across 13 files. Removed `TileSize(tile_size)` wrapping. Added `.0` access for inner `GridShape2D` where needed (`bounding_box()`, `Grid::new()`). Added `.into()` where APIs expect `GridShape2D`.

### Files changed
| File | Count |
|------|-------|
| `plot/box_plot.rs` | 7 |
| `plot/statistics.rs` | 5 |
| `plot/histogram.rs` | 2 |
| `plot/class_histogram.rs` | 2 |
| `processing/downsample/mod.rs` | 2 |
| `processing/retile/mod.rs` | 1 |
| `processing/raster_scaling.rs` | 2 |
| `processing/raster_type_conversion.rs` | 1 |
| `processing/band_filter.rs` | 2 |
| `processing/time_shift.rs` | 2 |
| `processing/reprojection.rs` | 1 |
| `processing/temporal_raster_aggregation/...` | 19 |
| `adapters/raster_subquery/raster_subquery_reprojection.rs` | 1 |

---

## All Phases Complete

### Verification results
```
cargo test -p geoengine-datatypes --lib:    673 passed ✅
cargo test -p geoengine-operators --lib:    575 passed ✅
cargo test -p geoengine-services --lib:     461 passed ✅
```

**Total: 1709 tests passed, 0 failed.**
