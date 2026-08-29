# Tile Overlap (Halo) Support Plan

Branch: `feature/tile-overlap` (created locally off `feature/retile-and-dataset-origins`; GitHub stays read-only)

## Goals

- Allow raster tiles to carry an overlapping halo around their core region.
- Record overlap in the `ResultDescriptor` so operators can discriminate inputs.
- Provide `AddTileOverlap` (create halos) and `RemoveTileOverlap` (strip halos) operators.
- Enable ML segmentation patterns (convolution input larger than output) as a downstream consumer.
- Existing unsafe operators reject overlapped inputs with clear errors.
- **GIS correctness**: coverage/query intersection semantics remain *core-based*;
  halo pixels are extra data, never double-counted.
- **Readability**: two small single-purpose operators instead of overloading `ReTile`;
  tiles are self-describing so every placement helper stays correct.

## Design decisions (locked)

| Decision | Choice |
| --- | --- |
| Operator names | `AddTileOverlap` / `RemoveTileOverlap` (new ops, not a `ReTile` extension) |
| Why not extend `ReTile` | Removing overlap is a pure in-stream crop, not a grid re-alignment; routing it through ReTile's sub-query adapter would needlessly re-fetch data or hide special cases. Chaining `ReTile → AddTileOverlap` covers combined cases. |
| ONNX operator | Rejects overlap in this branch (segmentation adaptation = follow-up) |
| HTTP sinks | Auto-crop overlap at the service boundary before rendering (WMS/WCS/GeoTIFF/PNG) |
| Tile self-description | `BaseTile` gains an `overlap` field (serde default = zero for cache compat) |
| Edge behavior | Halo beyond dataset extent = no-data |
| v1 restriction | `AddTileOverlap` requires zero-overlap input; symmetric per-axis (y, x) halo |

## Data model

```rust
// geoengine/datatypes/src/raster/tiling.rs
pub struct TileOverlap { y: u32, x: u32 } // Default = zero
```

- `TileInformation` gains `overlap`; new `core_pixel_bounds()` (= today's
  `global_pixel_bounds()`, kept as alias) and `data_pixel_bounds()` (core ± overlap).
- `TilingStrategy` gains `overlap` (threaded into emitted `TileInformation`);
  tile *enumeration* stays core-intersection based — iteration counts unchanged.
- `BaseTile.overlap`: `tile_position` remains the CORE anchor
  (`position × tile_size`); grid array shape = `(ty + 2·oy, tx + 2·ox)`.
  Update `tile_information()`, `tile_geo_transform()`,
  `global_pixel_spatial_grid_definition()` accordingly.
- `SpatialGridDescriptor` (operators crate) gains `overlap` (serde default,
  runtime-only like `tile_size`; not persisted in `SpatialGridDescriptorDbType`).
- QueryRectangle semantics unchanged: queries intersect CORES.

## Phases

### Phase 0 — Branch setup

- [x] `git checkout -b feature/tile-overlap`
- [x] Save this plan.

### Phase 1 — Datatypes: `TileOverlap` + tiling math — **complete**

- [x] Type + constructors + validation (halo ≤ core per axis)
- [x] `TileInformation::{core_pixel_bounds, data_pixel_bounds}`; `global_pixel_bounds()`
      kept as documented core alias so all query/coverage semantics stay unchanged
- [x] Unit tests: validation, padded bounds, corner georeference, iterator core-stability

### Phase 2 — Datatypes: self-describing tiles — **complete**

- [x] `BaseTile.overlap` (+ serde default for cache compat)
- [x] Constructor churn handled via compiler-guided loop (~380 literal sites);
      `new_with_tile_info*` inherit overlap from `TileInformation`;
      `bounding_box()` now covers data extents (core anchor ± halo)
- [x] `crop_overlap(amount)` tile operation via positioned-grid blit
- [x] Roundtrip test: legacy serialized tile loads as zero overlap

### Phase 3 — Operators crate: descriptor support — **complete**

- [x] `SpatialGridDescriptor.overlap` + accessors (`tile_overlap()`, `has_tile_overlap()`,
      `with_tile_overlap()`); runtime-only like `tile_size`
- [x] `merge` requires equal overlaps (else `None`); `map`/`try_map`/reprojection reset
      to zero on geometry change; same-pixel-grid intersection preserves it

### Phase 4 — Rejection infrastructure — **complete**

- [x] Errors: `OverlappingTilesNotSupported`, `InvalidOverlap`, `NotEnoughTileOverlap`,
      `UnequalTileOverlap`
- [x] Helpers `RasterResultDescriptor::ensure_no_tile_overlap` +
      `engine::descriptor_multi_input::ensure_equal_tile_overlap`; applied in Onnx,
      RasterStacker, NeighborhoodAggregate, BandNeighborhoodAggregate,
      TemporalRasterAggregation, Downsampling, Interpolation, BandFilter, Histogram,
      ClassHistogram, Statistics, BoxPlot, MeanRasterPixelValuesOverTime
- [x] Equal-overlap rule available for pointwise multi-input operators

### Phase 5 — `AddTileOverlap` — **complete**

- [x] `processing/add_tile_overlap/`: validates halo ≤ core and zero-overlap input
- [x] `RasterSubQueryAdapter` aggregator pads each enumerated core tile; no-data at edges
- [x] API DTO + registration
- [x] Tests (5): neighbor-filled halo, border no-data, georeference of padded bounds,
      rejection of overlapping inputs and oversized halos

### Phase 6 — `RemoveTileOverlap` — **complete**

- [x] `amount: Option<TileOverlap>` (None = strip fully); sufficiency validated
- [x] Stream-map crop via `crop_overlap`; passthrough when nothing to remove
- [x] API DTO + registration
- [x] Tests (4): Add→Remove roundtrip equals original stream, partial removal,
      insufficient-overlap error, passthrough

### Phase 7 — Service boundary normalization — **complete**

- [x] `WrapWithProjectionAndResample::wrap_with_overlap_removal` runs before projection
      and resampling in `wrap_with_projection_and_resample` (entry point of WMS/WCS)

### Phase 8 — Verification — **complete**

- [x] `cargo fmt --all -- --check` clean; clippy clean for new code
      (24 pre-existing base-branch warnings in plot tests remain)
- [x] datatypes 682 ✓, operators 585 ✓ (incl. 9 new overlap tests),
      services 462 ✓ (`--skip external::`)
- [x] Zero golden drift (sources emit zero overlap; feature is opt-in)
- [x] `openapi.json` regenerated via `geoengine-cli openapi`; snapshot test passes

## Risks / notes

- Verify `ChangeGridBounds`/crop helpers suffice for symmetric local-grid crop.
- Cached serialized tiles: serde default keeps compatibility.
- ONNX segmentation adaptation documented as follow-up (input pad = receptive field
  radius; run model on padded tile; crop output core).

## Resume checkpoint

**Status: complete.** All phases implemented and verified on `feature/tile-overlap`.
Follow-up candidates (out of scope here): adapt the ONNX operator for segmentation
(run model on padded input, crop output core), expose equal-overlap tolerance in
pointwise multi-input operators, persist overlap alongside tile_size if descriptors
are ever stored at runtime fidelity.
