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

### Phase 1 — Datatypes: `TileOverlap` + tiling math

- [ ] Type + constructors + validation (`overlap axis < tile_size axis`)
- [ ] `TileInformation::{core_pixel_bounds, data_pixel_bounds}`, overlap-aware helpers;
      audit/rename `global_pixel_bounds()` call sites
- [ ] Unit tests: padded bounds, corner georeference, iterator core-stability

### Phase 2 — Datatypes: self-describing tiles

- [ ] `BaseTile.overlap` (+ serde default for cache compat)
- [ ] Constructor churn (~25 sites incl. mocks/tests/examples);
      `new_with_tile_info` inherits overlap from `TileInformation`
- [ ] Placement helpers account for overlap; add `crop_overlap(amount)` tile operation
- [ ] Roundtrip test: legacy serialized tile (no overlap key) loads as zero

### Phase 3 — Operators crate: descriptor support

- [ ] `SpatialGridDescriptor.overlap` + accessors (`has_overlap()`)
- [ ] Constructors / merge / reproject / intersection paths preserve or reset overlap
      correctly (any grid geometry change ⇒ reset to zero unless provably unchanged)

### Phase 4 — Rejection infrastructure

- [ ] Errors: `OverlappingTilesNotSupported { operator }`, `InvalidOverlap`,
      `NotEnoughTileOverlap { available, requested }`, `UnequalTileOverlap`
- [ ] Helper `ensure_no_input_overlap(desc, op)`; apply in `_initialize` of:
      Onnx, RasterStacker, NeighborhoodAggregate, reprojection/resampling wrappers,
      temporal aggregation, statistics/counting ops (final list via audit)
- [ ] Pointwise multi-input ops: allow equal overlap only

### Phase 5 — `AddTileOverlap`

- [ ] `processing/add_tile_overlap/`: validate params; descriptor overlap set
- [ ] Processor via `RasterSubQueryAdapter`: accu = empty padded grid,
      `tile_query_rectangle` = padded bounds, fold = `grid_blit_from`,
      emit padded tile with same time/band/tile_position
- [ ] API DTO + registration (mirror ReTile: api_operator, enum, try_from)
- [ ] Tests: interior halo = neighbor pixels; border halo = no-data;
      georeference check; zero-overlap-input guard

### Phase 6 — `RemoveTileOverlap`

- [ ] Params `amount: Option<[u32; 2]>` (None = strip fully); validate sufficiency
- [ ] Plain stream-map crop (no sub-queries); descriptor overlap reduced
- [ ] API DTO + registration
- [ ] Tests: Add → Remove roundtrip equals original stream; partial removal;
      insufficient-overlap error

### Phase 7 — Service boundary normalization

- [ ] Where projection/resolution wrapping occurs: if final raster descriptor
      `has_overlap()` → wrap with `RemoveTileOverlap` before sinks (WMS/WCS/GeoTIFF/PNG)
- [ ] Handler test: overlapped workflow renders identically to non-overlapped

### Phase 8 — Verification

- [ ] `cargo fmt --all -- --check`; `cargo clippy --all-features --all-targets`
- [ ] Focused suites: datatypes, operators, services (`--skip external::`)
- [ ] Zero golden drift expected (opt-in feature; sources emit zero overlap)
- [ ] Regenerate `openapi.json`

## Risks / notes

- Verify `ChangeGridBounds`/crop helpers suffice for symmetric local-grid crop.
- Cached serialized tiles: serde default keeps compatibility.
- ONNX segmentation adaptation documented as follow-up (input pad = receptive field
  radius; run model on padded tile; crop output core).

## Resume checkpoint

- Branch created; plan saved. Implementation not yet started.
