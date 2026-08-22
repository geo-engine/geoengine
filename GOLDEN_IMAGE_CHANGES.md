# Golden Image Changes — `feature/retile-and-dataset-origins`

Branch changes the tiling grid origin from **snapped** (to tiling origin reference)
to **source-anchored** (dataset's own origin). This shifts pixel-space queries by an
exact integer and moves tile boundaries by a fraction of a tile size.

## Summary

| Category | Count | Reason |
|----------|-------|--------|
| Adapted test query (image unchanged) | 1 | Exact pixel shift compensated in query |
| Regenerated golden (image changed) | 118 | Tile boundaries shifted / new tiling grid |
| New golden (image added) | 1 | New test case |

---

## PNG Images

| File | Test | Issue | Action | Verified |
|------|------|-------|--------|----------|
| `test_data/ogc/tiles/ndvi_3857_0_0_0.png` | `tiles::tests::tile_png_with_datetime` (CustomNativeTMS, 512×512) | Tile boundaries shift ~1px vertically due to source-anchored grid | **Regenerated** — clean 1px vertical shift, mean\_diff=0 after shift | Pixel-compared |
| `test_data/ogc/tiles/ndvi_webmercator_0_0_0.png` | `tiles::tests::tile_png_with_datetime` (WebMercatorQuad, 256×256) | OLD code forced query to exactly tile\_size (256×256 source px), cropping to NW quadrant of actual 357×356 grid at res 112424. NEW `tile_grid_bbox` queries full grid (full world). Bug fix, not just a shift. | **Regenerated** — correct full-world view (was NW crop) | GIS-correctness analysis |
| `test_data/ogc/tiles/natural_earth_rgb_1_0_1.png` | `tiles::tests::tile_png_with_datetime` (CustomNativeTMS, 512×512) | Tile boundaries shift ~2px vertically | **Regenerated** — clean 2px vertical shift, mean\_diff=0 after shift | Pixel-compared |
| `test_data/wms/raster_small.png` | `wms::tests::png_from_stream_non_full` | Query coordinates [-900,899]×[-1800,1799] in OLD grid (origin 0,0) are outside NEW grid (origin -180,90). Operator clips to valid bounds → same result | **No change** — operator clips gracefully, test passes, golden unchanged | Test passes |
| `test_data/wms/get_map_ndvi.png` | `wms::tests::get_map` (WMS GetMap, 600×600) | WMS GetMap uses BBOX in CRS coordinates (EPSG:4326), not pixel indices — not affected by grid origin change | **No change** — test passes | Test passes |
| `test_data/raster/png/png_from_stream.png` | `raster_stream_to_png::tests::png_from_stream` | Pixel query `[-800,-100]×[-199,499]` in OLD grid maps outside NEW grid → mostly nodata (8 KB). Adapted to `[100,1700]×[701,2299]` (+900 y, +1800 x) | **Adapted query**, golden **unchanged** (byte-identical, 470 KB) | Pixel-compared, byte-identical |

---

## GeoTIFF Images

### OGC / Stream Output Goldens

| File | Test | Issue | Action | Verified |
|------|------|-------|--------|----------|
| `test_data/raster/geotiff_from_stream_compressed.tiff` | `raster_stream_to_geotiff::tests::it_creates_geotiff_from_stream` | Tiling grid origin changed → tile boundaries shift | **Renamed** to `geotiff_with_no_data_from_stream_compressed.tiff` (corrected name reflecting nodata behavior) | Test passes |
| `test_data/raster/geotiff_with_no_data_from_stream_compressed.tiff` | (same as above) | New file, renamed from `geotiff_from_stream_compressed.tiff` | **New golden** (renamed) | Test passes |
| `test_data/raster/geotiff_big_tiff_from_stream_compressed.tiff` | `raster_stream_to_geotiff::tests::it_creates_big_tiff_from_stream` | Tiling grid origin changed → tile boundaries shift | **Regenerated** | Test passes |
| `test_data/raster/geotiff_with_mask_from_stream_compressed.tiff` | `raster_stream_to_geotiff::tests::it_creates_geotiff_with_mask_from_stream` | Tiling grid origin changed → tile boundaries shift | **Regenerated** | Test passes |
| `test_data/raster/cloud_optimized_geotiff_from_stream_compressed.tiff` | `raster_stream_to_geotiff::tests::it_creates_cloud_optimized_geotiff_from_stream` | Tiling grid origin changed → tile boundaries shift | **Regenerated** | Test passes |
| `test_data/raster/cloud_optimized_geotiff_big_tiff_from_stream_compressed.tiff` | `raster_stream_to_geotiff::tests::it_creates_cloud_optimized_big_geotiff_from_stream` | Tiling grid origin changed → tile boundaries shift | **Regenerated** | Test passes |
| `test_data/raster/cloud_optimized_geotiff_timestep_0_from_stream_compressed.tiff` | `raster_stream_to_geotiff::tests::it_creates_cloud_optimized_geotiff_from_stream_with_multiple_time_steps` | Tiling grid origin changed → tile boundaries shift | **Regenerated** | Test passes |
| `test_data/raster/cloud_optimized_geotiff_timestep_1_from_stream_compressed.tiff` | (same) | Tiling grid origin changed → tile boundaries shift | **Regenerated** | Test passes |
| `test_data/raster/cloud_optimized_geotiff_timestep_2_from_stream_compressed.tiff` | (same) | Tiling grid origin changed → tile boundaries shift | **Regenerated** | Test passes |

### MODIS NDVI Projected 3857

| File | Test | Issue | Action | Verified |
|------|------|-------|--------|----------|
| `test_data/raster/modis_ndvi/projected_3857/MOD13A2_M_NDVI_2014-04-01_tile-20_v6.rst` | `multi_band_gdal_source::tests::*` (reprojection) | Native 3857 dataset has origin `(-20037508.34, 19971868.88)` — source-anchored tiling changes the read region slightly | **Regenerated** | Test passes |

### Multi-Tile Test Goldens (108 TIFs)

| Directory | Count | Test | Issue | Action | Verified |
|-----------|-------|------|-------|--------|----------|
| `test_data/raster/multi_tile/results/z_index/tiles/` | 48 | `multi_band_gdal_source::tests::it_loads_multi_band_multi_file_mosaics*` | Source-anchored tiling shifts tile boundaries → all tiles regenerated for consistency | **Regenerated** | 13 tests pass (0 fail) |
| `test_data/raster/multi_tile/results/z_index_reversed/tiles/` | 48 | `multi_band_gdal_source::tests::it_loads_multi_band_multi_file_mosaics*` | Same as above | **Regenerated** | Same |
| `test_data/raster/multi_tile/results/overview_level_2/tiles/` | 12 | `multi_band_gdal_source::tests::it_loads_overview_level` | Same as above | **Regenerated** | Same |

---

## Code Fixes Enabling the Golden Changes

| Fix | File | Description |
|-----|------|-------------|
| OGC f64 round-trip bug | `services/src/api/handlers/ogc/tiles.rs` | Replaced `tile_spatial_bounds` → `spatial_to_grid_bounds` (f64, imprecise) with exact-integer `tile_grid_bbox`. Removed 20 lines. |
| Dead code removal | `services/src/api/handlers/ogc/tms_spec.rs` | Removed `tile_spatial_bounds` trait method + WebMercatorQuad override + tests. Removed `#[allow(dead_code)]` from `tile_grid_bbox`. -98 lines. |
| png\_from\_stream query shift | `operators/src/util/raster_stream_to_png.rs:391` | `[-800,-100]×[-199,499]` → `[100,1700]×[701,2299]` (+900 y, +1800 x) for source-anchored grid. |
| Retile error handling | `operators/src/processing/retile/mod.rs` | `.expect()` → `.ok_or_else()` for missing input tiles. |
| Geotiff param cleanup | `operators/src/util/raster_stream_to_geotiff.rs` | Removed unused `_tiling_specification` parameter. |
| Stacker DRY | `operators/src/processing/raster_stacker.rs` | Replaced 10 identical match arms with `stacker_arm!` macro. -57 lines. |
