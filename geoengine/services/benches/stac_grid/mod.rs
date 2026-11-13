//! STAC metadata benchmark scenarios and reports.

mod fixtures;

use criterion::{Criterion, SamplingMode, Throughput};
use fixtures::*;
use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{
        AxisAlignedRectangle, BandSelection, BoundingBox2D, DateTime, SpatialPartition2D,
        TimeInterval,
    },
    raster::{GeoTransform, TileIdx, TileInformation, TileSize},
    spatial_reference::SpatialReference,
};
use geoengine_operators::{engine::MetaData, source::MultiBandGdalLoadingInfoQueryRectangle};
use geoengine_services::{
    datasets::external::stac::{StacGrid, StacMultiBandMetaData},
    util::join_base_url_and_path,
};
use httptest::Server;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs,
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use url::Url;

fn expected_scene_files(bbox: (f64, f64, f64, f64)) -> BTreeSet<String> {
    matching_scene_ids(bbox)
        .into_iter()
        .map(|(x, y)| format!("https://assets.example/scene-{x}-{y}.tif"))
        .collect()
}

fn xyz_bbox(z: u8, x: u32, y: u32) -> (f64, f64, f64, f64) {
    let n = 2_f64.powi(i32::from(z));
    let lon = |x: f64| x / n * 360. - 180.;
    let lat = |y: f64| {
        (std::f64::consts::PI * (1. - 2. * y / n))
            .sinh()
            .atan()
            .to_degrees()
    };
    (
        lon(f64::from(x)),
        lat(f64::from(y + 1)),
        lon(f64::from(x + 1)),
        lat(f64::from(y)),
    )
}

#[derive(Clone)]
struct Tile {
    x: u32,
    y: u32,
    bbox: (f64, f64, f64, f64),
    arrival_ms: u64,
}

fn viewport(z: u8, center: (f64, f64), reverse: bool) -> Vec<Tile> {
    let n = 2_f64.powi(i32::from(z));
    let x = ((center.0 + 180.) / 360. * n).floor() as u32;
    let lat = center.1.to_radians();
    let y =
        ((1. - (lat.tan() + 1. / lat.cos()).ln() / std::f64::consts::PI) / 2. * n).floor() as u32;
    let mut coordinates = Vec::new();
    for dy in -1_i64..=1 {
        for dx in -1_i64..=1 {
            coordinates.push((
                (i64::from(x) + dx).clamp(0, n as i64 - 1) as u32,
                (i64::from(y) + dy).clamp(0, n as i64 - 1) as u32,
            ));
        }
    }
    if reverse {
        // deterministic Fisher-Yates, seed 7
        let mut state = 7_u64;
        for i in (1..coordinates.len()).rev() {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            coordinates.swap(i, (state as usize) % (i + 1));
        }
    } else {
        coordinates.sort_by_key(|(tx, ty)| (tx.abs_diff(x) + ty.abs_diff(y), *ty, *tx));
    }
    coordinates
        .into_iter()
        .enumerate()
        .map(|(i, (x, y))| Tile {
            x,
            y,
            bbox: xyz_bbox(z, x, y),
            arrival_ms: (i * 2) as u64,
        })
        .collect()
}

fn tile_information(tile: &Tile) -> TileInformation {
    TileInformation::new(
        TileIdx::new_y_x(0, 0),
        TileSize::new_unchecked(1, 1),
        GeoTransform::new_unchecked(
            (tile.bbox.0, tile.bbox.3).into(),
            tile.bbox.2 - tile.bbox.0,
            tile.bbox.1 - tile.bbox.3,
        ),
    )
}

#[derive(serde::Deserialize)]
struct BenchResult {
    zoom: u8,
    center: String,
    order: String,
    rate: String,
    mode: String,
    target_cells: usize,
    viewport_ms: f64,
    attempts: usize,
    inflight_joins: Option<usize>,
    inflight_pages_avoided: Option<usize>,
    cache_hits: Option<usize>,
    cached_file_references: usize,
    cached_unique_files: usize,
    estimated_cache_bytes: usize,
}

/// Cold working set of complete cell results, before eviction. Uses the same
/// metadata parser and byte weigher as the provider, outside the timed queries.
fn cache_footprint(
    meta: &StacMultiBandMetaData,
    cells: &BTreeMap<String, Vec<(i32, i32)>>,
) -> (usize, usize, usize) {
    let mut references = 0;
    let mut unique_files = BTreeSet::new();
    let mut bytes = 0;
    for scenes in cells.values() {
        let mut files = Vec::new();
        for &(x, y) in scenes {
            let scene: stac::Item =
                serde_json::from_value(item(x, y)).expect("mock scene is valid STAC");
            meta.process_stac_item(&scene, &mut files)
                .expect("mock asset metadata is valid");
            // The fixture has one asset/band/version per scene, so scene IDs are
            // sufficient here; production interning would need full metadata identity.
            unique_files.insert((x, y));
        }
        assert_eq!(files.len(), scenes.len());
        references += files.len();
        bytes += StacMultiBandMetaData::cache_weight(&files);
    }
    (references, unique_files.len(), bytes)
}

fn median(values: impl Iterator<Item = f64>) -> f64 {
    let mut values = values.collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        values[middle - 1].midpoint(values[middle])
    } else {
        values[middle]
    }
}

#[allow(clippy::print_stdout)]
fn print_cache_footprints(results: &[BenchResult]) {
    println!("\nMetadata working set, zoom 8 ordinary viewport (before eviction):");
    println!("\n| Cells | File references / unique files | Copies/file | Cache weight KiB |");
    println!("|-------|--------------------------------|-------------|------------------|");
    for (target, label) in [(512, "512"), (8192, "8,192"), (131_072, "131,072")] {
        let grid = results
            .iter()
            .filter(|r| {
                r.zoom == 8
                    && r.center == "ordinary"
                    && r.order == "center-out"
                    && r.rate == "unlimited"
                    && r.mode == "grid-cache"
                    && r.target_cells == target
            })
            .collect::<Vec<_>>();
        if grid.is_empty() {
            continue;
        }
        let references = median(grid.iter().map(|r| r.cached_file_references as f64));
        let unique = median(grid.iter().map(|r| r.cached_unique_files as f64));
        let copies = if unique == 0. {
            0.
        } else {
            references / unique
        };
        let kib = median(grid.iter().map(|r| r.estimated_cache_bytes as f64)) / 1024.;
        println!(
            "| {label:>5} | {references:24.0} / {unique:<3.0} | {copies:10.2}x | {kib:16.1} |"
        );
    }
    println!(
        "\nWeights estimate metadata, keys and result headers; exclude raster pixels, cache internals and allocation overhead."
    );
}

#[allow(clippy::print_stdout)]
fn print_trace_reuse(results: &[BenchResult], positions: &[(&str, &str)]) {
    if CacheTraceCapture::enabled() {
        println!("\nTRACE-derived reuse at 8 pages/s, same zoom and arrival order:");
        println!(
            "\n| Cells | Position | Queries attached in-flight | Duplicate pages avoided | Completed-cache hits |"
        );
        println!(
            "|-------|----------|----------------------------|-------------------------|----------------------|"
        );
        for (target, label) in [(512, "512"), (8192, "8,192"), (131_072, "131,072")] {
            for &(center, position) in positions {
                let grid = results
                    .iter()
                    .filter(|r| {
                        r.zoom == 8
                            && r.order == "center-out"
                            && r.center == center
                            && r.rate == "8ps"
                            && r.mode == "grid-cache"
                            && r.target_cells == target
                    })
                    .collect::<Vec<_>>();
                if grid.is_empty() {
                    continue;
                }
                println!(
                    "| {label:>5} | {position:8} | {:26.0} | {:23.0} | {:20.0} |",
                    median(
                        grid.iter()
                            .filter_map(|r| r.inflight_joins.map(|n| n as f64))
                    ),
                    median(
                        grid.iter()
                            .filter_map(|r| r.inflight_pages_avoided.map(|n| n as f64))
                    ),
                    median(grid.iter().filter_map(|r| r.cache_hits.map(|n| n as f64)))
                );
            }
        }
        println!(
            "\nAttached queries are cell lookups; avoided pages compare identical grid lookups without sharing."
        );
        println!(
            "HTTP attempts above compare the per-tile baseline; hypothetical retry savings are excluded."
        );
    } else {
        println!(
            "\nTRACE events are compiled out in this profile; lookup-reuse CSV fields are blank."
        );
    }
}

#[allow(clippy::print_stdout)]
fn print_summary(csv: &str) {
    let results = csv::Reader::from_reader(csv.as_bytes())
        .deserialize::<BenchResult>()
        .collect::<Result<Vec<_>, _>>()
        .expect("benchmark CSV matches its result schema");
    // Summarize zoom 8; the full scenario matrix stays in the CSV files.
    let group = |center: &str, rate: &str, mode: &str, target: usize| {
        results
            .iter()
            .filter(|r| {
                r.zoom == 8
                    && r.order == "center-out"
                    && r.center == center
                    && r.rate == rate
                    && r.mode == mode
                    && r.target_cells == target
            })
            .collect::<Vec<_>>()
    };
    let positions = [("ordinary", "Ordinary"), ("grid-boundary", "Boundary")]
        .into_iter()
        .filter(|(center, _)| !group(center, "unlimited", "baseline", 0).is_empty())
        .collect::<Vec<_>>();
    println!(
        "\nSTAC grid benchmark: {} viewport iterations (including warm-up)",
        results.len()
    );
    println!("Nine XYZ tiles, six concurrent jobs; local paginated STAC; metadata lookup only.");
    println!("\nMedian viewport times (ms), zoom 8, center-out arrivals:");
    println!("\n| STAC limit | Position | Per-tile | 512 (default) | 8,192 | 131,072 |");
    println!("|------------|----------|----------|---------------|-------|---------|");
    for (rate, label) in [("unlimited", "Unlimited"), ("8ps", "8 pages/s")] {
        for &(center, position) in &positions {
            let comparisons = [
                ("baseline", 0),
                ("grid-cache", 512),
                ("grid-cache", 8192),
                ("grid-cache", 131_072),
            ]
            .map(|(mode, target)| group(center, rate, mode, target));
            if comparisons.iter().any(Vec::is_empty) {
                continue;
            }
            let timings = comparisons.map(|rows| median(rows.iter().map(|r| r.viewport_ms)));
            println!(
                "| {label:10} | {position:8} | {:8.0} | {:13.0} | {:5.0} | {:7.0} |",
                timings[0], timings[1], timings[2], timings[3]
            );
        }
    }
    println!("\n8,192 cells at 8 pages/s versus per-tile searches (including 429 retries):");
    for &(center, position) in &positions {
        let baseline = group(center, "8ps", "baseline", 0);
        let grid = group(center, "8ps", "grid-cache", 8192);
        if baseline.is_empty() || grid.is_empty() {
            continue;
        }
        let before = median(baseline.iter().map(|r| r.attempts as f64));
        let after = median(grid.iter().map(|r| r.attempts as f64));
        let reduction = 100. * (1. - after / before);
        let speedup = median(baseline.iter().map(|r| r.viewport_ms))
            / median(grid.iter().map(|r| r.viewport_ms));
        println!(
            "{position}: HTTP attempts {before:.0} -> {after:.0} ({reduction:.0}% fewer), viewport {speedup:.1}x faster."
        );
    }
    print_trace_reuse(&results, &positions);
    print_cache_footprints(&results);
    println!("All executed cases (including warm-up iterations) are in the CSV files.");
}

struct BenchCase<'a> {
    zoom: u8,
    center_name: &'a str,
    order: &'a str,
    rate: Option<usize>,
    mode: &'a str,
    target: usize,
    capacity: usize,
    tiles: Vec<Tile>,
}

struct BenchOutput {
    csv: String,
    tile_csv: String,
    trace: String,
    cache_log: String,
}

#[allow(clippy::too_many_lines)]
async fn run_case(case: &BenchCase<'_>, repeat: u64, output: &mut BenchOutput) -> Duration {
    let BenchCase {
        zoom,
        center_name,
        order,
        rate,
        mode,
        target,
        capacity,
        ref tiles,
    } = *case;
    let captured_logs = CacheTraceCapture::default();
    let trace_enabled = CacheTraceCapture::enabled();
    let _trace_guard = trace_enabled.then(|| captured_logs.install());
    let server = Server::run();
    let counters = Arc::new(Counters::default());
    let grid_target = if target == 0 { 512 } else { target as i32 };
    let provider = provider(
        &server,
        counters.clone(),
        rate,
        StacGrid {
            target_number_of_cells: grid_target,
        },
        capacity,
    );
    let meta = metadata(&provider);
    let base_url = Url::parse(&provider.api_url).expect("mock URL parses");
    let items_url =
        join_base_url_and_path(&base_url, "collections/synthetic/items").expect("items URL");
    let projected = provider
        .dataset
        .projection
        .area_of_use_projected::<BoundingBox2D>()
        .expect("dataset CRS extent");
    let side = if target == 0 {
        0.
    } else {
        projected.size_x().sqrt() * projected.size_y().sqrt() / (target as f64).sqrt()
    };
    let mut intersections = 0;
    let mut unique = BTreeSet::new();
    let mut cell_pages = BTreeMap::new();
    let mut cell_scenes = BTreeMap::new();
    if target > 0 {
        for tile in tiles {
            let bounds = SpatialPartition2D::new(
                (tile.bbox.0, tile.bbox.3).into(),
                (tile.bbox.2, tile.bbox.1).into(),
            )
            .expect("tile bounds");
            let grid = provider
                .grid
                .for_extent(projected)
                .expect("grid query bounds");
            let cells = grid
                .cells_for_bbox(bounds.as_bbox())
                .expect("cell intersections");
            for cell in cells {
                intersections += 1;
                unique.insert(cell_key(cell.index));
                let footprint = grid
                    .clip_cell_to_extent(cell.bbox)
                    .expect("selected cell overlaps CRS extent");
                // The synthetic dataset uses EPSG:4326, so no reprojection is needed.
                let bbox = footprint;
                let scenes = matching_scene_ids((
                    bbox.lower_left().x,
                    bbox.lower_left().y,
                    bbox.upper_right().x,
                    bbox.upper_right().y,
                ));
                cell_pages.insert(
                    cell_key(cell.index),
                    scenes.len().div_ceil(PAGE_SIZE).max(1),
                );
                cell_scenes.insert(cell_key(cell.index), scenes);
            }
        }
    }
    let origin = tokio::time::Instant::now();
    let mut queue = futures::stream::iter(tiles.iter().cloned().enumerate().map(|(rank, tile)| {
        let meta = &meta;
        let items_url = items_url.clone();
        async move {
            tokio::time::sleep_until(origin + Duration::from_millis(tile.arrival_ms)).await;
            let dispatch = tokio::time::Instant::now()
                .duration_since(origin)
                .as_secs_f64()
                * 1000.;
            let time = TimeInterval::new(
                DateTime::new_utc(2026, 1, 3, 0, 0, 0),
                DateTime::new_utc(2026, 1, 4, 0, 0, 0),
            )
            .expect("day interval");
            let spatial = SpatialPartition2D::new(
                (tile.bbox.0, tile.bbox.3).into(),
                (tile.bbox.2, tile.bbox.1).into(),
            )
            .expect("tile spatial bounds");
            let actual = if mode == "baseline" {
                let bbox = BoundingBox2D::new(
                    (tile.bbox.0, tile.bbox.1).into(),
                    (tile.bbox.2, tile.bbox.3).into(),
                )
                .expect("tile bbox");
                meta.search_bbox(items_url, bbox, time)
                    .await
                    .expect("baseline STAC search succeeds")
                    .into_iter()
                    .filter(|file| {
                        file.time.intersects(&time) && file.spatial_partition.intersects(&spatial)
                    })
                    .map(|file| file.params.file_path.to_string_lossy().into_owned())
                    .collect::<BTreeSet<_>>()
            } else {
                let query = MultiBandGdalLoadingInfoQueryRectangle::new(
                    spatial,
                    time,
                    BandSelection::first(),
                    true,
                );
                let info = meta
                    .loading_info(query)
                    .await
                    .expect("production loading_info succeeds");
                info.tile_files(time, tile_information(&tile), 0)
                    .into_iter()
                    .map(|params| params.file_path.to_string_lossy().into_owned())
                    .collect::<BTreeSet<_>>()
            };
            let expected = expected_scene_files(tile.bbox);
            (
                rank,
                actual,
                expected,
                dispatch,
                tokio::time::Instant::now()
                    .duration_since(origin)
                    .as_secs_f64()
                    * 1000.,
            )
        }
    }))
    .buffer_unordered(6);
    let mut latencies = Vec::new();
    let mut correct = true;
    tokio::time::timeout(Duration::from_mins(1), async {
        while let Some((rank, actual, expected, dispatch, completion)) = queue.next().await {
            if actual != expected { correct = false; }
            latencies.push(completion);
            writeln!(output.tile_csv, "{repeat},{zoom},{center_name},{order},{},{mode},{target},{},{},{},{dispatch:.3},{completion:.3}",
                if rate.is_some() { "8ps" } else { "unlimited" }, tiles[rank].x, tiles[rank].y, tiles[rank].arrival_ms)
                .expect("writing a tile row to a String cannot fail");
        }
    }).await.expect("benchmark case must finish within 60 seconds");
    let elapsed_duration = origin.elapsed();
    assert!(
        correct,
        "benchmark output differs from synthetic catalog: {zoom}/{center_name}/{order}/{mode}/{target}"
    );
    latencies.sort_by(f64::total_cmp);
    let percentile = |p: f64| latencies[((latencies.len() - 1) as f64 * p).ceil() as usize];
    let elapsed = elapsed_duration.as_secs_f64() * 1000.;
    let observations = captured_logs.observations();
    if trace_enabled {
        assert_eq!(observations.len(), intersections);
    }
    let mut cell_fetches = 0;
    let mut fetched_pages = 0;
    let mut inflight_joins = 0;
    let mut inflight_pages_avoided = 0;
    let mut cache_hits = 0;
    let mut cached_pages_avoided = 0;
    for observation in observations {
        let pages = cell_pages[&cell_key(observation.cell)];
        match observation.kind {
            LookupKind::Fetch => {
                cell_fetches += 1;
                fetched_pages += pages;
            }
            LookupKind::InFlight => {
                inflight_joins += 1;
                inflight_pages_avoided += pages;
            }
            LookupKind::CacheHit => {
                cache_hits += 1;
                cached_pages_avoided += pages;
            }
        }
    }
    if trace_enabled && target > 0 {
        assert_eq!(
            fetched_pages,
            counters.accepted_pages.load(Ordering::Relaxed)
        );
        assert_eq!(cell_fetches + inflight_joins + cache_hits, intersections);
    }
    if trace_enabled && mode == "grid-inflight" {
        assert_eq!(cache_hits, 0);
    }
    let (cached_file_references, cached_unique_files, estimated_cache_bytes) =
        if mode == "grid-cache" {
            cache_footprint(&meta, &cell_scenes)
        } else {
            (0, 0, 0)
        };
    let cache_duplication_factor = if cached_unique_files == 0 {
        0.
    } else {
        cached_file_references as f64 / cached_unique_files as f64
    };
    if mode == "grid-cache" {
        assert!(
            estimated_cache_bytes < capacity,
            "cold working set fits the benchmark cache"
        );
        assert_eq!(
            cached_file_references,
            counters.items.load(Ordering::Relaxed)
        );
    }
    let [
        cell_fetches,
        inflight_joins,
        inflight_pages_avoided,
        cache_hits,
        cached_pages_avoided,
    ] = [
        cell_fetches,
        inflight_joins,
        inflight_pages_avoided,
        cache_hits,
        cached_pages_avoided,
    ]
    .map(|count| {
        if trace_enabled {
            count.to_string()
        } else {
            String::new()
        }
    });
    let row = format!(
        "{repeat},{zoom},{center_name},{order},{},{mode},{target},{side:.3},{},{intersections},{},{:.3},{:.3},{:.3},{elapsed:.3},{},{},{},{},{},{cell_fetches},{inflight_joins},{inflight_pages_avoided},{cache_hits},{cached_pages_avoided},{cached_file_references},{cached_unique_files},{cache_duplication_factor:.3},{estimated_cache_bytes},true\n",
        if rate.is_some() { "8ps" } else { "unlimited" },
        tiles.len(),
        unique.len(),
        latencies[0],
        percentile(0.5),
        percentile(0.95),
        counters.attempts.load(Ordering::Relaxed),
        counters.accepted_pages.load(Ordering::Relaxed),
        counters.limited.load(Ordering::Relaxed),
        counters.items.load(Ordering::Relaxed),
        counters.bytes.load(Ordering::Relaxed)
    );
    output.csv.push_str(&row);
    writeln!(output.cache_log, "repeat={repeat} zoom={zoom} center={center_name} order={order} rate={rate:?} mode={mode} target={target}")
        .expect("writing the trace case header cannot fail");
    captured_logs.append_log(&mut output.cache_log);
    elapsed_duration
}

/// Benchmark cold STAC metadata lookups for nine XYZ tiles with six concurrent jobs.
///
/// Each Criterion iteration starts with a fresh provider cache and mock rate limiter.
/// Only viewport processing is timed; setup, cache-weight analysis and report writing
/// happen outside the measured duration.
///
/// # Measured results (2026-10-14)
///
/// Run from `geoengine/`:
/// `cargo bench --profile bench-ci -p geoengine-services --bench stac_grid -j 2`.
/// Rust 1.97.1, `x86_64`; 80 cases, 10 Criterion samples per case, 1 s warm-up
/// and a 5 s measurement target. Each iteration uses a fresh provider cache.
/// Local paginated mock: 32 items/page, 20 ms + 1 ms/item response delay;
/// nine XYZ tiles, six concurrent jobs, metadata only. No raster I/O is timed.
///
/// Criterion estimated mean viewport time (ms); these are illustrative measurements,
/// not assertions. Cached columns use the indicated target cell count; in-flight
/// sharing uses 512 cells with completed-result caching disabled.
///
/// | Zoom | Position | Arrival order | STAC limit | Per-tile | Cache 512 | Cache 8,192 | Cache 131,072 | In-flight 512 |
/// |------|----------|---------------|------------|----------|-----------|-------------|---------------|---------------|
/// | 8 | Ordinary | Center-out | Unlimited | 57.9 | 255.2 | 76.7 | 197.6 | 509.1 |
/// | 8 | Ordinary | Center-out | 8 pages/s | 1059.9 | 254.9 | 77.0 | 3201.8 | 1510.5 |
/// | 8 | Ordinary | Shuffled | Unlimited | 59.8 | 255.1 | 48.7 | 160.2 | 509.2 |
/// | 8 | Ordinary | Shuffled | 8 pages/s | 1061.4 | 255.6 | 48.8 | 3163.9 | 1509.9 |
/// | 8 | Boundary | Center-out | Unlimited | 55.8 | 508.9 | 62.4 | 191.1 | 514.0 |
/// | 8 | Boundary | Center-out | 8 pages/s | 1057.4 | 2511.3 | 62.4 | 3195.1 | 3317.3 |
/// | 8 | Boundary | Shuffled | Unlimited | 55.8 | 263.9 | 41.7 | 149.6 | 539.3 |
/// | 8 | Boundary | Shuffled | 8 pages/s | 1057.9 | 2266.1 | 41.8 | 3154.0 | 3593.2 |
/// | 10 | Ordinary | Center-out | Unlimited | 52.9 | 255.3 | 43.0 | 54.1 | 509.4 |
/// | 10 | Ordinary | Center-out | 8 pages/s | 1054.4 | 255.4 | 43.0 | 54.1 | 1510.0 |
/// | 10 | Ordinary | Shuffled | Unlimited | 52.9 | 255.0 | 42.8 | 53.8 | 509.3 |
/// | 10 | Ordinary | Shuffled | 8 pages/s | 1054.6 | 255.0 | 42.9 | 54.2 | 1510.7 |
/// | 10 | Boundary | Center-out | Unlimited | 50.7 | 509.2 | 62.6 | 46.2 | 514.5 |
/// | 10 | Boundary | Center-out | 8 pages/s | 1052.2 | 2512.4 | 62.7 | 46.5 | 3319.3 |
/// | 10 | Boundary | Shuffled | Unlimited | 50.5 | 264.4 | 42.0 | 33.6 | 589.9 |
/// | 10 | Boundary | Shuffled | 8 pages/s | 1050.9 | 2266.5 | 41.6 | 33.3 | 3543.0 |
///
/// HTTP attempts at zoom 8, center-out arrivals, 8 pages/s (median, including
/// warm-up and sampling iterations; attempts include 429 retries):
///
/// | Position | Per-tile attempts | Cache 8,192 attempts | Reduction |
/// |----------|-------------------|----------------------|-----------|
/// | Ordinary | 10 | 4 | 60% |
/// | Boundary | 10 | 4 | 60% |
///
/// TRACE events are compiled out by the workspace `release_max_level_debug` setting.
/// Lookup-reuse counts are unavailable in this profile and remain blank in CSV;
/// no cache-hit or in-flight-join counts are inferred from HTTP counters.
///
/// Cold metadata working set, zoom 8 ordinary viewport, center-out, unlimited STAC:
///
/// | Cells | File references | Unique files | Copies/file | Cache weight KiB |
/// |-------|-----------------|--------------|-------------|------------------|
/// | 512 | 144 | 144 | 1.00 | 53.1 |
/// | 8,192 | 64 | 49 | 1.31 | 23.9 |
/// | 131,072 | 90 | 25 | 3.60 | 35.3 |
///
/// Cache weights exclude raster pixels, cache internals and allocation overhead.
/// Smaller cells duplicate metadata; larger cells overfetch. This cold-viewport
/// benchmark does not measure eviction churn. Criterion reports and the CSV/trace
/// artifacts retain the full measurements.
///
/// # Panics
/// Panics if fixture setup or queries fail, results are incorrect, or reports cannot be written.
#[allow(clippy::too_many_lines, clippy::print_stderr)]
pub fn stac_grid_benchmark(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime can be created");
    runtime.block_on(validate_trace_capture());
    let out = std::env::var_os("STAC_BENCH_OUTPUT_DIR").map_or_else(
        || std::env::temp_dir().join("stac-grid-benchmark"),
        PathBuf::from,
    );
    fs::create_dir_all(&out).expect("benchmark output directory is writable");
    let quick = std::env::var_os("STAC_BENCH_QUICK").is_some();
    let zooms: &[u8] = if quick { &[8] } else { &[8, 10] };
    let centers: &[(&str, (f64, f64))] = if quick {
        &[("ordinary", (8.6, 50.1))]
    } else {
        &[("ordinary", (8.6, 50.1)), ("grid-boundary", (0., 45.))]
    };
    let csv = String::from(
        "repeat,zoom,center,order,rate,mode,target_cells,cell_side_units,tile_count,total_cell_intersections,unique_cells,tile_first_ms,tile_p50_ms,tile_p95_ms,viewport_ms,attempts,accepted_pages,rate_limited,returned_items,returned_bytes,cell_fetches,inflight_joins,inflight_pages_avoided,cache_hits,cached_pages_avoided,cached_file_references,cached_unique_files,cache_duplication_factor,estimated_cache_bytes,correct\n",
    );
    let tile_csv = String::from(
        "repeat,zoom,center,order,rate,mode,target_cells,tile_x,tile_y,arrival_ms,dispatch_ms,completion_ms\n",
    );
    let trace = String::from(
        "zoom,center,order,tile_z,tile_x,tile_y,arrival_rank,arrival_ms,bbox_west,bbox_south,bbox_east,bbox_north,cells_512,cells_8192,cells_131072\n",
    );
    let cache_log = String::new();
    let mut output = BenchOutput {
        csv,
        tile_csv,
        trace,
        cache_log,
    };
    let mut group = criterion.benchmark_group("stac_grid");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5))
        .sampling_mode(SamplingMode::Flat)
        .throughput(Throughput::Elements(9));
    for &zoom in zooms {
        for &(center_name, center) in centers {
            for reverse in [false, true] {
                let order = if reverse {
                    "seeded-permutation"
                } else {
                    "center-out"
                };
                let tiles = viewport(zoom, center, reverse);
                for tile in &tiles {
                    let bounds = SpatialPartition2D::new(
                        (tile.bbox.0, tile.bbox.3).into(),
                        (tile.bbox.2, tile.bbox.1).into(),
                    )
                    .expect("tile bounds");
                    let projected = SpatialReference::epsg_4326()
                        .area_of_use_projected::<BoundingBox2D>()
                        .expect("EPSG:4326 extent");
                    let mut ids = Vec::new();
                    for target in [512, 8192, 131_072] {
                        let grid = StacGrid {
                            target_number_of_cells: target,
                        }
                        .for_extent(projected)
                        .expect("grid is valid");
                        let cells = grid
                            .cells_for_bbox(bounds.as_bbox())
                            .expect("cell intersections")
                            .map(|cell| cell_key(cell.index))
                            .collect::<Vec<_>>();
                        ids.push(cells.join(";"));
                    }
                    writeln!(output.trace, "{zoom},{center_name},{order},{zoom},{},{},{},{},{:.8},{:.8},{:.8},{:.8},{},{},{}",
                        tile.x, tile.y, tile.arrival_ms / 2, tile.arrival_ms, tile.bbox.0, tile.bbox.1, tile.bbox.2, tile.bbox.3, ids[0], ids[1], ids[2])
                        .expect("writing a trace row to a String cannot fail");
                }
                for rate in [None, Some(8)] {
                    let rate_label = if rate.is_some() { "8ps" } else { "unlimited" };
                    for (mode, target, capacity) in [
                        ("baseline", 0_usize, 0_usize),
                        ("grid-cache", 512, 100 * 1024 * 1024),
                        ("grid-cache", 8192, 100 * 1024 * 1024),
                        ("grid-cache", 131_072, 100 * 1024 * 1024),
                        ("grid-inflight", 512, 0),
                    ] {
                        let case = BenchCase {
                            zoom,
                            center_name,
                            order,
                            rate,
                            mode,
                            target,
                            capacity,
                            tiles: tiles.clone(),
                        };
                        let name =
                            format!("z{zoom}/{center_name}/{order}/{rate_label}/{mode}/{target}");
                        let mut repeat = 0;
                        group.bench_function(name, |bencher| {
                            bencher.iter_custom(|iterations| {
                                runtime.block_on(async {
                                    let mut elapsed = Duration::ZERO;
                                    for _ in 0..iterations {
                                        repeat += 1;
                                        elapsed += run_case(&case, repeat, &mut output).await;
                                    }
                                    elapsed
                                })
                            });
                        });
                    }
                }
            }
        }
    }
    group.finish();
    print_summary(&output.csv);
    fs::write(out.join("results.csv"), output.csv).expect("write benchmark results");
    fs::write(out.join("tiles.csv"), output.tile_csv).expect("write tile timings");
    fs::write(out.join("trace.csv"), output.trace).expect("write tile and cell trace");
    fs::write(out.join("cache-trace.log"), output.cache_log)
        .expect("write captured cache trace logs");
    eprintln!("STAC grid benchmark output: {}", out.display());
}
