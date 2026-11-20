#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

//! One 256 px OGC API tile of the NDVI test data (EPSG:4326) as `render_tile` builds it
//! (`queries::ogc_tile::tile_on_grid`, then PNG encoding), without the database, for each kind of
//! tile matrix set at two tile matrices; plus the same `WebMercatorQuad` tile through the WMS
//! wrapper (`AdaptToGrid`) as the reference.
//!
//! With `GEOENGINE_BENCH_METRICS=1`, prints per case the operator grid and the tile's pixels, the
//! mean wall time per stage (the spans of `tile_on_grid`), the stream metrics per operator and the
//! CPU time per tile by thread group of this process (from `/proc/self/task/*/schedstat`; the GDAL
//! workers are other processes).
//!
//! The execution context is the operator benches' mock, so initializing reads no metadata from
//! the database and costs less than in the server.
//!
//! `cargo build --release --bin gdalsource-process && [GEOENGINE_BENCH_METRICS=1] cargo bench --bench ogc_tiles`

#[path = "../../operators/benches/common/mod.rs"]
mod common;

use common::{Env, metrics};
use criterion::{Criterion, criterion_group, criterion_main};
use geoengine_datatypes::{
    primitives::{
        BandSelection, DateTime, RasterQueryRectangle, SpatialPartition2D, SpatialResolution,
        TimeInterval,
    },
    raster::GridSize,
    spatial_reference::SpatialReference,
};
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{
        ExecutionContext, InitializedRasterOperator, ProcessingGraphOperatorPath, TypedOperator,
        TypedRasterOperator,
    },
    source::{GdalSource, GdalSourceParameters},
    util::{
        AdaptToGrid, TargetGrid, WarpMethods, raster_stream_to_png::raster_stream_to_png_bytes,
    },
};
use geoengine_services::{
    api::model::datatypes::{DataProviderId, LayerId},
    processing_graphs::processing_graph::ProcessingGraph,
    queries::ogc_tile::{TileQuery, tile_on_grid},
};
use ogcapi_types::tiles::TileMatrixSetId;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use tracing::Instrument;

const TILE_SIZE: u32 = 256;
const STAGE_RUNS: u32 = 20;
const HALF_WORLD: f64 = 20_037_508.342_789_244;

/// The tile that `ogc_tiles` renders: an OGC API tile, or a WMS `GetMap` of a `WebMercatorQuad` tile
#[derive(Clone, Copy)]
enum Tile {
    Ogc {
        tile_matrix_set: &'static str,
        tile_matrix: u8,
        row: u32,
        col: u32,
    },
    Wms {
        zoom: u8,
        row: u32,
        col: u32,
    },
}

/// The tiles: Frankfurt at zoom 3 and at zoom 5 (the zoom of the load benches) of
/// `WebMercatorQuad`, the two finest tile matrices of the custom tile matrix sets
const TILES: [(&str, Tile); 7] = [
    ("WebMercatorQuad/z3", ogc("WebMercatorQuad", 3, 2, 4)),
    ("WebMercatorQuad/z5", ogc("WebMercatorQuad", 5, 10, 16)),
    ("Custom/finest", ogc("Custom", 2, 0, 4)),
    ("Custom/finest-1", ogc("Custom", 1, 0, 2)),
    (
        "CustomWebMercator/finest",
        ogc("CustomWebMercator", 2, 1, 2),
    ),
    (
        "CustomWebMercator/finest-1",
        ogc("CustomWebMercator", 1, 0, 1),
    ),
    (
        "wms/z5",
        Tile::Wms {
            zoom: 5,
            row: 10,
            col: 16,
        },
    ),
];

const fn ogc(tile_matrix_set: &'static str, tile_matrix: u8, row: u32, col: u32) -> Tile {
    Tile::Ogc {
        tile_matrix_set,
        tile_matrix,
        row,
        col,
    }
}

fn tile_matrix_set_id(name: &str) -> TileMatrixSetId {
    if name == "WebMercatorQuad" {
        TileMatrixSetId::WebMercatorQuad
    } else {
        TileMatrixSetId::Custom(name.to_owned())
    }
}

/// The time of the load benches
fn time() -> TimeInterval {
    TimeInterval::new_instant(DateTime::new_utc(2014, 4, 1, 12, 0, 0)).unwrap()
}

/// Renders `tile` and returns the PNG; if `describe`, prints the operator grid and the tile's pixels
#[allow(
    clippy::too_many_lines,
    reason = "the steps of the two paths side by side"
)]
async fn render(env: &Env, source: &TypedRasterOperator, tile: Tile, describe: bool) -> Vec<u8> {
    let exec_ctx: &dyn ExecutionContext = &env.exe_ctx;
    let (initialized, grid_bounds, (width, height)) = match tile {
        Tile::Ogc {
            tile_matrix_set,
            tile_matrix,
            row,
            col,
        } => {
            let query = TileQuery {
                data_connector_id: DataProviderId::from_u128(0),
                layer_id: LayerId("ndvi".to_owned()),
                time_interval: time(),
                tile_matrix_set_id: tile_matrix_set_id(tile_matrix_set),
                tile_matrix,
                tile_row: row,
                tile_col: col,
            };
            let graph = ProcessingGraph {
                operator: TypedOperator::Raster(source.clone()),
            };
            // nearest neighbour in both paths, so that they compare the same
            let tile = tile_on_grid(graph, exec_ctx, &query, |_| Ok((WarpMethods::NEAREST, ())))
                .instrument(tracing::debug_span!("initialize"))
                .await
                .unwrap();
            (
                tile.initialized_operator,
                tile.grid_bounds,
                (tile.width, tile.height),
            )
        }
        Tile::Wms { zoom, row, col } => {
            let size = 2. * HALF_WORLD / f64::from(1_u32 << zoom);
            let (x_min, y_max) = (
                -HALF_WORLD + f64::from(col) * size,
                HALF_WORLD - f64::from(row) * size,
            );
            let bounds =
                SpatialPartition2D::new((x_min, y_max).into(), (x_min + size, y_max - size).into())
                    .unwrap();
            let resolution =
                SpatialResolution::new(size / f64::from(TILE_SIZE), size / f64::from(TILE_SIZE))
                    .unwrap();
            let wrapped = async {
                let initialized = source
                    .clone()
                    .initialize(ProcessingGraphOperatorPath::initialize_root(), exec_ctx)
                    .await
                    .unwrap();
                AdaptToGrid::new_create_result_descriptor(source.clone(), initialized)
                    .adapt_to_grid(
                        TargetGrid {
                            spatial_reference: SpatialReference::web_mercator(),
                            resolution: Some(resolution),
                            origin: None,
                        },
                        WarpMethods::NEAREST,
                        exec_ctx,
                    )
                    .await
                    .unwrap()
            }
            .instrument(tracing::debug_span!("initialize"))
            .await;
            let grid_bounds = wrapped
                .result_descriptor
                .spatial_grid_descriptor()
                .tiling_grid_definition(exec_ctx.tiling_specification())
                .tiling_spatial_grid_definition()
                .spatial_bounds_to_compatible_spatial_grid(bounds)
                .grid_bounds();
            (
                wrapped.initialized_operator,
                grid_bounds,
                (TILE_SIZE, TILE_SIZE),
            )
        }
    };
    if describe {
        describe_grid(initialized.as_ref(), grid_bounds, width, height);
    }

    let query = RasterQueryRectangle::new(grid_bounds, time(), BandSelection::first());
    let mut query_ctx = env.new_query_ctx();
    query_ctx.new_raster_cache = None; // measure computation, not cache hits
    let (png, _) = call_on_generic_raster_processor!(
        initialized.query_processor().unwrap(),
        p => raster_stream_to_png_bytes(
            p,
            query,
            query_ctx,
            width,
            height,
            Some(time()),
            None,
            Box::pin(futures::future::pending()),
        )
        .instrument(tracing::debug_span!("query"))
        .await
    )
    .unwrap();
    png
}

fn describe_grid(
    initialized: &dyn InitializedRasterOperator,
    grid_bounds: geoengine_datatypes::raster::GridBoundingBox2D,
    width: u32,
    height: u32,
) {
    let descriptor = initialized.result_descriptor();
    let grid = descriptor.spatial_grid_descriptor();
    eprintln!(
        "  operator {} in {}: resolution {:?}, origin {:?}, grid {:?}",
        initialized.name(),
        descriptor.spatial_reference,
        grid.spatial_resolution(),
        grid.geo_transform().origin_coordinate,
        grid.as_parts().1,
    );
    eprintln!(
        "  tile pixels {grid_bounds:?} ({:?} y×x), encoded as {width}×{height}",
        grid_bounds.axis_size()
    );
}

/// CPU time of this process's threads by name, digits and trailing separators removed
fn cpu_by_thread_group() -> BTreeMap<String, Duration> {
    let mut groups = BTreeMap::<String, Duration>::new();
    for task in std::fs::read_dir("/proc/self/task").unwrap().flatten() {
        let (Ok(comm), Ok(schedstat)) = (
            std::fs::read_to_string(task.path().join("comm")),
            std::fs::read_to_string(task.path().join("schedstat")),
        ) else {
            continue; // the thread ended
        };
        let name = comm
            .trim()
            .trim_end_matches(|c: char| c.is_ascii_digit() || c == '-' || c == '_' || c == '#');
        let nanos: u64 = schedstat
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap();
        *groups.entry(name.to_owned()).or_default() += Duration::from_nanos(nanos);
    }
    groups
}

fn print_cpu(before: &BTreeMap<String, Duration>, after: &BTreeMap<String, Duration>, runs: u32) {
    eprintln!("  {:<40} {:>10}", "thread group", "CPU ms/tile");
    let mut total = Duration::ZERO;
    for (group, time) in after {
        // shortcut: threads that ended between the snapshots are missing; fine for pools that live as long as the bench
        let spent = time.saturating_sub(before.get(group).copied().unwrap_or_default());
        total += spent;
        if spent > Duration::ZERO {
            eprintln!(
                "  {group:<40} {:>10.2}",
                spent.as_secs_f64() * 1000. / f64::from(runs)
            );
        }
    }
    eprintln!(
        "  {:<40} {:>10.2}",
        "total",
        total.as_secs_f64() * 1000. / f64::from(runs)
    );
}

fn ogc_tiles(c: &mut Criterion) {
    let env = Env::new();
    let source = TypedRasterOperator::from(GdalSource {
        params: GdalSourceParameters::new(env.raster.clone()),
    });

    let mut group = c.benchmark_group("ogc_tiles");
    for (name, tile) in TILES {
        group.bench_function(name, |b| {
            b.to_async(&env.runtime).iter_custom(|iters| {
                let (env, source) = (&env, &source);
                async move {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let start = Instant::now();
                        std::hint::black_box(render(env, source, tile, false).await);
                        elapsed += start.elapsed();
                    }
                    elapsed
                }
            });
        });
        // opt-in: these runs are outside Criterion's closures, so a case filter would not skip them
        if !common::metrics_enabled() {
            continue;
        }
        eprintln!("ogc_tiles/{name}:");
        env.runtime.block_on(render(&env, &source, tile, true));
        let before = cpu_by_thread_group();
        metrics::record(&format!("ogc_tiles/{name}"), STAGE_RUNS, || {
            env.runtime.block_on(render(&env, &source, tile, false));
        });
        print_cpu(&before, &cpu_by_thread_group(), STAGE_RUNS);
    }
    group.finish();
}

criterion_group!(benches, ogc_tiles);
criterion_main!(benches);
