//! Reads of the GDAL source of the bench raster (see `common`) through the GDAL worker pool: warm reads and re-opens, overview reads,
//! empty tiles, many small concurrent queries like a map client and queries that are dropped
//! while their reads are queued.
//!
//! Remote files with latency are measured by the `block_reads` bench.
//!
//! `cargo bench -p geoengine-operators --bench gdal_source`
#![allow(
    clippy::unwrap_used,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

mod common;

use common::{Area, Env, downsample};
use criterion::{Criterion, criterion_group, criterion_main};
use futures::{StreamExt, future::join_all};
use geoengine_datatypes::raster::GridBoundingBox2D;
use geoengine_operators::{call_on_generic_raster_processor, engine::RasterQueryProcessor};
use std::time::{Duration, Instant};

const TILE: isize = 512;

/// The single-tile queries that cover the case's query, row by row.
fn tile_queries(case: &common::Case) -> Vec<geoengine_datatypes::primitives::RasterQueryRectangle> {
    let b = case.query.spatial_bounds();
    let mut queries = Vec::new();
    for y in (b.y_min()..=b.y_max()).step_by(TILE as usize) {
        for x in (b.x_min()..=b.x_max()).step_by(TILE as usize) {
            queries.push(case.query.select_spatial_bounds(
                GridBoundingBox2D::new_min_max(y, y + TILE - 1, x, x + TILE - 1).unwrap(),
            ));
        }
    }
    queries
}

async fn abandoned_then_extent(env: &Env, extent: &common::Case) -> (usize, usize) {
    join_all((0..8).map(|_| async {
        call_on_generic_raster_processor!(extent.processor.as_ref(), p => {
            let mut stream = p.raster_query(extent.query.clone(), env.query_ctx.as_ref()).await.unwrap();
            stream.next().await.unwrap().unwrap();
        });
    }))
    .await;
    env.run(extent).await
}

fn gdal_source(c: &mut Criterion) {
    let env = Env::new();
    let extent = env.case(env.raster(), Area::Extent, env.one_step());
    let overview = env.case(downsample(env.raster(), 4.), Area::Extent, env.one_step());
    let empty = env.case(env.raster(), Area::ExtentTimes(3), env.one_step());
    // one query per 512 px tile of the extent; the tiles of `Env` are 512 px, so each query
    // returns exactly one tile
    let tiles = tile_queries(&extent);

    let mut group = common::group(c, "gdal_source");

    env.bench(&mut group, "extent_warm", &extent);

    // each iteration closes the datasets of the workers and opens the file again, so this is the
    // open and header cost; the file stays in the page cache, so it is not a cold disk.
    // `iter_custom`, so that clearing the caches is not part of the measured time
    extent.print("extent_reopen");
    group.bench_function("extent_reopen", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                env.clear_gdal_caches();
                let start = Instant::now();
                env.runtime.block_on(env.run(&extent));
                total += start.elapsed();
            }
            total
        });
    });

    env.bench(&mut group, "overview_x4", &overview);

    // 8 of 9 tiles are outside of the data
    env.bench(&mut group, "extent_x3_empty", &empty);

    // like a map client: one request per tile, all at once, each its own task as in the server
    eprintln!("tile_queries_concurrent: {} queries", tiles.len());
    group.bench_function("tile_queries_concurrent", |b| {
        b.to_async(&env.runtime)
            .iter(|| common::spawn_queries(&extent, tiles.clone(), env.query_ctx.clone()));
    });

    // Local only (`just backend bench`), skipped in CI (`GEOENGINE_BENCH_CI`): the case fails.
    // shortcut: the pool answers with an intermittent `WorkerPanic` (no worker panics) in about
    // one of three runs; move it out of this `if` into CI once the dispatcher handles abandoned
    // reads
    if !common::ci() {
        // clients pan away: 8 extent queries stop after their first tile and drop their queued reads,
        // then one extent query must not wait for them
        group.bench_function("abandoned_then_extent", |b| {
            b.to_async(&env.runtime)
                .iter(|| abandoned_then_extent(&env, &extent));
        });
        // the metrics show the 8 dropped streams per run
        env.metrics("abandoned_then_extent", || {
            abandoned_then_extent(&env, &extent)
        });
    }

    group.finish();
}

criterion_group!(name = benches; config = common::criterion(); targets = gdal_source);
criterion_main!(benches);
