//! Queries through the raster cache operator: cold, with the cache cleared before every run,
//! warm, with every tile cached, concurrent cold queries for the same tiles, and a cached stack
//! of rasters with different time steps. For comparison, the same extent without a cache
//! (`extent_uncached`) and through the old cache operator (`extent_old_cold`, `extent_old_warm`).
//!
//! The source is local, so the GDAL workers keep the file open: cold runs measure the cache
//! operator and its source queries, not remote reads.
//!
//! `cargo bench -p geoengine-operators --bench raster_cache`
#![allow(clippy::unwrap_used, reason = "okay in benchmarks")]

mod common;

use common::{Area, Env, stack};
use criterion::{Criterion, criterion_group, criterion_main};
use geoengine_operators::{
    cache::{
        cache_operator::InitializedCacheOperator,
        new_raster_cache::{NewRasterCacheEnum, RasterCacheOperator},
        shared_cache::SharedCache,
    },
    engine::InitializedRasterOperator,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const CACHE_BYTES: usize = 1 << 30;

/// Runs `concurrent` queries of the case at once, each as its own task, after clearing the cache, `iters` times, and
/// returns the time of the queries only: for `iter_custom`, so that clearing is not measured.
fn cold(
    env: &Env,
    clear: impl Fn(),
    case: &common::Case,
    concurrent: usize,
    iters: u64,
) -> Duration {
    let mut total = Duration::ZERO;
    for _ in 0..iters {
        clear();
        let start = Instant::now();
        env.runtime.block_on(env.run_concurrent(case, concurrent));
        total += start.elapsed();
    }
    total
}

fn raster_cache(c: &mut Criterion) {
    let mut env = Env::new();
    let cache = Arc::new(NewRasterCacheEnum::new_lru(CACHE_BYTES));
    // before any case shares the query context
    let old_cache = Arc::new(SharedCache::new(CACHE_BYTES >> 20, 0.1).unwrap());
    let query_ctx = Arc::get_mut(&mut env.query_ctx).unwrap();
    query_ctx.new_raster_cache = Some(cache.clone());
    query_ctx.cache = Some(old_cache.clone());
    let clear = || cache.clear();
    let clear_old = || env.runtime.block_on(old_cache.clear());

    let cached = |initialized: Box<dyn InitializedRasterOperator>| {
        RasterCacheOperator::wrap_operator(initialized).boxed()
    };
    let extent = env.wrapped_case(env.raster(), Area::Extent, env.one_step(), cached);
    let uncached = env.case(env.raster(), Area::Extent, env.one_step());
    let old = env.wrapped_case(env.raster(), Area::Extent, env.one_step(), |initialized| {
        Box::new(InitializedCacheOperator::new(initialized))
    });
    let series = env.wrapped_case(env.raster(), Area::Extent, env.six_steps(), cached);
    let stacked = env.wrapped_case(
        stack(vec![env.raster(), env.six_step_mean(env.raster())]),
        Area::Extent,
        env.one_step(),
        cached,
    );

    let mut group = common::group(c, "raster_cache");
    extent.print("extent_cold");
    group.bench_function("extent_cold", |b| {
        b.iter_custom(|iters| cold(&env, clear, &extent, 1, iters));
    });
    env.bench(&mut group, "extent_warm", &extent);
    env.bench(&mut group, "extent_uncached", &uncached);
    old.print("extent_old_cold");
    group.bench_function("extent_old_cold", |b| {
        b.iter_custom(|iters| cold(&env, clear_old, &old, 1, iters));
    });
    env.bench(&mut group, "extent_old_warm", &old);
    // the queries must share the source reads, not each read every tile
    group.bench_function("extent_cold_concurrent8", |b| {
        b.iter_custom(|iters| cold(&env, clear, &extent, 8, iters));
    });
    // Local only (`just backend bench`), skipped in CI (`GEOENGINE_BENCH_CI`): 70 ms and more per
    // iteration, beyond the ~60 ms budget of a CI case. `extent_cold` covers cold runs in CI.
    if !common::ci() {
        series.print("series_6_months_cold");
        group.bench_function("series_6_months_cold", |b| {
            b.iter_custom(|iters| cold(&env, clear, &series, 1, iters));
        });
        stacked.print("stack_month_and_half_year_cold");
        group.bench_function("stack_month_and_half_year_cold", |b| {
            b.iter_custom(|iters| cold(&env, clear, &stacked, 1, iters));
        });
    }
    group.finish();
}

criterion_group!(name = benches; config = common::criterion(); targets = raster_cache);
criterion_main!(benches);
