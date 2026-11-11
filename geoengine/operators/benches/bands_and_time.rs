//! Bands and time steps: stacking rasters as bands, reading a time series, a temporal aggregation
//! and a stack of rasters with different time steps, where the stacker must return only the
//! steps of the query.
//!
//! `cargo bench -p geoengine-operators --bench bands_and_time`
#![allow(clippy::unwrap_used, reason = "okay in benchmarks")]

mod common;

use common::{Area, Env, stack};
use criterion::{Criterion, criterion_group, criterion_main};

fn bands_and_time(c: &mut Criterion) {
    let env = Env::new();
    let cases = [
        (
            "stack3_extent",
            env.case(
                stack(vec![env.raster(), env.raster(), env.raster()]),
                Area::Extent,
                env.one_step(),
            ),
        ),
        (
            "series_6_months",
            env.case(env.raster(), Area::Extent, env.six_steps()),
        ),
        (
            "mean_6_months",
            env.case(
                env.six_step_mean(env.raster()),
                Area::Extent,
                env.six_steps(),
            ),
        ),
        // the half-year steps of the mean split at the monthly steps of the series
        (
            "stack_month_and_half_year",
            env.case(
                stack(vec![env.raster(), env.six_step_mean(env.raster())]),
                Area::Extent,
                env.one_step(),
            ),
        ),
    ];

    let mut group = common::group(c, "bands_and_time");
    for (name, case) in &cases {
        env.bench(&mut group, name, case);
    }
    group.finish();
}

criterion_group!(name = benches; config = common::criterion(); targets = bands_and_time);
criterion_main!(benches);
