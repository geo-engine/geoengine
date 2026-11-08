//! Bands and time steps: stacking rasters as bands, reading a time series, a temporal aggregation
//! and a stack of rasters with different time steps, where the stacker must return only the
//! steps of the query.
//!
//! `cargo bench -p geoengine-operators --bench bands_and_time`
#![allow(clippy::unwrap_used, reason = "okay in benchmarks")]

mod common;

use common::{Area, Env, one_month, six_months, stack};
use criterion::{Criterion, criterion_group, criterion_main};
use geoengine_datatypes::primitives::{TimeGranularity, TimeStep};
use geoengine_operators::{
    engine::{RasterOperator, SingleRasterSource, TypedRasterOperator},
    processing::{Aggregation, TemporalRasterAggregation, TemporalRasterAggregationParameters},
};

fn six_month_mean(raster: TypedRasterOperator) -> TypedRasterOperator {
    TemporalRasterAggregation {
        params: TemporalRasterAggregationParameters {
            aggregation: Aggregation::Mean {
                ignore_no_data: true,
            },
            window: TimeStep {
                granularity: TimeGranularity::Months,
                step: 6,
            },
            window_reference: None,
            output_type: None,
        },
        sources: SingleRasterSource { raster },
    }
    .typed()
}

fn bands_and_time(c: &mut Criterion) {
    let env = Env::new();
    let cases = [
        (
            "stack3_extent",
            env.case(
                stack(vec![env.ndvi(), env.ndvi(), env.ndvi()]),
                Area::Extent,
                one_month(),
            ),
        ),
        (
            "series_6_months",
            env.case(env.ndvi(), Area::Extent, six_months()),
        ),
        (
            "mean_6_months",
            env.case(six_month_mean(env.ndvi()), Area::Extent, six_months()),
        ),
        // the half-year steps of the mean split at the monthly steps of the series
        (
            "stack_month_and_half_year",
            env.case(
                stack(vec![env.ndvi(), six_month_mean(env.ndvi())]),
                Area::Extent,
                one_month(),
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
