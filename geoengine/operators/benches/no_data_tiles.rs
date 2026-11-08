//! Benchmarks queries with many tiles without data. It is not part of the CI benchmarks.
//!
//! The NDVI cases use local test data. Pools, runtime and GDAL workers follow the server settings
//! and the operators are wrapped as in the server, see `common`.
//! The Sentinel-2 cases run if `NO_DATA_BENCH_COG` is set to a GDAL path of a Sentinel-2 band COG,
//! e.g. `/vsicurl/http://127.0.0.1:8099/B04.tif`.
//! If `NO_DATA_BENCH_STATS` points to a file with one `<status> <bytes>` line per HTTP response,
//! the HTTP requests and bytes of the first run are reported, too.
//! `NO_DATA_BENCH_RUNS` sets the number of Sentinel-2 runs (default 5).
//!
//! The first run of a case is cold: its dataset was not opened before.
//! Later runs are warm, because the GDAL workers keep the dataset and its block cache.
//!
//! Run with `cargo bench -p geoengine-operators --bench no_data_tiles`.

#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

mod common;

use common::{Area, Env, count_tiles, downsample, expression};
use geoengine_datatypes::dataset::NamedData;
use geoengine_datatypes::primitives::{DateTime, TimeInterval};
use geoengine_operators::{
    engine::{RasterOperator, TypedRasterOperator},
    source::{GdalMetaDataStatic, GdalSource, GdalSourceParameters},
    util::gdal::{
        create_ndvi_meta_data, gdal_parameters_from_dataset, raster_descriptor_from_dataset,
    },
};
use std::{path::Path, time::Instant};

const NDVI_RUNS: usize = 20;
const DEFAULT_COG_RUNS: usize = 5;

fn source(data: NamedData) -> TypedRasterOperator {
    GdalSource {
        params: GdalSourceParameters::new(data),
    }
    .typed()
}

/// One benchmark case: a processing graph and the query area.
struct Case {
    name: &'static str,
    operator: TypedRasterOperator,
    area: Area,
    time: TimeInterval,
    runs: usize,
}

/// HTTP responses and bytes logged by the server so far.
fn http_stats() -> Option<(usize, usize)> {
    let path = std::env::var("NO_DATA_BENCH_STATS").ok()?;
    let log = std::fs::read_to_string(path).ok()?;
    Some(log.lines().fold((0, 0), |(requests, bytes), line| {
        let line_bytes = line
            .split_whitespace()
            .nth(1)
            .and_then(|b| b.parse::<usize>().ok())
            .unwrap_or(0);
        (requests + 1, bytes + line_bytes)
    }))
}

#[allow(clippy::cast_precision_loss)]
fn run_case(env: &Env, case: Case) {
    // cold: the first run opens the dataset
    let prepared = env.cold_case(case.operator, case.area, case.time);

    let mut durations = Vec::with_capacity(case.runs);
    let mut tiles = (0, 0);
    let mut cold_http = None;

    for run in 0..case.runs {
        let stats_before = http_stats();
        let start = Instant::now();

        tiles = env.runtime.block_on(count_tiles(
            &prepared.processor,
            prepared.query.clone(),
            env.query_ctx.as_ref(),
        ));

        durations.push(start.elapsed());
        if run == 0
            && let (Some(before), Some(after)) = (stats_before, http_stats())
        {
            cold_http = Some((after.0 - before.0, after.1 - before.1));
        }
    }

    let cold = durations.remove(0);
    durations.sort();
    let warm = durations
        .get(durations.len() / 2)
        .map_or("-".to_string(), |d| format!("{:.3}", d.as_secs_f64()));
    let (requests, megabytes) = cold_http.map_or(("-".to_string(), "-".to_string()), |(r, b)| {
        (r.to_string(), format!("{:.1}", b as f64 / 1_000_000.))
    });

    println!(
        "| {} | {:.3} | {requests} | {megabytes} | {warm} ({}) | {} | {} |",
        case.name,
        cold.as_secs_f64(),
        durations.len(),
        tiles.0,
        tiles.1,
    );
}

fn cog_meta_data(path: &str) -> GdalMetaDataStatic {
    let dataset = gdal::Dataset::open(path).unwrap();
    GdalMetaDataStatic {
        time: None,
        params: gdal_parameters_from_dataset(&dataset, 1, Path::new(path), None, None).unwrap(),
        result_descriptor: raster_descriptor_from_dataset(&dataset, 1).unwrap(),
        cache_ttl: None,
    }
}

fn main() {
    // the pools, runtime and GDAL workers of the server settings, see `common`
    let mut env = Env::new();

    // the ocean has the value 255, so declaring it as no-data makes all ocean tiles no-data
    let mut ocean_meta_data = create_ndvi_meta_data();
    ocean_meta_data.params.no_data_value = Some(255.);
    let ndvi_ocean_no_data = env.add_data("ndvi_ocean_no_data", Box::new(ocean_meta_data));
    let ndvi_time = env.six_steps();

    let mut cases = vec![
        Case {
            name: "ndvi_ocean_as_no_data_expression",
            operator: expression(source(ndvi_ocean_no_data), "A / 255", false),
            area: Area::Extent,
            time: ndvi_time,
            runs: NDVI_RUNS,
        },
        Case {
            name: "ndvi_3x_width_downsample",
            operator: downsample(env.raster(), 2.),
            area: Area::ExtentTimes(3),
            time: ndvi_time,
            runs: NDVI_RUNS,
        },
    ];

    if let Ok(cog) = std::env::var("NO_DATA_BENCH_COG") {
        // one dataset per case, so that the first run of each case is cold;
        // for remote files, the query string makes GDAL open a separate dataset for the same file
        let mut s2 = |id: u128| {
            env.add_data(
                &format!("s2_{id}"),
                Box::new(cog_meta_data(&if cog.starts_with("/vsicurl/") {
                    format!("{cog}?case={id}")
                } else {
                    cog.clone()
                })),
            )
        };
        let s2_reflectance = s2(2);
        let s2_downsample = s2(3);
        let cog_time = TimeInterval::new_instant(DateTime::new_utc(2024, 6, 30, 0, 0, 0)).unwrap();
        let cog_runs = std::env::var("NO_DATA_BENCH_RUNS")
            .ok()
            .and_then(|runs| runs.parse().ok())
            .unwrap_or(DEFAULT_COG_RUNS);

        cases.push(Case {
            name: "s2_scene_reflectance",
            operator: expression(source(s2_reflectance), "A / 10000", false),
            area: Area::Extent,
            time: cog_time,
            runs: cog_runs,
        });
        cases.push(Case {
            name: "s2_3x3_scenes_downsample_60m",
            operator: downsample(source(s2_downsample), 6.),
            area: Area::ExtentTimes(3),
            time: cog_time,
            runs: cog_runs,
        });
    }

    println!(
        "| case | cold s | cold HTTP requests | cold MB | warm median s (runs) | tiles | empty tiles |"
    );
    println!("|---|---|---|---|---|---|---|");
    for case in cases {
        run_case(&env, case);
    }
}
