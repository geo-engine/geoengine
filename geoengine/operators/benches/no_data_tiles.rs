//! Benchmarks queries with many tiles without data. It is not part of the CI benchmarks.
//!
//! The NDVI cases use local test data.
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

use futures::StreamExt;
use geoengine_datatypes::{
    dataset::{DataId, DatasetId, NamedData},
    primitives::{BandSelection, DateTime, RasterQueryRectangle, TimeInterval},
    raster::{GridBoundingBox2D, RasterDataType, TilingSpecification},
    util::test::TestDefault,
};
use geoengine_operators::engine::TypedRasterOperator;
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{
        MockExecutionContext, QueryContext, RasterOperator, RasterQueryProcessor,
        SingleRasterSource, WorkflowOperatorPath,
    },
    processing::{
        Downsampling, DownsamplingMethod, DownsamplingParams, DownsamplingResolution, Expression,
        ExpressionParams, Fraction,
    },
    source::{
        GdalMetaDataStatic, GdalProcessPool, GdalSource, GdalSourceParameters,
        gdal_worker_process::WorkerConfig,
    },
    util::gdal::{
        add_ndvi_dataset, create_ndvi_meta_data, gdal_parameters_from_dataset,
        raster_descriptor_from_dataset,
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

fn downsampling(raster: TypedRasterOperator, factor: f64) -> TypedRasterOperator {
    Downsampling {
        params: DownsamplingParams {
            sampling_method: DownsamplingMethod::NearestNeighbor,
            output_resolution: DownsamplingResolution::Fraction(Fraction {
                x: factor,
                y: factor,
            }),
            output_origin_reference: None,
        },
        sources: SingleRasterSource { raster },
    }
    .typed()
}

fn expression(raster: TypedRasterOperator, expression: &str) -> TypedRasterOperator {
    Expression {
        params: ExpressionParams {
            expression: expression.to_string(),
            output_type: RasterDataType::F32,
            output_band: None,
            map_no_data: false,
        },
        sources: SingleRasterSource { raster },
    }
    .typed()
}

/// One benchmark case: a workflow and the query area in units of the data extent.
struct Case {
    name: &'static str,
    operator: TypedRasterOperator,
    /// 1 queries the data extent, 3 queries a 3×3 area with the data in the center.
    extent_factor: isize,
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

/// Returns the number of all tiles and of empty tiles.
async fn count_tiles<P: RasterQueryProcessor + ?Sized>(
    processor: &P,
    query: RasterQueryRectangle,
    query_ctx: &dyn QueryContext,
) -> (usize, usize) {
    processor
        .raster_query(query, query_ctx)
        .await
        .unwrap()
        .fold((0, 0), |(all, empty), tile| async move {
            (all + 1, empty + usize::from(tile.unwrap().is_empty()))
        })
        .await
}

#[allow(clippy::cast_precision_loss)]
async fn run_case(case: Case, exe_ctx: &MockExecutionContext, query_ctx: &dyn QueryContext) {
    let initialized = case
        .operator
        .initialize(WorkflowOperatorPath::initialize_root(), exe_ctx)
        .await
        .unwrap();
    let data = initialized
        .result_descriptor()
        .tiling_grid_definition(exe_ctx.tiling_specification)
        .tiling_spatial_grid_definition()
        .grid_bounds();
    let processor = initialized.query_processor().unwrap();

    let margin_x = (case.extent_factor - 1) / 2 * (data.x_max() - data.x_min() + 1);
    let margin_y = (case.extent_factor - 1) / 2 * (data.y_max() - data.y_min() + 1);
    let query = RasterQueryRectangle::new(
        GridBoundingBox2D::new_min_max(
            data.y_min() - margin_y,
            data.y_max() + margin_y,
            data.x_min() - margin_x,
            data.x_max() + margin_x,
        )
        .unwrap(),
        case.time,
        BandSelection::first(),
    );

    let mut durations = Vec::with_capacity(case.runs);
    let mut tiles = (0, 0);
    let mut cold_http = None;

    for run in 0..case.runs {
        let stats_before = http_stats();
        let start = Instant::now();

        tiles = call_on_generic_raster_processor!(&processor, p => count_tiles(p.as_ref(), query.clone(), query_ctx).await);

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
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();

    let tiling_spec = TilingSpecification::test_default();
    let mut exe_ctx =
        MockExecutionContext::new_with_tiling_spec_and_tokio_handle(tiling_spec, runtime.handle());
    // like the server default: up to 8 workers read one dataset in parallel
    exe_ctx.gdal_process_pool = GdalProcessPool::new_with_tokio_handle(
        runtime.handle(),
        16,
        16,
        8,
        WorkerConfig::default(),
    );
    let query_ctx = exe_ctx.mock_query_context_test_default();

    let ndvi = add_ndvi_dataset(&mut exe_ctx);

    // the ocean has the value 255, so declaring it as no-data makes all ocean tiles no-data
    let mut ocean_meta_data = create_ndvi_meta_data();
    ocean_meta_data.params.no_data_value = Some(255.);
    let ndvi_ocean_no_data = NamedData::with_system_name("ndvi_ocean_no_data");
    exe_ctx.add_meta_data(
        DataId::from(DatasetId::from_u128(1)),
        ndvi_ocean_no_data.clone(),
        Box::new(ocean_meta_data),
    );

    let ndvi_time = TimeInterval::new(
        DateTime::new_utc(2014, 1, 1, 0, 0, 0),
        DateTime::new_utc(2014, 7, 1, 0, 0, 0),
    )
    .unwrap();

    let mut cases = vec![
        Case {
            name: "ndvi_ocean_as_no_data_expression",
            operator: expression(source(ndvi_ocean_no_data), "A / 255"),
            extent_factor: 1,
            time: ndvi_time,
            runs: NDVI_RUNS,
        },
        Case {
            name: "ndvi_3x_width_downsample",
            operator: downsampling(source(ndvi), 2.),
            extent_factor: 3,
            time: ndvi_time,
            runs: NDVI_RUNS,
        },
    ];

    if let Ok(cog) = std::env::var("NO_DATA_BENCH_COG") {
        // one dataset per case, so that the first run of each case is cold;
        // for remote files, the query string makes GDAL open a separate dataset for the same file
        let mut s2 = |id: u128| {
            let name = NamedData::with_system_name(format!("s2_{id}"));
            exe_ctx.add_meta_data(
                DataId::from(DatasetId::from_u128(id)),
                name.clone(),
                Box::new(cog_meta_data(&if cog.starts_with("/vsicurl/") {
                    format!("{cog}?case={id}")
                } else {
                    cog.clone()
                })),
            );
            name
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
            operator: expression(source(s2_reflectance), "A / 10000"),
            extent_factor: 1,
            time: cog_time,
            runs: cog_runs,
        });
        cases.push(Case {
            name: "s2_3x3_scenes_downsample_60m",
            operator: downsampling(source(s2_downsample), 6.),
            extent_factor: 3,
            time: cog_time,
            runs: cog_runs,
        });
    }

    println!(
        "| case | cold s | cold HTTP requests | cold MB | warm median s (runs) | tiles | empty tiles |"
    );
    println!("|---|---|---|---|---|---|---|");
    for case in cases {
        runtime.block_on(run_case(case, &exe_ctx, &query_ctx));
    }
}
