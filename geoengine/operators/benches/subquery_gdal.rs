#![allow(clippy::unwrap_used, reason = "okay in benchmarks")]

use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{BandSelection, Coordinate2D, RasterQueryRectangle, TimeInterval},
    raster::{GridBoundingBox2D, GridSize, RenameBands, TilingSpecification},
    spatial_reference::SpatialReference,
};
use geoengine_operators::engine::SubQueryParallelism;
use geoengine_operators::engine::{MultipleRasterSources, SingleRasterOrVectorSource};
use geoengine_operators::source::gdal_worker_process::{GdalProcessPool, WorkerConfig};
use geoengine_operators::{
    engine::{
        BoxRasterQueryProcessor, MockExecutionContext, QueryContext, RasterOperator,
        TypedRasterOperator, WorkflowOperatorPath,
    },
    processing::{
        DeriveOutRasterSpecsSource, RasterStacker, RasterStackerParams, Reprojection,
        ReprojectionParams,
    },
    source::{GdalSource, GdalSourceParameters},
    util::{Result, gdal::add_ndvi_dataset},
};
use std::hint::black_box;
use std::sync::Arc;

fn reproject(source: TypedRasterOperator, target: SpatialReference) -> TypedRasterOperator {
    Reprojection {
        params: ReprojectionParams {
            target_spatial_reference: target,
            derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
        },
        sources: SingleRasterOrVectorSource {
            source: source.into(),
        },
    }
    .typed()
}

/// the day of the base cases, which spans the files of March and April 2014.
fn one_day() -> TimeInterval {
    TimeInterval::new_unchecked(1_396_303_200_000, 1_396_389_600_000)
}
/// 2014-01-01 to 2014-07-01, the six monthly NDVI files.
fn six_months() -> TimeInterval {
    TimeInterval::new_unchecked(1_388_534_400_000, 1_404_172_800_000)
}

#[derive(Clone, Copy)]
struct Case {
    /// nests two reprojections to measure that fan-out does not multiply per level
    chained: bool,
    /// stacks the NDVI this many times, so that sub-queries of one tile differ only by band
    bands: u32,
    time: TimeInterval,
    /// queries only the output tile that contains 10°E 50°N instead of the whole extent
    single_tile: bool,
}

fn setup(
    runtime: &tokio::runtime::Runtime,
    case: Case,
    gdal_process_pool: Arc<GdalProcessPool>,
) -> (
    BoxRasterQueryProcessor<u8>,
    geoengine_operators::engine::MockQueryContext,
    RasterQueryRectangle,
) {
    let tiling = TilingSpecification::new([512, 512].into());
    let mut execution_context =
        MockExecutionContext::new_with_tiling_spec_and_tokio_handle(tiling, runtime.handle());
    execution_context.gdal_process_pool = gdal_process_pool;

    let dataset = add_ndvi_dataset(&mut execution_context);
    let ndvi = || {
        GdalSource {
            params: GdalSourceParameters::new(dataset.clone()),
        }
        .typed()
    };
    let source = if case.bands == 1 {
        ndvi()
    } else {
        RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: (0..case.bands).map(|_| ndvi()).collect(),
            },
        }
        .typed()
    };
    let mut operator = reproject(source, SpatialReference::web_mercator());
    if case.chained {
        operator = reproject(operator, SpatialReference::epsg_4326());
    }
    let initialized = runtime
        .block_on(operator.initialize(WorkflowOperatorPath::initialize_root(), &execution_context))
        .unwrap();
    let processor = initialized.query_processor().unwrap().get_u8().unwrap();
    let query_context = execution_context.mock_query_context_test_default();
    let tiling_grid = processor
        .result_descriptor()
        .spatial_grid_descriptor()
        .tiling_grid_definition(query_context.tiling_specification());
    let query_bounds = if case.single_tile {
        let strategy = tiling_grid.generate_data_tiling_strategy();
        let pixel = tiling_grid
            .tiling_geo_transform()
            .coordinate_to_grid_idx_2d(Coordinate2D::new(1_113_195., 6_446_276.));
        let [y, x] = strategy
            .tile_idx_to_global_pixel_idx(strategy.pixel_idx_to_tile_idx(pixel))
            .0;
        let [height, width] = [
            tiling.tile_size_in_pixels.axis_size_y(),
            tiling.tile_size_in_pixels.axis_size_x(),
        ];
        GridBoundingBox2D::new([y, x], [y + height as isize - 1, x + width as isize - 1]).unwrap()
    } else {
        tiling_grid.tiling_grid_bounds()
    };
    let query = RasterQueryRectangle::new(
        query_bounds,
        case.time,
        BandSelection::new((0..case.bands).collect()).unwrap(),
    );

    (processor, query_context, query)
}

async fn run(
    processor: &BoxRasterQueryProcessor<u8>,
    query_context: &geoengine_operators::engine::MockQueryContext,
    query: RasterQueryRectangle,
) {
    let result = processor
        .raster_query(query, query_context)
        .await
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;
    assert_ne!(result.len(), 0);
    black_box(result);
}

fn subquery_gdal_benchmark(c: &mut Criterion) {
    // `SUBQUERY_BENCH_TOKIO_WORKERS=1` comes close to the server, whose actix workers each run
    // a single-threaded runtime, so all tasks of one request share one thread
    let runtime = match std::env::var("SUBQUERY_BENCH_TOKIO_WORKERS") {
        Ok(workers) => tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers.parse().unwrap())
            .enable_all()
            .build()
            .unwrap(),
        Err(_) => tokio::runtime::Runtime::new().unwrap(),
    };
    let _guard = runtime.enter();
    // one pool for all cases: pools are not shut down, so one per case would pile up workers.
    // `SUBQUERY_BENCH_GDAL_PROCESSES` shows whether the fan-out saturates at the pool size.
    let gdal_processes =
        std::env::var("SUBQUERY_BENCH_GDAL_PROCESSES").map_or(8, |n| n.parse().unwrap());
    let gdal_process_pool = GdalProcessPool::new_with_tokio_handle(
        runtime.handle(),
        gdal_processes,
        gdal_processes,
        gdal_processes,
        WorkerConfig::default(),
    );

    let base = Case {
        chained: false,
        bands: 1,
        time: one_day(),
        single_tile: false,
    };
    for (case, name) in [
        (base, "subquery_gdal"),
        (
            Case {
                chained: true,
                ..base
            },
            "subquery_gdal_chained",
        ),
    ] {
        for factor in [1, 2, 4, 8, 16] {
            let (processor, mut query_context, query) =
                setup(&runtime, case, gdal_process_pool.clone());
            query_context.subquery_parallelism = SubQueryParallelism::new(factor);
            c.bench_function(&format!("{name}_fanout_{factor}"), |b| {
                b.to_async(&runtime)
                    .iter(|| run(&processor, &query_context, query.clone()));
            });
        }
    }

    // sub-queries that share a tile across bands or time steps; each iteration takes seconds
    let mut group = c.benchmark_group("subquery_gdal_shared_tiles");
    group.sample_size(20);
    for (case, name) in [
        (Case { bands: 3, ..base }, "bands3"),
        (
            Case {
                time: six_months(),
                ..base
            },
            "time6",
        ),
        (
            Case {
                time: six_months(),
                single_tile: true,
                ..base
            },
            "tile_time6",
        ),
    ] {
        for factor in [1, 4] {
            let (processor, mut query_context, query) =
                setup(&runtime, case, gdal_process_pool.clone());
            query_context.subquery_parallelism = SubQueryParallelism::new(factor);
            group.bench_function(format!("{name}_fanout_{factor}"), |b| {
                b.to_async(&runtime)
                    .iter(|| run(&processor, &query_context, query.clone()));
            });
        }
    }
    group.finish();
}

criterion_group!(subquery_gdal, subquery_gdal_benchmark);
criterion_main!(subquery_gdal);
