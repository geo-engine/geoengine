#![allow(clippy::unwrap_used, reason = "okay in benchmarks")]

use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{BandSelection, RasterQueryRectangle, TimeInterval},
    raster::TilingSpecification,
    spatial_reference::SpatialReference,
};
use geoengine_operators::engine::SingleRasterOrVectorSource;
use geoengine_operators::engine::SubQueryParallelism;
use geoengine_operators::source::gdal_worker_process::{GdalProcessPool, WorkerConfig};
use geoengine_operators::{
    engine::{
        BoxRasterQueryProcessor, MockExecutionContext, QueryContext, RasterOperator,
        WorkflowOperatorPath,
    },
    processing::{DeriveOutRasterSpecsSource, Reprojection, ReprojectionParams},
    source::{GdalSource, GdalSourceParameters},
    util::{Result, gdal::add_ndvi_dataset},
};
use std::hint::black_box;
use std::sync::Arc;

fn reproject(source: Box<dyn RasterOperator>, target: SpatialReference) -> Box<dyn RasterOperator> {
    RasterOperator::boxed(Reprojection {
        params: ReprojectionParams {
            target_spatial_reference: target,
            derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
        },
        sources: SingleRasterOrVectorSource {
            source: source.into(),
        },
    })
}

/// `chained` nests two reprojections to measure that fan-out does not multiply per level.
fn setup(
    runtime: &tokio::runtime::Runtime,
    chained: bool,
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
    let source = GdalSource {
        params: GdalSourceParameters::new(dataset),
    }
    .boxed();
    let mut operator = reproject(source, SpatialReference::web_mercator());
    if chained {
        operator = reproject(operator, SpatialReference::epsg_4326());
    }
    let initialized = runtime
        .block_on(operator.initialize(WorkflowOperatorPath::initialize_root(), &execution_context))
        .unwrap();
    let processor = initialized.query_processor().unwrap().get_u8().unwrap();
    let query_context = execution_context.mock_query_context_test_default();
    let query_bounds = processor
        .result_descriptor()
        .spatial_grid_descriptor()
        .tiling_grid_definition(query_context.tiling_specification())
        .tiling_grid_bounds();
    let query = RasterQueryRectangle::new(
        query_bounds,
        TimeInterval::new_unchecked(1_396_303_200_000, 1_396_389_600_000),
        BandSelection::first(),
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
    assert!(result.len() > 8);
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

    for (chained, name) in [(false, "subquery_gdal"), (true, "subquery_gdal_chained")] {
        for factor in [1, 2, 4, 8, 16] {
            let (processor, mut query_context, query) =
                setup(&runtime, chained, gdal_process_pool.clone());
            query_context.subquery_parallelism = SubQueryParallelism::new(factor);
            c.bench_function(&format!("{name}_fanout_{factor}"), |b| {
                b.to_async(&runtime)
                    .iter(|| run(&processor, &query_context, query.clone()));
            });
        }
    }
}

criterion_group!(subquery_gdal, subquery_gdal_benchmark);
criterion_main!(subquery_gdal);
