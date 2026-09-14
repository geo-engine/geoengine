#![allow(clippy::unwrap_used, unused_mut, reason = "okay in benchmarks")]

use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{BandSelection, RasterQueryRectangle, TimeInterval},
    raster::TilingSpecification,
    spatial_reference::SpatialReference,
};
use geoengine_operators::engine::SingleRasterOrVectorSource;
#[cfg(feature = "new-subquery-adapter")]
use geoengine_operators::engine::TileScheduler;
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

fn setup(
    runtime: &tokio::runtime::Runtime,
) -> (
    BoxRasterQueryProcessor<u8>,
    geoengine_operators::engine::MockQueryContext,
    RasterQueryRectangle,
) {
    let tiling = TilingSpecification::new([512, 512].into());
    let mut execution_context =
        MockExecutionContext::new_with_tiling_spec_and_tokio_handle(tiling, runtime.handle());
    execution_context.gdal_process_pool =
        geoengine_operators::source::gdal_worker_process::GdalProcessPool::new_with_tokio_handle(
            runtime.handle(),
            8,
            8,
            8,
            geoengine_operators::source::gdal_worker_process::WorkerConfig::default(),
        );

    let dataset = add_ndvi_dataset(&mut execution_context);
    let operator = RasterOperator::boxed(Reprojection {
        params: ReprojectionParams {
            target_spatial_reference: SpatialReference::web_mercator(),
            derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
        },
        sources: SingleRasterOrVectorSource {
            source: GdalSource {
                params: GdalSourceParameters::new(dataset),
            }
            .boxed()
            .into(),
        },
    });
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
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();

    #[cfg(not(feature = "new-subquery-adapter"))]
    {
        let (processor, query_context, query) = setup(&runtime);
        c.bench_function("subquery_gdal_old", |b| {
            b.to_async(&runtime)
                .iter(|| run(&processor, &query_context, query.clone()));
        });
    }

    #[cfg(feature = "new-subquery-adapter")]
    for factor in 1..=8 {
        let (processor, mut query_context, query) = setup(&runtime);
        query_context.tile_scheduler = TileScheduler::fixed(factor);
        c.bench_function(&format!("subquery_gdal_fanout_{factor}"), |b| {
            b.to_async(&runtime)
                .iter(|| run(&processor, &query_context, query.clone()));
        });
    }
}

criterion_group!(subquery_gdal, subquery_gdal_benchmark);
criterion_main!(subquery_gdal);
