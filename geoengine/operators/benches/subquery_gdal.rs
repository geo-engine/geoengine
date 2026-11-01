#![allow(clippy::unwrap_used, reason = "okay in benchmarks")]

use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{
        BandSelection, Coordinate2D, RasterQueryRectangle, SpatialPartition2D, TimeInterval,
    },
    raster::TilingSpecification,
    spatial_reference::SpatialReference,
};
use geoengine_operators::{
    engine::{
        BoxRasterQueryProcessor, MockExecutionContext, MockQueryContext,
        ProcessingGraphOperatorPath, QueryContext, RasterOperator, SingleRasterOrVectorSource,
        SingleRasterSource, SubQueryParallelism, TypedRasterOperator,
    },
    processing::{
        DeriveOutRasterSpecsSource, Downsampling, DownsamplingMethod, DownsamplingParams,
        DownsamplingResolution, Fraction, Reprojection, ReprojectionParams,
    },
    source::{
        GdalProcessPool, GdalSource, GdalSourceParameters, gdal_worker_process::WorkerConfig,
    },
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

fn mercator(source: TypedRasterOperator) -> TypedRasterOperator {
    reproject(source, SpatialReference::web_mercator())
}

struct Case {
    name: &'static str,
    operator: fn(TypedRasterOperator) -> TypedRasterOperator,
    /// `None` queries the whole result.
    area: Option<SpatialPartition2D>,
}

fn setup(
    runtime: &tokio::runtime::Runtime,
    case: &Case,
    gdal_process_pool: Arc<GdalProcessPool>,
) -> (
    BoxRasterQueryProcessor<u8>,
    MockQueryContext,
    RasterQueryRectangle,
) {
    let tiling = TilingSpecification::new([512, 512].into());
    let mut execution_context =
        MockExecutionContext::new_with_tiling_spec_and_tokio_handle(tiling, runtime.handle());
    execution_context.gdal_process_pool = gdal_process_pool;

    let source = GdalSource {
        params: GdalSourceParameters::new(add_ndvi_dataset(&mut execution_context)),
    }
    .typed();
    let initialized = runtime
        .block_on((case.operator)(source).initialize(
            ProcessingGraphOperatorPath::initialize_root(),
            &execution_context,
        ))
        .unwrap();
    let processor = initialized.query_processor().unwrap().get_u8().unwrap();
    let query_context = execution_context.mock_query_context_test_default();
    let grid = processor
        .result_descriptor()
        .spatial_grid_descriptor()
        .tiling_grid_definition(query_context.tiling_specification());
    let query_bounds = case.area.map_or_else(
        || grid.tiling_grid_bounds(),
        |area| grid.tiling_geo_transform().spatial_to_grid_bounds(&area),
    );
    let query = RasterQueryRectangle::new(
        query_bounds,
        TimeInterval::new_unchecked(1_396_303_200_000, 1_396_389_600_000),
        BandSelection::first(),
    );

    (processor, query_context, query)
}

async fn run(
    processor: &BoxRasterQueryProcessor<u8>,
    query_context: &MockQueryContext,
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
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    // one pool for all cases: pools are not shut down, so one per case would pile up workers
    let gdal_process_pool =
        GdalProcessPool::new_with_tokio_handle(runtime.handle(), 8, 8, 8, WorkerConfig::default());

    let cases = [
        Case {
            name: "reprojection",
            operator: mercator,
            area: None,
        },
        // nested adapters: their parallelism must add up, not multiply
        Case {
            name: "chained_reprojection",
            operator: |source| reproject(mercator(source), SpatialReference::epsg_4326()),
            area: None,
        },
        // a WMS GetMap tile at zoom 4 over Europe
        Case {
            name: "wms_tile",
            operator: mercator,
            area: Some(SpatialPartition2D::new_unchecked(
                Coordinate2D::new(0., 7_514_065.6),
                Coordinate2D::new(2_504_688.5, 5_009_377.1),
            )),
        },
        // a zoomed-out map of the whole world
        Case {
            name: "overview",
            operator: |source| {
                Downsampling {
                    params: DownsamplingParams {
                        sampling_method: DownsamplingMethod::NearestNeighbor,
                        output_resolution: DownsamplingResolution::Fraction(Fraction {
                            x: 4.,
                            y: 4.,
                        }),
                        output_origin_reference: None,
                    },
                    sources: SingleRasterSource {
                        raster: mercator(source),
                    },
                }
                .typed()
            },
            area: None,
        },
    ];

    for case in &cases {
        for parallelism in [1, 4, 8] {
            let (processor, mut query_context, query) =
                setup(&runtime, case, gdal_process_pool.clone());
            query_context.subquery_parallelism = SubQueryParallelism::new(parallelism);
            c.bench_function(&format!("{}_{parallelism}", case.name), |b| {
                b.to_async(&runtime)
                    .iter(|| run(&processor, &query_context, query.clone()));
            });
        }
    }
}

criterion_group!(subquery_gdal, subquery_gdal_benchmark);
criterion_main!(subquery_gdal);
