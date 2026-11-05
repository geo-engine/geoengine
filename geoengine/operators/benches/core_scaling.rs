#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

//! Tiles/s of map path operators on the NDVI test dataset over the Rayon pool size and the number
//! of concurrent queries. The GDAL pool has the server's default size and the raster cache is off.
//!
//! `cargo build --release --bin gdalsource-process && cargo bench --bench core_scaling`

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::TryStreamExt;
use geoengine_datatypes::{
    primitives::{BandSelection, RasterQueryRectangle, TimeInterval},
    raster::{RasterDataType, TilingSpecification},
    spatial_reference::{SpatialReference, SpatialReferenceAuthority},
    util::test::TestDefault,
};
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{
        ChunkByteSize, InitializedRasterOperator, MockExecutionContext, MockQueryContext,
        ProcessingGraphOperatorPath, SingleRasterOrVectorSource, SingleRasterSource,
        TypedRasterOperator,
    },
    processing::{
        DeriveOutRasterSpecsSource, Downsampling, DownsamplingMethod, DownsamplingParams,
        DownsamplingResolution, Expression, ExpressionParams, Fraction, Reprojection,
        ReprojectionParams, initialize_expression_dependencies,
    },
    source::{
        GdalProcessPool, GdalSource, GdalSourceParameters, gdal_worker_process::WorkerConfig,
    },
    util::gdal::add_ndvi_dataset,
};
use std::sync::Arc;

const RAYON_THREADS: [usize; 7] = [1, 2, 4, 8, 16, 32, 64];
const CONCURRENT_QUERIES: [usize; 2] = [1, 8];

fn operators(source: &TypedRasterOperator) -> Vec<(&'static str, TypedRasterOperator)> {
    vec![
        (
            "ndvi_expression",
            TypedRasterOperator::from(Expression {
                // NDVI-like arithmetic; the test dataset has a single band
                params: ExpressionParams {
                    expression: "(A - 100) / (A + 100)".to_string(),
                    output_type: RasterDataType::F32,
                    output_band: None,
                    map_no_data: false,
                },
                sources: SingleRasterSource {
                    raster: source.clone(),
                },
            }),
        ),
        (
            "downsample_3x3",
            TypedRasterOperator::from(Downsampling {
                params: DownsamplingParams {
                    sampling_method: DownsamplingMethod::NearestNeighbor,
                    output_resolution: DownsamplingResolution::Fraction(Fraction { x: 3., y: 3. }),
                    output_origin_reference: None,
                },
                sources: SingleRasterSource {
                    raster: source.clone(),
                },
            }),
        ),
        (
            "reproject_3857",
            TypedRasterOperator::from(Reprojection {
                params: ReprojectionParams {
                    target_spatial_reference: SpatialReference::new(
                        SpatialReferenceAuthority::Epsg,
                        3857,
                    ),
                    derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
                },
                sources: SingleRasterOrVectorSource {
                    source: source.clone().into(),
                },
            }),
        ),
    ]
}

/// All tiles of the operator's grid for the six months of the dataset
fn full_query(operator: &dyn InitializedRasterOperator) -> RasterQueryRectangle {
    let grid_bounds = operator
        .result_descriptor()
        .tiling_grid_definition(TilingSpecification::test_default())
        .tiling_grid_bounds();
    RasterQueryRectangle::new(
        grid_bounds,
        TimeInterval::new(1_388_534_400_000, 1_404_172_800_000).unwrap(), // 2014-01-01 – 2014-07-01
        BandSelection::first(),
    )
}

/// Runs `concurrent` queries as separate tasks, like the server does, and returns the number of tiles
async fn run_queries(
    operator: Arc<dyn InitializedRasterOperator>,
    query_ctx: Arc<MockQueryContext>,
    concurrent: usize,
) -> usize {
    let tasks = (0..concurrent).map(|_| {
        let operator = operator.clone();
        let query_ctx = query_ctx.clone();
        tokio::spawn(async move {
            let query = full_query(operator.as_ref());
            call_on_generic_raster_processor!(operator.query_processor().unwrap(), p => {
                p.raster_query(query, query_ctx.as_ref())
                    .await
                    .unwrap()
                    .try_fold(0, |n, _tile| async move { Ok(n + 1) })
                    .await
                    .unwrap()
            })
        })
    });
    futures::future::try_join_all(tasks)
        .await
        .unwrap()
        .into_iter()
        .sum()
}

fn core_scaling(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();

    runtime
        .block_on(initialize_expression_dependencies())
        .unwrap();

    let mut exec_ctx = MockExecutionContext::test_default();
    // server defaults of `[gdal_process_pool]`
    exec_ctx.gdal_process_pool = GdalProcessPool::new(32, 16, 8, WorkerConfig::default());
    let source = TypedRasterOperator::from(GdalSource {
        params: GdalSourceParameters::new(add_ndvi_dataset(&mut exec_ctx)),
    });

    for (name, operator) in operators(&source) {
        let initialized: Arc<dyn InitializedRasterOperator> = runtime
            .block_on(
                operator.initialize(ProcessingGraphOperatorPath::initialize_root(), &exec_ctx),
            )
            .unwrap()
            .into();

        let mut group = c.benchmark_group(format!("core_scaling/{name}"));
        group.sample_size(10);

        for threads in RAYON_THREADS {
            let mut query_ctx = exec_ctx.mock_query_context_with_chunk_size_and_thread_count(
                ChunkByteSize::test_default(),
                threads,
            );
            query_ctx.new_raster_cache = None; // measure computation, not cache hits
            let query_ctx = Arc::new(query_ctx);

            for concurrent in CONCURRENT_QUERIES {
                let tiles = runtime.block_on(run_queries(
                    initialized.clone(),
                    query_ctx.clone(),
                    concurrent,
                ));
                group.throughput(Throughput::Elements(tiles as u64));
                group.bench_function(
                    BenchmarkId::new(format!("queries_{concurrent}"), format!("rayon_{threads}")),
                    |b| {
                        b.to_async(&runtime).iter(|| {
                            run_queries(initialized.clone(), query_ctx.clone(), concurrent)
                        });
                    },
                );
            }
        }
        group.finish();
    }
}

criterion_group!(benches, core_scaling);
criterion_main!(benches);
