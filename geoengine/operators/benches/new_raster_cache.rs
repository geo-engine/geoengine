#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::print_stderr)] // okay in benchmarks

use crate::CacheVariant::{NewCache, NoCache, OldCache};
use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use geoengine_datatypes::primitives::TimeInstance;
use geoengine_datatypes::{
    primitives::{BandSelection, RasterQueryRectangle, TimeInterval},
    raster::{GridBoundingBox2D, TilesEqualIgnoringCacheHint},
    util::test::TestDefault,
};
use geoengine_operators::cache::new_raster_cache::{NewRasterCacheEnum, RasterCacheOperator};
use geoengine_operators::engine::{BoxRasterQueryProcessor, ExecutionContext, MockQueryContext};
use geoengine_operators::{
    cache::{cache_operator::InitializedCacheOperator, shared_cache::SharedCache},
    engine::{
        ChunkByteSize, InitializedRasterOperator, MockExecutionContext, QueryProcessor,
        RasterOperator, SingleRasterSource, WorkflowOperatorPath,
    },
    processing::{
        AggregateFunctionParams, NeighborhoodAggregate, NeighborhoodAggregateParams,
        NeighborhoodParams,
    },
    source::{GdalSource, GdalSourceParameters},
    util::Result,
    util::gdal::add_ndvi_dataset,
};
use lazy_static::lazy_static;
use std::hint::black_box;
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};

/// This benchmarks runs a workflow twice to see the impact of the cache
/// Run it with `cargo bench --bench new_raster_cache`
//#[tokio::main]
async fn main_f() {
    let mut exe_ctx = MockExecutionContext::test_default();

    let ndvi_id = add_ndvi_dataset(&mut exe_ctx);

    let operator = NeighborhoodAggregate {
        params: NeighborhoodAggregateParams {
            neighborhood: NeighborhoodParams::WeightsMatrix {
                weights: vec![vec![1., 2., 3.], vec![4., 5., 6.], vec![7., 8., 9.]],
            },
            aggregate_function: AggregateFunctionParams::Sum,
        },
        sources: SingleRasterSource {
            raster: GdalSource {
                params: GdalSourceParameters::new(ndvi_id),
            }
            .boxed(),
        },
    }
    .boxed()
    .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
    .await
    .unwrap();

    let cached_op = InitializedCacheOperator::new(operator);

    let processor = cached_op.query_processor().unwrap().get_u8().unwrap();

    let tile_cache = Arc::new(NewRasterCacheEnum::test_default());

    let query_ctx = exe_ctx.mock_query_context_with_query_extensions(
        ChunkByteSize::test_default(),
        None,
        Some(tile_cache),
        None,
        None,
    );

    let start = std::time::Instant::now();

    let stream = processor
        .query(
            RasterQueryRectangle::new(
                GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
                TimeInterval::default(),
                BandSelection::first(),
            ),
            &query_ctx,
        )
        .await
        .unwrap();

    let tiles = stream.collect::<Vec<_>>().await;

    println!("First run: {:?}", start.elapsed());

    let tiles = tiles.into_iter().collect::<Result<Vec<_>>>().unwrap();

    let start = std::time::Instant::now();

    let stream_from_cache = processor
        .query(
            RasterQueryRectangle::new(
                GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
                TimeInterval::default(),
                BandSelection::first(),
            ),
            &query_ctx,
        )
        .await
        .unwrap();

    let tiles_from_cache = stream_from_cache.collect::<Vec<_>>().await;

    println!("From cache: {:?}", start.elapsed());

    let tiles_from_cache = tiles_from_cache
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert!(tiles.tiles_equal_ignoring_cache_hint(&tiles_from_cache));
}

async fn setup(cache: CacheVariant) -> (Arc<BoxRasterQueryProcessor<u8>>, MockQueryContext) {
    let exe_ctx = EXE_CTX.get_or_init(|| {
        println!("Initializing OnceLock");
        Mutex::new(MockExecutionContext::test_default())
    });
    let mut exe_ctx = exe_ctx.lock().unwrap();

    let ndvi_id = add_ndvi_dataset(&mut exe_ctx);

    let operator = NeighborhoodAggregate {
        params: NeighborhoodAggregateParams {
            neighborhood: NeighborhoodParams::WeightsMatrix {
                weights: vec![vec![1., 2., 3.], vec![4., 5., 6.], vec![7., 8., 9.]],
            },
            aggregate_function: AggregateFunctionParams::Sum,
        },
        sources: SingleRasterSource {
            raster: GdalSource {
                params: GdalSourceParameters::new(ndvi_id),
            }
            .boxed(),
        },
    }
    .boxed()
    .initialize(WorkflowOperatorPath::initialize_root(), &*exe_ctx)
    .await
    .unwrap();

    match cache {
        NewCache => {
            let cached_op = RasterCacheOperator::wrap_operator(operator);

            let tile_cache = Arc::new(NewRasterCacheEnum::test_default());

            let query_ctx = exe_ctx.mock_query_context_with_query_extensions(
                ChunkByteSize::test_default(),
                None,
                Some(tile_cache),
                None,
                None,
            );

            let processor = cached_op.query_processor().unwrap().get_u8().unwrap();

            let processor = Arc::new(processor);

            query(processor.clone(), &query_ctx).await;

            return (processor, query_ctx);
        }
        OldCache => {
            let cached_op = InitializedCacheOperator::new(operator);

            let tile_cache = Arc::new(SharedCache::test_default());

            let query_ctx = exe_ctx.mock_query_context_with_query_extensions(
                ChunkByteSize::test_default(),
                Some(tile_cache),
                None,
                None,
                None,
            );

            let processor = cached_op.query_processor().unwrap().get_u8().unwrap();

            let processor = Arc::new(processor);

            query(processor.clone(), &query_ctx).await;

            return (processor, query_ctx);
        }
        NoCache => {
            let processor = operator.query_processor().unwrap().get_u8().unwrap();

            let query_ctx = exe_ctx.mock_query_context_with_query_extensions(
                ChunkByteSize::test_default(),
                None,
                None,
                None,
                None,
            );
            (Arc::new(processor), query_ctx)
        }
    }
}

async fn query(processor: Arc<BoxRasterQueryProcessor<u8>>, query_ctx: &MockQueryContext) {
    let stream = processor
        .query(
            RasterQueryRectangle::new(
                GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
                //TimeInterval::default(),
                TimeInterval::new_unchecked(
                    TimeInstance::from_str("2014-01-01T00:00:00.000Z")
                        .expect("it should only be used in tests"),
                    TimeInstance::from_str("2015-01-01T00:00:00.000Z")
                        .expect("it should only be used in tests"),
                ),
                BandSelection::first(),
            ),
            query_ctx,
        )
        .await
        .unwrap();

    let tiles = stream.collect::<Vec<_>>().await;

    let tiles = tiles.into_iter().collect::<Result<Vec<_>>>().unwrap();

    black_box(tiles);
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("main_f", |b| b.iter(|| main_f()));
}

static EXE_CTX: OnceLock<Mutex<MockExecutionContext>> = OnceLock::new();

enum CacheVariant {
    NoCache,
    OldCache,
    NewCache,
}

fn benchmark_both(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut g = c.benchmark_group("ndvi_neighborhood_aggregate");
    g.sample_size(10);

    // uncached
    let (uncached_processor, uncached_query_ctx) = runtime.block_on(async { setup(NoCache).await });
    let uncached_query_ctx = Arc::new(uncached_query_ctx);
    g.bench_function("without_cache", |b| {
        let proc = uncached_processor.clone();
        let ctx = uncached_query_ctx.clone();
        b.to_async(&runtime).iter(|| {
            let proc = proc.clone();
            let ctx = ctx.clone();
            async move {
                // `query` erwartet `&MockQueryContext`, daher `&*ctx`
                query(proc, &*ctx).await;
            }
        })
    });

    // new cache
    let (cached_processor, cached_query_ctx) = runtime.block_on(async { setup(NewCache).await });
    let cached_query_ctx = Arc::new(cached_query_ctx);
    g.bench_function("with_new_cache", |b| {
        let proc = cached_processor.clone();
        let ctx = cached_query_ctx.clone();
        b.to_async(&runtime).iter(|| {
            let proc = proc.clone();
            let ctx = ctx.clone();
            async move {
                query(proc, &*ctx).await;
            }
        })
    });

    // old cache
    let (cached_processor, cached_query_ctx) = runtime.block_on(async { setup(OldCache).await });
    let cached_query_ctx = Arc::new(cached_query_ctx);
    g.bench_function("with_old_cache", |b| {
        let proc = cached_processor.clone();
        let ctx = cached_query_ctx.clone();
        b.to_async(&runtime).iter(|| {
            let proc = proc.clone();
            let ctx = ctx.clone();
            async move {
                query(proc, &*ctx).await;
            }
        })
    });

    g.finish();
}

criterion_group!(test_benches, criterion_benchmark);
criterion_group!(benches, benchmark_both);
criterion_main!(benches);
