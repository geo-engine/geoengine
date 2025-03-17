#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::print_stderr)] // okay in benchmarks

use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{BandSelection, RasterQueryRectangle, TimeInterval},
    raster::{GridBoundingBox2D, TilesEqualIgnoringCacheHint},
    util::test::TestDefault,
};
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
use std::sync::Arc;

/// This benchmarks runs a workflow twice to see the impact of the cache
/// Run it with `cargo bench --bench cache --features pro`
#[tokio::main]
async fn main() {
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

    let tile_cache = Arc::new(SharedCache::test_default());

    let query_ctx = exe_ctx.mock_query_context_with_query_extensions(
        ChunkByteSize::test_default(),
        Some(tile_cache),
        None,
        None,
    );

    let start = std::time::Instant::now();

    let stream = processor
        .query(
            RasterQueryRectangle::new_with_grid_bounds(
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
            RasterQueryRectangle::new_with_grid_bounds(
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
