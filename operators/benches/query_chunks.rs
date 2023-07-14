use std::sync::Arc;

use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{QueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval},
    raster::TilesEqualIgnoringCacheHint,
    util::test::TestDefault,
};
use geoengine_operators::{
    engine::{
        ChunkByteSize, InitializedRasterOperator, MockExecutionContext, MockQueryContext,
        QueryContextExtensions, QueryProcessor, RasterOperator, SingleRasterSource,
        WorkflowOperatorPath,
    },
    pro::cache::{cache_operator::InitializedCacheOperator, tile_cache::TileCache},
    processing::{
        AggregateFunctionParams, NeighborhoodAggregate, NeighborhoodAggregateParams,
        NeighborhoodParams,
    },
    source::{GdalSource, GdalSourceParameters},
    util::gdal::add_ndvi_dataset,
    util::Result,
};
use log::{error, info, warn, Level, LevelFilter, Metadata, Record};

static MY_LOGGER: MyLogger = MyLogger;

// TODO: remove
// for debugging:
// cargo bench --profile=dev --features pro --bench query_chunks

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
                params: GdalSourceParameters { data: ndvi_id },
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

    let tile_cache = Arc::new(TileCache::test_default());

    let mut extensions = QueryContextExtensions::default();

    extensions.insert(tile_cache);

    let query_ctx =
        MockQueryContext::new_with_query_extensions(ChunkByteSize::test_default(), extensions);

    let stream = processor
        .query(
            QueryRectangle {
                spatial_bounds: SpatialPartition2D::new_unchecked(
                    [-180., -90.].into(),
                    [180., 90.].into(),
                ),
                time_interval: TimeInterval::default(),
                spatial_resolution: SpatialResolution::zero_point_one(),
            },
            &query_ctx,
        )
        .await
        .unwrap();

    log::set_logger(&MY_LOGGER).unwrap();
    log::set_max_level(LevelFilter::Trace);

    // TODO: remove
    info!("hello log");
    warn!("warning");
    error!("oops");
    eprintln!("hello stderr");

    let tiles = stream.collect::<Vec<_>>().await;

    // println!("First run: {:?}", start.elapsed());

    // let tiles = tiles.into_iter().collect::<Result<Vec<_>>>().unwrap();

    // let start = std::time::Instant::now();

    // let stream_from_cache = processor
    //     .query(
    //         QueryRectangle {
    //             spatial_bounds: SpatialPartition2D::new_unchecked(
    //                 [-180., -90.].into(),
    //                 [180., 90.].into(),
    //             ),
    //             time_interval: TimeInterval::default(),
    //             spatial_resolution: SpatialResolution::zero_point_one(),
    //         },
    //         &query_ctx,
    //     )
    //     .await
    //     .unwrap();

    // let tiles_from_cache = stream_from_cache.collect::<Vec<_>>().await;

    // println!("From cache: {:?}", start.elapsed());

    // let tiles_from_cache = tiles_from_cache
    //     .into_iter()
    //     .collect::<Result<Vec<_>>>()
    //     .unwrap();

    // assert!(tiles.tiles_equal_ignoring_cache_hint(&tiles_from_cache));
}

struct MyLogger;

impl log::Log for MyLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        if record.level() != Level::Trace {
            return;
        }

        // if !record.target().contains("poll_next") {
        //     return;
        // }

        let args = record.args().to_string();
        if args.contains("spawn_blocking")
            || args.contains("cache key")
            || args.contains("Inserted tile for query")
        {
            return;
        }

        // if self.enabled(record.metadata()) {
        eprintln!(
            "{} - {} - {}",
            record.level(),
            record.args(),
            record.target()
        );

        tracing::log
        // }
    }
    fn flush(&self) {}
}
