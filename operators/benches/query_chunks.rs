use async_trait::async_trait;
use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{QueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval},
    util::{test::TestDefault, Identifier},
};
use geoengine_operators::{
    engine::{
        ChunkByteSize, InitializedRasterOperator, MockQueryContext, QueryContextExtensions,
        QueryProcessor, RasterOperator, SingleRasterSource, StatisticsWrappingMockExecutionContext,
        WorkflowOperatorPath,
    },
    pro::{
        cache::{cache_operator::InitializedCacheOperator, tile_cache::TileCache},
        meta::quota::{
            ComputationContext, ComputationUnit, QuotaCheck, QuotaChecker, QuotaTracking,
        },
    },
    processing::{
        AggregateFunctionParams, NeighborhoodAggregate, NeighborhoodAggregateParams,
        NeighborhoodParams,
    },
    source::{GdalSource, GdalSourceParameters},
    util::{gdal::add_ndvi_dataset, Result},
};
use std::{
    collections::HashMap,
    io::Write,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, metadata::LevelFilter, Level};
use tracing_subscriber::{self, prelude::__tracing_subscriber_field_MakeExt, EnvFilter};

// static MY_LOGGER: MyLogger = MyLogger;

// TODO: remove
// for debugging:
// cargo bench --profile=dev --features pro --bench query_chunks

/// This benchmarks runs a workflow twice to see the impact of the cache
/// Run it with `cargo bench --bench cache --features pro`
#[tokio::main]
async fn main() {
    let mut exe_ctx = StatisticsWrappingMockExecutionContext::test_default();

    let ndvi_id = add_ndvi_dataset(&mut exe_ctx.inner);

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

    let processor = operator.query_processor().unwrap().get_u8().unwrap();

    let query_ctx = MockQueryContext::new_with_query_extensions(
        ChunkByteSize::test_default(),
        create_necessary_extensions(),
    );

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

    // log::set_logger(&MY_LOGGER).unwrap();
    // log::set_max_level(LevelFilter::Trace);

    let filter = EnvFilter::default()
        // .add_directive("my_crate::module=trace".parse().unwrap())
        // .add_directive(LevelFilter::DEBUG.into())
        .add_directive(
            "geoengine_operators::pro::adapters::stream_statistics_adapter=debug"
                .parse()
                .unwrap(),
        );

    let poll_next_writer = PollNextWriterWrapper::new();

    let writer_clone = poll_next_writer.clone();

    let collector = tracing_subscriber::fmt()
        .json()
        // filter spans/events with level TRACE or higher.
        .with_max_level(Level::TRACE)
        // .with_env_filter(filter)
        .with_current_span(true)
        .with_writer(move || writer_clone.clone())
        // .flatten_event(true)
        // .with_span_list(true)
        // .map_fmt_fields(|f| f.debug_alt())
        // build but do not install the subscriber.
        .finish();

    tracing::subscriber::set_global_default(collector).unwrap();

    info!("This will be logged to stdout");

    // let runtime = tokio::runtime::Handle::current();

    // tracing::subscriber::with_default(collector, move || {
    //     info!("This will be logged to stdout");

    //     // TODO: does this make sense?
    //     // tokio::runtime::Runtime::.unwrap().block_on(async {
    //     //     let tiles = stream.collect::<Vec<_>>().await;
    //     // });

    //     runtime.spawn(async move {
    //         let tiles = stream.collect::<Vec<_>>().await;
    //     });
    // });

    // TODO: remove
    // info!("hello log");
    // warn!("warning");
    // error!("oops");
    // eprintln!("hello stderr");

    let tiles = stream.collect::<Vec<_>>().await;

    dbg!(&poll_next_writer.inner.lock().unwrap().element_counts);

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

// struct MyLogger;

// impl log::Log for MyLogger {
//     fn enabled(&self, _metadata: &Metadata) -> bool {
//         true
//     }

//     fn log(&self, record: &Record) {
//         if record.level() != Level::Trace {
//             return;
//         }

//         // if !record.target().contains("poll_next") {
//         //     return;
//         // }

//         let args = record.args().to_string();
//         if args.contains("spawn_blocking")
//             || args.contains("cache key")
//             || args.contains("Inserted tile for query")
//         {
//             return;
//         }

//         // if self.enabled(record.metadata()) {
//         eprintln!(
//             "{} - {} - {}",
//             record.level(),
//             record.args(),
//             record.target()
//         );

//         // }
//     }
//     fn flush(&self) {}
// }

struct MockQuotaChecker;

#[async_trait]
impl QuotaCheck for MockQuotaChecker {
    async fn ensure_quota_available(&self) -> Result<()> {
        Ok(())
    }
}

/// we don't need this for this bench, but to prevent panics from the wrapping op
fn create_necessary_extensions() -> QueryContextExtensions {
    let mut extensions = QueryContextExtensions::default();

    // let tile_cache = Arc::new(TileCache::test_default());
    // extensions.insert(tile_cache);

    let computation_unit = ComputationUnit {
        issuer: uuid::Uuid::new_v4(),
        context: ComputationContext::new(),
    };
    extensions.insert(QuotaTracking::new(
        tokio::sync::mpsc::unbounded_channel().0,
        computation_unit,
    ));
    extensions.insert(Box::new(MockQuotaChecker) as QuotaChecker);

    extensions
}

struct PollNextWriter {
    element_counts: HashMap<String, u64>,
}

impl PollNextWriter {
    fn new() -> Self {
        Self {
            element_counts: HashMap::new(),
        }
    }
}

impl Write for PollNextWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // eprintln!("hallo!");

        let record: serde_json::Value = serde_json::from_slice(buf).unwrap();

        if record["level"] != "DEBUG"
            || record["target"] != "geoengine_operators::pro::adapters::stream_statistics_adapter"
            || record["fields"]["empty"] != false
            // how can this be?
            || record["span"]["name"].is_null()
        {
            return Ok(buf.len());
        }

        let operator = record["span"]["name"].to_string();
        let element_count = record["fields"]["element_count"].as_u64().unwrap();

        let entry = self.element_counts.entry(operator).or_insert(0);
        *entry += element_count;

        // let s = std::str::from_utf8(buf).unwrap();
        // if s.contains("poll_next") {
        //     return Ok(0);
        // }
        // eprintln!("Event: {}", record["fields"]["event"]);
        // eprintln!("Elements: {}", record["fields"]["element_count"]);
        // eprintln!("Operator: {}", record["span"]["name"]);
        // eprintln!("Target: {}", record["target"]);
        // eprintln!();
        // eprintln!("{record}");
        // eprintln!();

        // pretend that we read everything
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct PollNextWriterWrapper {
    inner: Arc<Mutex<PollNextWriter>>,
}

impl PollNextWriterWrapper {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(PollNextWriter::new())),
        }
    }
}

impl Write for PollNextWriterWrapper {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
