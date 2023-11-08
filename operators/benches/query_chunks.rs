//!
//! This benchmark tests the performance of queries in the form of produced tiles or chunks.
//!
//! The benchmark is run with the following command:
//!
//! ```bash
//! cargo bench --features pro --bench query_chunks
//! ```
//!
//! For development, you can run it also in dev mode (does not change the results):
//!
//! ```bash
//! cargo bench --profile=dev --features pro --bench query_chunks
//! ```
//!

use async_trait::async_trait;
use csv::WriterBuilder;
use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{
        BoundingBox2D, QueryRectangle, RasterQueryRectangle, SpatialPartition2D, SpatialResolution,
        TimeInterval, VectorQueryRectangle,
    },
    raster::Pixel,
    util::{test::TestDefault, Identifier},
};
use geoengine_operators::{
    engine::{
        ChunkByteSize, ExecutionContext, InitializedRasterOperator, MockQueryContext, QueryContext,
        QueryContextExtensions, QueryProcessor, RasterOperator, RasterQueryProcessor,
        SingleRasterSource, SingleVectorMultipleRasterSources, TypedRasterQueryProcessor,
        VectorOperator, VectorQueryProcessor, WorkflowOperatorPath,
    },
    pro::{
        engine::StatisticsWrappingMockExecutionContext,
        meta::quota::{
            ComputationContext, ComputationUnit, QuotaCheck, QuotaChecker, QuotaTracking,
        },
    },
    processing::{
        AggregateFunctionParams, FeatureAggregationMethod, NeighborhoodAggregate,
        NeighborhoodAggregateParams, NeighborhoodParams, RasterVectorJoin, RasterVectorJoinParams,
        TemporalAggregationMethod,
    },
    source::{GdalSource, GdalSourceParameters, OgrSource, OgrSourceParameters},
    util::{
        gdal::{add_ndvi_dataset, add_ports_dataset},
        Result,
    },
};
use std::{
    collections::{BTreeSet, HashMap},
    io::Write,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::Level;

fn main() {
    let (mut exe_ctx, query_ctx) = setup_contexts();

    let benchmarks = setup_benchmarks(&mut exe_ctx);

    let (results, element_headers) = run_benchmarks(benchmarks, exe_ctx, query_ctx);

    let csv = write_csv(element_headers, results);

    println!("{csv}");
}

type BenchmarkElementCounts = HashMap<String, u64>;

fn setup_contexts() -> (StatisticsWrappingMockExecutionContext, MockQueryContext) {
    let exe_ctx = StatisticsWrappingMockExecutionContext::test_default();
    let query_ctx = MockQueryContext::new_with_query_extensions(
        ChunkByteSize::test_default(),
        create_necessary_extensions(),
    );
    (exe_ctx, query_ctx)
}

/// This functions defines the benchmarks that are run.
fn setup_benchmarks(exe_ctx: &mut StatisticsWrappingMockExecutionContext) -> Vec<Benchmark> {
    let ndvi_id = add_ndvi_dataset(&mut exe_ctx.inner);
    let ports_id = add_ports_dataset(&mut exe_ctx.inner);

    vec![
        Benchmark::Raster {
            name: "neighborhood_aggregate".to_string(),
            operator: NeighborhoodAggregate {
                params: NeighborhoodAggregateParams {
                    neighborhood: NeighborhoodParams::WeightsMatrix {
                        weights: vec![vec![1., 2., 3.], vec![4., 5., 6.], vec![7., 8., 9.]],
                    },
                    aggregate_function: AggregateFunctionParams::Sum,
                },
                sources: SingleRasterSource {
                    raster: GdalSource {
                        params: GdalSourceParameters {
                            data: ndvi_id.clone(),
                        },
                    }
                    .boxed(),
                },
            }
            .boxed(),
            query_rectangle: QueryRectangle {
                spatial_bounds: SpatialPartition2D::new_unchecked(
                    [-180., -90.].into(),
                    [180., 90.].into(),
                ),
                time_interval: TimeInterval::default(),
                spatial_resolution: SpatialResolution::zero_point_one(),
                selection: Default::default(),
            },
        },
        Benchmark::Vector {
            name: "raster_vector_join".to_string(),
            operator: RasterVectorJoin {
                params: RasterVectorJoinParams {
                    names: vec!["ndvi".to_string()],
                    feature_aggregation: FeatureAggregationMethod::Mean,
                    feature_aggregation_ignore_no_data: true,
                    temporal_aggregation: TemporalAggregationMethod::Mean,
                    temporal_aggregation_ignore_no_data: true,
                },
                sources: SingleVectorMultipleRasterSources {
                    vector: OgrSource {
                        params: OgrSourceParameters {
                            data: ports_id,
                            attribute_projection: None,
                            attribute_filters: None,
                        },
                    }
                    .boxed(),
                    rasters: vec![GdalSource {
                        params: GdalSourceParameters { data: ndvi_id },
                    }
                    .boxed()],
                },
            }
            .boxed(),
            query_rectangle: QueryRectangle {
                spatial_bounds: BoundingBox2D::new_unchecked(
                    [-180., -90.].into(),
                    [180., 90.].into(),
                ),
                time_interval: TimeInterval::default(),
                spatial_resolution: SpatialResolution::zero_point_one(),
                selection: Default::default(),
            },
        },
    ]
}

fn run_benchmarks(
    benchmarks: Vec<Benchmark>,
    exe_ctx: StatisticsWrappingMockExecutionContext,
    query_ctx: MockQueryContext,
) -> (Vec<(String, BenchmarkElementCounts)>, BTreeSet<String>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut poll_next_receiver = setup_tracing();

    let mut results: Vec<(String, BenchmarkElementCounts)> = Vec::with_capacity(benchmarks.len());
    let mut element_headers = BTreeSet::new();
    for benchmark in benchmarks {
        let name = benchmark.name().to_string();
        let result = run_benchmark(
            &runtime,
            &exe_ctx,
            &query_ctx,
            benchmark,
            &mut poll_next_receiver,
        );

        for header in result.keys() {
            element_headers.insert(header.to_string());
        }

        results.push((name, result));
    }

    (results, element_headers)
}

fn write_csv(
    element_headers: BTreeSet<String>,
    results: Vec<(String, HashMap<String, u64>)>,
) -> String {
    let mut csv = WriterBuilder::new().has_headers(true).from_writer(vec![]);

    csv.write_field("name").unwrap();
    for header in &element_headers {
        csv.write_field(header).unwrap();
    }
    csv.write_record(None::<&[u8]>).unwrap();

    for (name, result) in results {
        csv.write_field(name).unwrap();
        for header in &element_headers {
            csv.write_field(result.get(header).unwrap_or(&0).to_string())
                .unwrap();
        }
        csv.write_record(None::<&[u8]>).unwrap();
    }

    csv.flush().unwrap();
    let csv = csv.into_inner().unwrap();

    String::from_utf8(csv).unwrap()
}

fn setup_tracing() -> UnboundedReceiver<Record> {
    let (poll_next_sender, poll_next_receiver) = tokio::sync::mpsc::unbounded_channel::<Record>();

    let collector = tracing_subscriber::fmt()
        .json()
        // filter spans/events with level TRACE or higher.
        .with_max_level(Level::TRACE)
        .with_current_span(true)
        .with_writer(move || PollNextForwarder::new(poll_next_sender.clone()))
        // build but do not install the subscriber.
        .finish();

    tracing::subscriber::set_global_default(collector).unwrap();

    poll_next_receiver
}

fn run_benchmark(
    runtime: &tokio::runtime::Runtime,
    exe_ctx: &dyn ExecutionContext,
    query_ctx: &dyn QueryContext,
    benchmark: Benchmark,
    poll_next_receiver: &mut UnboundedReceiver<Record>,
) -> BenchmarkElementCounts {
    match benchmark {
        Benchmark::Raster {
            name: _,
            operator,
            query_rectangle,
        } => {
            let operator = runtime.block_on(async {
                operator
                    .initialize(WorkflowOperatorPath::initialize_root(), exe_ctx)
                    .await
                    .unwrap()
            });

            let processor = operator.query_processor().unwrap();

            match processor {
                TypedRasterQueryProcessor::U8(processor) => {
                    collect_raster_query(runtime, query_ctx, processor, query_rectangle)
                }
                TypedRasterQueryProcessor::U16(processor) => {
                    collect_raster_query(runtime, query_ctx, processor, query_rectangle)
                }
                TypedRasterQueryProcessor::U32(processor) => {
                    collect_raster_query(runtime, query_ctx, processor, query_rectangle)
                }
                TypedRasterQueryProcessor::U64(processor) => {
                    collect_raster_query(runtime, query_ctx, processor, query_rectangle)
                }
                TypedRasterQueryProcessor::I8(processor) => {
                    collect_raster_query(runtime, query_ctx, processor, query_rectangle)
                }
                TypedRasterQueryProcessor::I16(processor) => {
                    collect_raster_query(runtime, query_ctx, processor, query_rectangle)
                }
                TypedRasterQueryProcessor::I32(processor) => {
                    collect_raster_query(runtime, query_ctx, processor, query_rectangle)
                }
                TypedRasterQueryProcessor::I64(processor) => {
                    collect_raster_query(runtime, query_ctx, processor, query_rectangle)
                }
                TypedRasterQueryProcessor::F32(processor) => {
                    collect_raster_query(runtime, query_ctx, processor, query_rectangle)
                }
                TypedRasterQueryProcessor::F64(processor) => {
                    collect_raster_query(runtime, query_ctx, processor, query_rectangle)
                }
            }
        }
        Benchmark::Vector {
            name: _,
            operator,
            query_rectangle,
        } => {
            let operator = runtime.block_on(async {
                operator
                    .initialize(WorkflowOperatorPath::initialize_root(), exe_ctx)
                    .await
                    .unwrap()
            });

            let processor = operator.query_processor().unwrap();

            match processor {
                geoengine_operators::engine::TypedVectorQueryProcessor::Data(processor) => {
                    collect_vector_query(runtime, query_ctx, processor, query_rectangle)
                }
                geoengine_operators::engine::TypedVectorQueryProcessor::MultiPoint(processor) => {
                    collect_vector_query(runtime, query_ctx, processor, query_rectangle)
                }
                geoengine_operators::engine::TypedVectorQueryProcessor::MultiLineString(
                    processor,
                ) => collect_vector_query(runtime, query_ctx, processor, query_rectangle),
                geoengine_operators::engine::TypedVectorQueryProcessor::MultiPolygon(processor) => {
                    collect_vector_query(runtime, query_ctx, processor, query_rectangle)
                }
            }
        }
    }

    gather_poll_nexts(poll_next_receiver)
}

fn collect_raster_query<P: Pixel>(
    runtime: &tokio::runtime::Runtime,
    query_ctx: &dyn QueryContext,
    processor: Box<dyn RasterQueryProcessor<RasterType = P>>,
    query_rectangle: RasterQueryRectangle,
) {
    let stream =
        runtime.block_on(async { processor.query(query_rectangle, query_ctx).await.unwrap() });

    let _tiles = runtime.block_on(stream.collect::<Vec<_>>());
}

fn collect_vector_query<G: 'static>(
    runtime: &tokio::runtime::Runtime,
    query_ctx: &dyn QueryContext,
    processor: Box<dyn VectorQueryProcessor<VectorType = G>>,
    query_rectangle: VectorQueryRectangle,
) {
    let stream =
        runtime.block_on(async { processor.query(query_rectangle, query_ctx).await.unwrap() });

    let _chunks = runtime.block_on(stream.collect::<Vec<_>>());
}

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

fn gather_poll_nexts(receiver: &mut UnboundedReceiver<Record>) -> BenchmarkElementCounts {
    let mut element_counts = HashMap::new();

    // poll everything from the receiver until it is empty
    while let Ok(record) = receiver.try_recv() {
        let entry = element_counts.entry(record.operator).or_insert(0);
        *entry += 1;
    }

    element_counts
}

struct Record {
    operator: String,
}

struct PollNextForwarder {
    sender: UnboundedSender<Record>,
}

impl PollNextForwarder {
    fn new(sender: UnboundedSender<Record>) -> Self {
        Self { sender }
    }

    fn process_input(&mut self, record: serde_json::Value) {
        // comment this in for seeing all tracing logs
        // dbg!(&record);

        if record["level"] != "DEBUG"
            || record["target"] != "geoengine_operators::pro::adapters::stream_statistics_adapter"
            || record["fields"]["empty"] != false
            || record["span"]["name"].is_null()
        {
            return;
        }

        let result = Record {
            operator: record["span"]["name"].as_str().unwrap().to_string(),
        };

        self.sender.send(result).unwrap();
    }
}

impl Write for PollNextForwarder {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.process_input(serde_json::from_slice(buf).unwrap());

        // just pretend that we read everything
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

enum Benchmark {
    Raster {
        name: String,
        operator: Box<dyn RasterOperator>,
        query_rectangle: RasterQueryRectangle,
    },
    Vector {
        name: String,
        operator: Box<dyn VectorOperator>,
        query_rectangle: VectorQueryRectangle,
    },
}

impl Benchmark {
    fn name(&self) -> &str {
        match self {
            Benchmark::Raster { name, .. } => name,
            Benchmark::Vector { name, .. } => name,
        }
    }
}
