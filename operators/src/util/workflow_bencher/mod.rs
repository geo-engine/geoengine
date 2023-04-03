use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use futures::TryStreamExt;

use geoengine_datatypes::primitives::{QueryRectangle, RasterQueryRectangle};

use crate::call_on_generic_raster_processor;
use crate::engine::{ChunkByteSize, MockExecutionContext, RasterOperator, RasterQueryProcessor};

use geoengine_datatypes::{
    primitives::SpatialPartition2D,
    raster::{GridSize, TilingSpecification},
};

use serde::{Serialize, Serializer};

mod macros;

pub struct WorkflowBenchmarkCollector {
    pub writer: csv::Writer<std::io::Stdout>,
}

impl WorkflowBenchmarkCollector {
    pub fn add_benchmark_result(&mut self, result: WorkflowBenchmarkResult) {
        self.writer.serialize(result).unwrap();
        self.writer.flush().unwrap();
    }
}

impl Default for WorkflowBenchmarkCollector {
    fn default() -> Self {
        WorkflowBenchmarkCollector {
            writer: csv::Writer::from_writer(std::io::stdout()),
        }
    }
}

pub trait BenchmarkRunner {
    fn run_all_benchmarks(self, bencher: &mut WorkflowBenchmarkCollector);
}

pub struct WorkflowSingleBenchmark<F, O> {
    bench_id: &'static str,
    query_name: &'static str,
    query_rect: QueryRectangle<SpatialPartition2D>,
    tiling_spec: TilingSpecification,
    chunk_byte_size: ChunkByteSize,
    num_threads: usize,
    operator_builder: O,
    context_builder: F,
}

impl<O, F> WorkflowSingleBenchmark<F, O>
where
    F: Fn(TilingSpecification, usize) -> MockExecutionContext,
    O: Fn(TilingSpecification, QueryRectangle<SpatialPartition2D>) -> Box<dyn RasterOperator>,
{
    #[inline(never)]
    pub fn run_bench(&self) -> WorkflowBenchmarkResult {
        let run_time = tokio::runtime::Runtime::new().unwrap();
        let exe_ctx = (self.context_builder)(self.tiling_spec, self.num_threads);
        let ctx = exe_ctx.mock_query_context(self.chunk_byte_size);

        let operator = (self.operator_builder)(self.tiling_spec, self.query_rect);
        let init_start = Instant::now();
        let initialized_operator = run_time.block_on(async { operator.initialize(&exe_ctx).await });
        let init_elapsed = init_start.elapsed();

        let initialized_queryprocessor = initialized_operator.unwrap().query_processor().unwrap();

        call_on_generic_raster_processor!(initialized_queryprocessor, op => { run_time.block_on(async {
                let start_query = Instant::now();
                // query the operator
                let query = op
                    .raster_query(self.query_rect, &ctx)
                    .await
                    .unwrap();
                let query_elapsed = start_query.elapsed();

                let start = Instant::now();
                // drain the stream
                let tile_count_res: Result<usize,_> = query.try_fold(0, |accu, tile|  async move {
                    black_box({
                        let _ = tile;
                        Ok(accu + 1)
                    })
                }).await;

                let elapsed = start.elapsed();
                let number_of_tiles = tile_count_res.unwrap();

                WorkflowBenchmarkResult{
                    bench_id: self.bench_id,
                    num_threads: self.num_threads,
                    chunk_byte_size: self.chunk_byte_size,
                    qrect_name: self.query_name,
                    tile_size_x: self.tiling_spec.tile_size_in_pixels.axis_size_x(),
                    tile_size_y: self.tiling_spec.tile_size_in_pixels.axis_size_y(),
                    number_of_tiles,
                    init_time: init_elapsed,
                    query_time: query_elapsed,
                    stream_time: elapsed,
                }
            })
        })
    }
}

impl<F, O> BenchmarkRunner for WorkflowSingleBenchmark<F, O>
where
    F: Fn(TilingSpecification, usize) -> MockExecutionContext,
    O: Fn(TilingSpecification, QueryRectangle<SpatialPartition2D>) -> Box<dyn RasterOperator>,
{
    fn run_all_benchmarks(self, bencher: &mut WorkflowBenchmarkCollector) {
        bencher.add_benchmark_result(WorkflowSingleBenchmark::run_bench(&self))
    }
}

pub struct WorkflowMultiBenchmark<C, Q, T, B, O, F> {
    bench_id: &'static str,
    named_querys: Q,
    tiling_specs: T,
    operator_builder: O,
    context_builder: F,
    chunk_byte_size: B,
    num_threads: C,
}

impl<C, Q, T, B, O, F> WorkflowMultiBenchmark<C, Q, T, B, O, F>
where
    C: IntoIterator<Item = usize> + Clone,
    Q: IntoIterator<Item = (&'static str, RasterQueryRectangle)> + Clone,
    T: IntoIterator<Item = TilingSpecification> + Clone,
    B: IntoIterator<Item = ChunkByteSize> + Clone,
    F: Clone + Fn(TilingSpecification, usize) -> MockExecutionContext,
    O: Clone
        + Fn(TilingSpecification, QueryRectangle<SpatialPartition2D>) -> Box<dyn RasterOperator>,
{
    pub fn new(
        bench_id: &'static str,
        named_querys: Q,
        tiling_specs: T,
        operator_builder: O,
        context_builder: F,
        chunk_byte_size: B,
        num_threads: C,
    ) -> WorkflowMultiBenchmark<C, Q, T, B, O, F> {
        WorkflowMultiBenchmark {
            bench_id,
            named_querys,
            tiling_specs,
            operator_builder,
            context_builder,
            chunk_byte_size,
            num_threads,
        }
    }

    pub fn into_benchmark_iterator(self) -> impl Iterator<Item = WorkflowSingleBenchmark<F, O>> {
        let iter = self
            .num_threads
            .clone()
            .into_iter()
            .flat_map(move |num_threads| {
                self.tiling_specs
                    .clone()
                    .into_iter()
                    .map(move |tiling_spec| (num_threads, tiling_spec))
            });

        let iter = iter.flat_map(move |(num_threads, tiling_spec)| {
            self.chunk_byte_size
                .clone()
                .into_iter()
                .map(move |chunk_byte_size| (num_threads, tiling_spec, chunk_byte_size))
        });

        let iter = iter.flat_map(move |(num_threads, tiling_spec, chunk_byte_size)| {
            self.named_querys
                .clone()
                .into_iter()
                .map(move |(query_name, query_rect)| {
                    (
                        num_threads,
                        tiling_spec,
                        chunk_byte_size,
                        query_name,
                        query_rect,
                    )
                })
        });

        iter.map(
            move |(num_threads, tiling_spec, chunk_byte_size, query_name, query_rect)| {
                WorkflowSingleBenchmark {
                    bench_id: self.bench_id,
                    query_name,
                    query_rect,
                    tiling_spec,
                    chunk_byte_size,
                    num_threads,
                    operator_builder: self.operator_builder.clone(),
                    context_builder: self.context_builder.clone(),
                }
            },
        )
    }
}

impl<C, Q, T, B, O, F> BenchmarkRunner for WorkflowMultiBenchmark<C, Q, T, B, O, F>
where
    C: IntoIterator<Item = usize> + Clone,
    Q: IntoIterator<Item = (&'static str, RasterQueryRectangle)> + Clone,
    T: IntoIterator<Item = TilingSpecification> + Clone,
    B: IntoIterator<Item = ChunkByteSize> + Clone,
    F: Clone + Fn(TilingSpecification, usize) -> MockExecutionContext,
    O: Clone
        + Fn(TilingSpecification, QueryRectangle<SpatialPartition2D>) -> Box<dyn RasterOperator>,
{
    fn run_all_benchmarks(self, bencher: &mut WorkflowBenchmarkCollector) {
        self.into_benchmark_iterator()
            .for_each(|bench| bencher.add_benchmark_result(bench.run_bench()));
    }
}

fn serialize_duration_as_millis<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_u128(duration.as_millis())
}

#[derive(Debug, Serialize)]
pub struct WorkflowBenchmarkResult {
    pub bench_id: &'static str,
    pub num_threads: usize,
    pub chunk_byte_size: ChunkByteSize,
    pub qrect_name: &'static str,
    pub tile_size_x: usize,
    pub tile_size_y: usize,
    pub number_of_tiles: usize,
    #[serde(rename = "init_time_ms")]
    #[serde(serialize_with = "serialize_duration_as_millis")]
    pub init_time: Duration,
    #[serde(rename = "query_time_ms")]
    #[serde(serialize_with = "serialize_duration_as_millis")]
    pub query_time: Duration,
    #[serde(rename = "stream_time_ms")]
    #[serde(serialize_with = "serialize_duration_as_millis")]
    pub stream_time: Duration,
}
