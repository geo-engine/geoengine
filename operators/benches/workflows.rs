use std::hint::black_box;
use std::time::{Duration, Instant};

use futures::TryStreamExt;
use geoengine_datatypes::dataset::{DataId, DatasetId};
use geoengine_datatypes::primitives::{
    Measurement, QueryRectangle, RasterQueryRectangle, SpatialPartitioned,
};
use geoengine_datatypes::raster::{Grid2D, RasterDataType};
use geoengine_datatypes::spatial_reference::SpatialReference;

use geoengine_datatypes::util::Identifier;
use geoengine_datatypes::{
    primitives::{SpatialPartition2D, SpatialResolution, TimeInterval},
    raster::{GridSize, RasterTile2D, TilingSpecification},
};
use geoengine_operators::call_on_generic_raster_processor;
use geoengine_operators::engine::{MetaData, RasterResultDescriptor, SingleRasterOrVectorSource};
use geoengine_operators::mock::{MockRasterSource, MockRasterSourceParams};
use geoengine_operators::processing::{
    Expression, ExpressionParams, ExpressionSources, Reprojection, ReprojectionParams,
};
use geoengine_operators::source::GdalSource;
use geoengine_operators::{
    engine::{ChunkByteSize, MockExecutionContext, RasterOperator, RasterQueryProcessor},
    source::GdalSourceParameters,
    util::gdal::create_ndvi_meta_data,
};

use serde::{Serialize, Serializer};

pub struct BenchmarkCollector {
    pub writer: csv::Writer<std::io::Stdout>,
}

impl BenchmarkCollector {
    pub fn add_benchmark_result(&mut self, result: WorkflowBenchmarkResult) {
        self.writer.serialize(result).unwrap();
        self.writer.flush().unwrap();
    }
}

impl Default for BenchmarkCollector {
    fn default() -> Self {
        BenchmarkCollector {
            writer: csv::Writer::from_writer(std::io::stdout()),
        }
    }
}

pub trait BenchmarkRunner {
    fn run_all_benchmarks(self, bencher: &mut BenchmarkCollector);
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
    fn run_all_benchmarks(self, bencher: &mut BenchmarkCollector) {
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
    fn run_all_benchmarks(self, bencher: &mut BenchmarkCollector) {
        self.into_benchmark_iterator()
            .for_each(|bench| bencher.add_benchmark_result(bench.run_bench()));
    }
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

fn serialize_duration_as_millis<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_u128(duration.as_millis())
}

fn bench_mock_source_operator(bench_collector: &mut BenchmarkCollector) {
    let qrect = RasterQueryRectangle {
        spatial_bounds: SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap(),
        time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        spatial_resolution: SpatialResolution::new(0.01, 0.01).unwrap(),
    };
    let tiling_spec = TilingSpecification::new((0., 0.).into(), [512, 512].into());

    let qrects = vec![("World in 36000x18000 pixels", qrect)];
    let tiling_specs = vec![tiling_spec];

    fn operator_builder(
        tiling_spec: TilingSpecification,
        query_rect: RasterQueryRectangle,
    ) -> Box<dyn RasterOperator> {
        let query_resolution = query_rect.spatial_resolution;
        let query_time = query_rect.time_interval;
        let tileing_strategy = tiling_spec.strategy(query_resolution.x, -1. * query_resolution.y);
        let tile_iter = tileing_strategy.tile_information_iterator(query_rect.spatial_partition());

        let mock_data = tile_iter
            .enumerate()
            .map(|(id, tile_info)| {
                let data = Grid2D::new(
                    tiling_spec.tile_size_in_pixels,
                    vec![(id % 255) as u8; tile_info.tile_size_in_pixels.number_of_elements()],
                )
                .unwrap();
                RasterTile2D::new_with_tile_info(query_time, tile_info, data.into())
            })
            .collect();

        MockRasterSource {
            params: MockRasterSourceParams {
                data: mock_data,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        }
        .boxed()
    }

    WorkflowMultiBenchmark::new(
        "mock_source",
        qrects,
        tiling_specs,
        operator_builder,
        |ts, num_threads| {
            MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads)
        },
        [ChunkByteSize::MAX],
        [4, 8, 16, 32],
    )
    .run_all_benchmarks(bench_collector);
}

fn bench_mock_source_operator_with_expression(bench_collector: &mut BenchmarkCollector) {
    let qrect = RasterQueryRectangle {
        spatial_bounds: SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap(),
        time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        spatial_resolution: SpatialResolution::new(0.005, 0.005).unwrap(),
    };

    let qrects = vec![("World in 72000x36000 pixels", qrect)];
    let tiling_specs = vec![
        TilingSpecification::new((0., 0.).into(), [512, 512].into()),
        TilingSpecification::new((0., 0.).into(), [1024, 1024].into()),
        TilingSpecification::new((0., 0.).into(), [2048, 2048].into()),
        TilingSpecification::new((0., 0.).into(), [4096, 4096].into()),
        TilingSpecification::new((0., 0.).into(), [9000, 9000].into()),
        TilingSpecification::new((0., 0.).into(), [18000, 18000].into()),
    ];

    fn operator_builder(
        tiling_spec: TilingSpecification,
        query_rect: RasterQueryRectangle,
    ) -> Box<dyn RasterOperator> {
        let query_resolution = query_rect.spatial_resolution;
        let query_time = query_rect.time_interval;
        let tileing_strategy = tiling_spec.strategy(query_resolution.x, -1. * query_resolution.y);
        let tile_iter = tileing_strategy.tile_information_iterator(query_rect.spatial_partition());

        let mock_data = tile_iter
            .enumerate()
            .map(|(id, tile_info)| {
                let data = Grid2D::new(
                    tiling_spec.tile_size_in_pixels,
                    vec![(id % 255) as u8; tile_info.tile_size_in_pixels.number_of_elements()],
                )
                .unwrap();
                RasterTile2D::new_with_tile_info(query_time, tile_info, data.into())
            })
            .collect();

        let mock_raster_operator = MockRasterSource {
            params: MockRasterSourceParams {
                data: mock_data,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        };

        Expression {
            params: ExpressionParams {
                expression: "A+B".to_string(),
                output_type: RasterDataType::U8,
                output_measurement: Some(Measurement::Unitless),
                map_no_data: false,
            },
            sources: ExpressionSources::new_a_b(
                mock_raster_operator.clone().boxed(),
                mock_raster_operator.boxed(),
            ),
        }
        .boxed()
    }

    WorkflowMultiBenchmark::new(
        "mock_source_with_expression_a+b",
        qrects,
        tiling_specs,
        operator_builder,
        |ts, num_threads| {
            MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads)
        },
        [ChunkByteSize::MAX],
        [1, 2, 4, 8, 16, 32],
    )
    .run_all_benchmarks(bench_collector);
}

fn bench_mock_source_operator_with_identity_reprojection(bench_collector: &mut BenchmarkCollector) {
    let qrect = RasterQueryRectangle {
        spatial_bounds: SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap(),
        time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        spatial_resolution: SpatialResolution::new(0.01, 0.01).unwrap(),
    };

    let qrects = vec![("World in 36000x18000 pixels", qrect)];
    let tiling_specs = vec![
        TilingSpecification::new((0., 0.).into(), [512, 512].into()),
        TilingSpecification::new((0., 0.).into(), [1024, 1024].into()),
        TilingSpecification::new((0., 0.).into(), [2048, 2048].into()),
        TilingSpecification::new((0., 0.).into(), [4096, 4096].into()),
        TilingSpecification::new((0., 0.).into(), [9000, 9000].into()),
        TilingSpecification::new((0., 0.).into(), [18000, 18000].into()),
    ];

    fn operator_builder(
        tiling_spec: TilingSpecification,
        query_rect: RasterQueryRectangle,
    ) -> Box<dyn RasterOperator> {
        let query_resolution = query_rect.spatial_resolution;
        let query_time = query_rect.time_interval;
        let tileing_strategy = tiling_spec.strategy(query_resolution.x, -1. * query_resolution.y);
        let tile_iter = tileing_strategy.tile_information_iterator(query_rect.spatial_partition());
        let mock_data = tile_iter
            .enumerate()
            .map(|(id, tile_info)| {
                let data = Grid2D::new(
                    tiling_spec.tile_size_in_pixels,
                    vec![(id % 255) as u8; tile_info.tile_size_in_pixels.number_of_elements()],
                )
                .unwrap();
                RasterTile2D::new_with_tile_info(query_time, tile_info, data.into())
            })
            .collect();

        let mock_raster_operator = MockRasterSource {
            params: MockRasterSourceParams {
                data: mock_data,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        };

        Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: SpatialReference::epsg_4326(),
            },
            sources: SingleRasterOrVectorSource::from(mock_raster_operator.boxed()),
        }
        .boxed()
    }

    WorkflowMultiBenchmark::new(
        "mock_source_512px_with_identity_reprojection",
        qrects,
        tiling_specs,
        operator_builder,
        |ts, num_threads| {
            MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads)
        },
        [ChunkByteSize::MAX],
        [4, 8, 16, 32],
    )
    .run_all_benchmarks(bench_collector);
}

fn bench_mock_source_operator_with_4326_to_3857_reprojection(
    bench_collector: &mut BenchmarkCollector,
) {
    let qrect = RasterQueryRectangle {
        spatial_bounds: SpatialPartition2D::new(
            (-20_037_508.342_789_244, 20_048_966.104_014_594).into(),
            (20_037_508.342_789_244, -20_048_966.104_014_594).into(),
        )
        .unwrap(),
        time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        spatial_resolution: SpatialResolution::new(1050., 2100.).unwrap(),
    };
    let tiling_spec = TilingSpecification::new((0., 0.).into(), [512, 512].into());

    let qrects = vec![("World in 36000x18000 pixels", qrect)];
    let tiling_specs = vec![tiling_spec];

    fn operator_builder(
        tiling_spec: TilingSpecification,
        query_rect: RasterQueryRectangle,
    ) -> Box<dyn RasterOperator> {
        let query_resolution = query_rect.spatial_resolution;
        let query_time = query_rect.time_interval;
        let tileing_strategy = tiling_spec.strategy(query_resolution.x, -1. * query_resolution.y);
        let tile_iter = tileing_strategy.tile_information_iterator(query_rect.spatial_partition());
        let mock_data = tile_iter
            .enumerate()
            .map(|(id, tile_info)| {
                let data = Grid2D::new(
                    tiling_spec.tile_size_in_pixels,
                    vec![(id % 255) as u8; tile_info.tile_size_in_pixels.number_of_elements()],
                )
                .unwrap();
                RasterTile2D::new_with_tile_info(query_time, tile_info, data.into())
            })
            .collect();
        let mock_raster_operator = MockRasterSource {
            params: MockRasterSourceParams {
                data: mock_data,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        };

        Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: SpatialReference::epsg_4326(),
            },
            sources: SingleRasterOrVectorSource::from(mock_raster_operator.boxed()),
        }
        .boxed()
    }

    WorkflowMultiBenchmark::new(
        "mock_source projection 4326 -> 3857",
        qrects,
        tiling_specs,
        operator_builder,
        |ts, num_threads| {
            MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads)
        },
        [ChunkByteSize::MAX],
        [4, 8, 16, 32],
    )
    .run_all_benchmarks(bench_collector);
}

fn bench_gdal_source_operator_tile_size(bench_collector: &mut BenchmarkCollector) {
    let qrects = vec![
        (
            "World in 36000x18000 pixels",
            RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into())
                    .unwrap(),
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::new(0.01, 0.01).unwrap(),
            },
        ),
        (
            "World in 72000x36000 pixels",
            RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into())
                    .unwrap(),
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::new(0.005, 0.005).unwrap(),
            },
        ),
    ];

    let tiling_specs = vec![
        TilingSpecification::new((0., 0.).into(), [32, 32].into()),
        TilingSpecification::new((0., 0.).into(), [64, 64].into()),
        TilingSpecification::new((0., 0.).into(), [128, 128].into()),
        TilingSpecification::new((0., 0.).into(), [256, 256].into()),
        TilingSpecification::new((0., 0.).into(), [512, 512].into()),
        // TilingSpecification::new((0., 0.).into(), [600, 600].into()),
        // TilingSpecification::new((0., 0.).into(), [900, 900].into()),
        TilingSpecification::new((0., 0.).into(), [1024, 1024].into()),
        TilingSpecification::new((0., 0.).into(), [2048, 2048].into()),
        TilingSpecification::new((0., 0.).into(), [4096, 4096].into()),
        // TilingSpecification::new((0., 0.).into(), [9000, 9000].into()),
    ];

    let id: DataId = DatasetId::new().into();
    let meta_data = create_ndvi_meta_data();

    let gdal_operator = GdalSource {
        params: GdalSourceParameters { data: id.clone() },
    }
    .boxed();

    WorkflowMultiBenchmark::new(
        "gdal_source",
        qrects,
        tiling_specs,
        |_, _| gdal_operator.clone(),
        |ts, num_threads| {
            let mut mex =
                MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads);
            mex.add_meta_data(id.clone(), meta_data.box_clone());
            mex
        },
        [ChunkByteSize::MAX],
        [8],
    )
    .run_all_benchmarks(bench_collector);
}

fn bench_gdal_source_operator_with_expression_tile_size(bench_collector: &mut BenchmarkCollector) {
    let qrects = vec![(
        "World in 36000x18000 pixels",
        RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into())
                .unwrap(),
            time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            spatial_resolution: SpatialResolution::new(0.01, 0.01).unwrap(),
        },
    )];

    let tiling_specs = vec![
        // TilingSpecification::new((0., 0.).into(), [32, 32].into()),
        TilingSpecification::new((0., 0.).into(), [64, 64].into()),
        TilingSpecification::new((0., 0.).into(), [128, 128].into()),
        TilingSpecification::new((0., 0.).into(), [256, 256].into()),
        TilingSpecification::new((0., 0.).into(), [512, 512].into()),
        // TilingSpecification::new((0., 0.).into(), [600, 600].into()),
        // TilingSpecification::new((0., 0.).into(), [900, 900].into()),
        TilingSpecification::new((0., 0.).into(), [1024, 1024].into()),
        TilingSpecification::new((0., 0.).into(), [2048, 2048].into()),
        TilingSpecification::new((0., 0.).into(), [4096, 4096].into()),
        // TilingSpecification::new((0., 0.).into(), [9000, 9000].into()),
    ];

    let id: DataId = DatasetId::new().into();
    let meta_data = create_ndvi_meta_data();

    let gdal_operator = GdalSource {
        params: GdalSourceParameters { data: id.clone() },
    };

    let expression_operator = Expression {
        params: ExpressionParams {
            expression: "A+B".to_string(),
            output_type: RasterDataType::U8,
            output_measurement: Some(Measurement::Unitless),
            map_no_data: false,
        },
        sources: ExpressionSources::new_a_b(gdal_operator.clone().boxed(), gdal_operator.boxed()),
    }
    .boxed();

    WorkflowMultiBenchmark::new(
        "expression a+b",
        qrects,
        tiling_specs,
        |_, _| expression_operator.clone(),
        |ts, num_threads| {
            let mut mex =
                MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads);
            mex.add_meta_data(id.clone(), meta_data.box_clone());
            mex
        },
        [ChunkByteSize::MAX],
        [8],
    )
    .run_all_benchmarks(bench_collector);
}

fn bench_gdal_source_operator_with_identity_reprojection(bench_collector: &mut BenchmarkCollector) {
    let qrects = vec![(
        "World in 36000x18000 pixels",
        RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into())
                .unwrap(),
            time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            spatial_resolution: SpatialResolution::new(0.01, 0.01).unwrap(),
        },
    )];

    let tiling_specs = vec![
        // TilingSpecification::new((0., 0.).into(), [32, 32].into()),
        // TilingSpecification::new((0., 0.).into(), [64, 64].into()),
        // TilingSpecification::new((0., 0.).into(), [128, 128].into()),
        TilingSpecification::new((0., 0.).into(), [256, 256].into()),
        TilingSpecification::new((0., 0.).into(), [512, 512].into()),
        // TilingSpecification::new((0., 0.).into(), [600, 600].into()),
        // TilingSpecification::new((0., 0.).into(), [900, 900].into()),
        TilingSpecification::new((0., 0.).into(), [1024, 1024].into()),
        TilingSpecification::new((0., 0.).into(), [2048, 2048].into()),
        TilingSpecification::new((0., 0.).into(), [4096, 4096].into()),
        // TilingSpecification::new((0., 0.).into(), [9000, 9000].into()),
    ];

    let id: DataId = DatasetId::new().into();
    let meta_data = create_ndvi_meta_data();

    let gdal_operator = GdalSource {
        params: GdalSourceParameters { data: id.clone() },
    };

    let projection_operator = Reprojection {
        params: ReprojectionParams {
            target_spatial_reference: SpatialReference::epsg_4326(),
        },
        sources: SingleRasterOrVectorSource::from(gdal_operator.boxed()),
    }
    .boxed();

    WorkflowMultiBenchmark::new(
        "gdal source + projection identity",
        qrects,
        tiling_specs,
        |_, _| projection_operator.clone(),
        |ts, num_threads| {
            let mut mex =
                MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads);
            mex.add_meta_data(id.clone(), meta_data.box_clone());
            mex
        },
        [ChunkByteSize::MAX],
        [8],
    )
    .run_all_benchmarks(bench_collector);
}

fn bench_gdal_source_operator_with_4326_to_3857_reprojection(
    bench_collector: &mut BenchmarkCollector,
) {
    let qrects = vec![(
        "World in EPSG:3857 ~ 40000 x 20000 px",
        RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new(
                (-20_037_508.342_789_244, 20_048_966.104_014_594).into(),
                (20_037_508.342_789_244, -20_048_966.104_014_594).into(),
            )
            .unwrap(),
            time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            spatial_resolution: SpatialResolution::new(1050., 2100.).unwrap(),
        },
    )];

    let tiling_specs = vec![
        // TilingSpecification::new((0., 0.).into(), [32, 32].into()),
        // TilingSpecification::new((0., 0.).into(), [64, 64].into()),
        // TilingSpecification::new((0., 0.).into(), [128, 128].into()),
        // TilingSpecification::new((0., 0.).into(), [256, 256].into()),
        TilingSpecification::new((0., 0.).into(), [512, 512].into()),
        // TilingSpecification::new((0., 0.).into(), [600, 600].into()),
        // TilingSpecification::new((0., 0.).into(), [900, 900].into()),
        TilingSpecification::new((0., 0.).into(), [1024, 1024].into()),
        TilingSpecification::new((0., 0.).into(), [2048, 2048].into()),
        TilingSpecification::new((0., 0.).into(), [4096, 4096].into()),
        // TilingSpecification::new((0., 0.).into(), [9000, 9000].into()),
    ];

    let id: DataId = DatasetId::new().into();
    let meta_data = create_ndvi_meta_data();

    let gdal_operator = GdalSource {
        params: GdalSourceParameters { data: id.clone() },
    };

    let projection_operator = Reprojection {
        params: ReprojectionParams {
            target_spatial_reference: SpatialReference::new(
                geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
                3857,
            ),
        },
        sources: SingleRasterOrVectorSource::from(gdal_operator.boxed()),
    }
    .boxed();

    WorkflowMultiBenchmark::new(
        "projection 4326 -> 3857",
        qrects,
        tiling_specs,
        |_, _| projection_operator.clone(),
        |ts, num_threads| {
            let mut mex =
                MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads);
            mex.add_meta_data(id.clone(), meta_data.box_clone());
            mex
        },
        [ChunkByteSize::MAX],
        [1, 4, 8, 16, 32],
    )
    .run_all_benchmarks(bench_collector);
}

fn main() {
    let mut bench_collector = BenchmarkCollector::default();

    bench_mock_source_operator(&mut bench_collector);
    bench_mock_source_operator_with_expression(&mut bench_collector);
    bench_mock_source_operator_with_identity_reprojection(&mut bench_collector);
    bench_mock_source_operator_with_4326_to_3857_reprojection(&mut bench_collector);
    bench_gdal_source_operator_tile_size(&mut bench_collector);
    bench_gdal_source_operator_with_expression_tile_size(&mut bench_collector);
    bench_gdal_source_operator_with_identity_reprojection(&mut bench_collector);
    bench_gdal_source_operator_with_4326_to_3857_reprojection(&mut bench_collector);
}
