#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::missing_panics_doc
)] // okay in benchmarks

use futures::TryStreamExt;

use geoengine_datatypes::primitives::Coordinate2D;
use geoengine_datatypes::primitives::RasterQueryRectangle;
use geoengine_datatypes::primitives::{BandSelection, CacheHint};
use geoengine_datatypes::raster::RenameBands;
use geoengine_datatypes::raster::{
    GeoTransform, Grid2D, GridBoundingBox2D, RasterDataType, TilingStrategy,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::SpatialGridDescriptor;
use std::hint::black_box;
use std::time::{Duration, Instant};

use geoengine_datatypes::{
    primitives::TimeInterval,
    raster::{GridSize, RasterTile2D, TilingSpecification},
};
use geoengine_operators::call_on_generic_raster_processor;
use geoengine_operators::engine::{
    ChunkByteSize, MockExecutionContext, RasterOperator, RasterQueryProcessor,
};
use geoengine_operators::engine::{
    MultipleRasterSources, RasterBandDescriptors, RasterResultDescriptor, SingleRasterSource,
    WorkflowOperatorPath,
};
use geoengine_operators::mock::{MockRasterSource, MockRasterSourceParams};
use geoengine_operators::processing::{
    Expression, ExpressionParams, RasterStacker, RasterStackerParams,
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
    query_rect: RasterQueryRectangle,
    tiling_spec: TilingSpecification,
    chunk_byte_size: ChunkByteSize,
    num_threads: usize,
    operator_builder: O,
    context_builder: F,
}

impl<O, F> WorkflowSingleBenchmark<F, O>
where
    F: Fn(TilingSpecification, usize) -> MockExecutionContext,
    O: Fn(TilingSpecification, RasterQueryRectangle) -> Box<dyn RasterOperator>,
{
    #[inline(never)]
    pub fn run_bench(&self) -> WorkflowBenchmarkResult {
        let run_time = tokio::runtime::Runtime::new().unwrap();
        let exe_ctx = (self.context_builder)(self.tiling_spec, self.num_threads);
        let ctx = exe_ctx.mock_query_context(self.chunk_byte_size);

        let operator = (self.operator_builder)(self.tiling_spec, self.query_rect.clone());
        let init_start = Instant::now();
        let initialized_operator = run_time.block_on(async {
            operator
                .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
                .await
        });
        let init_elapsed = init_start.elapsed();

        let initialized_queryprocessor = initialized_operator.unwrap().query_processor().unwrap();

        call_on_generic_raster_processor!(initialized_queryprocessor, op => { run_time.block_on(async {
                let start_query = Instant::now();
                // query the operator
                let query = op
                    .raster_query(self.query_rect.clone(), &ctx)
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
    O: Fn(TilingSpecification, RasterQueryRectangle) -> Box<dyn RasterOperator>,
{
    fn run_all_benchmarks(self, bencher: &mut BenchmarkCollector) {
        bencher.add_benchmark_result(WorkflowSingleBenchmark::run_bench(&self));
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
    O: Clone + Fn(TilingSpecification, RasterQueryRectangle) -> Box<dyn RasterOperator>,
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
    O: Clone + Fn(TilingSpecification, RasterQueryRectangle) -> Box<dyn RasterOperator>,
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
    let tiling_origin = Coordinate2D::new(0., 0.);

    let qrect = RasterQueryRectangle::new_with_grid_bounds(
        GridBoundingBox2D::new([-9000, -18000], [8999, 17999]).unwrap(),
        TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        BandSelection::first(),
    );
    let tiling_spec = TilingSpecification::new([512, 512].into());

    let qrects = [("World in 36000x18000 pixels", qrect)];
    let tiling_specs = [tiling_spec];

    #[allow(clippy::needless_pass_by_value)] // must match signature
    fn operator_builder(
        tiling_spec: TilingSpecification,
        query_rect: RasterQueryRectangle,
    ) -> Box<dyn RasterOperator> {
        let tileing_strategy = TilingStrategy::new(
            tiling_spec.tile_size_in_pixels,
            GeoTransform::new(Coordinate2D::new(0.0, 0.), 0.01, -0.01),
        );
        let tile_iter = tileing_strategy
            .tile_information_iterator_from_grid_bounds(query_rect.spatial_query().grid_bounds());

        let mock_data = tile_iter
            .enumerate()
            .map(|(id, tile_info)| {
                let data = Grid2D::new(
                    tiling_spec.tile_size_in_pixels,
                    vec![(id % 255) as u8; tile_info.tile_size_in_pixels.number_of_elements()],
                )
                .unwrap();
                RasterTile2D::new_with_tile_info(
                    query_rect.time_interval,
                    tile_info,
                    0,
                    data.into(),
                    CacheHint::default(),
                )
            })
            .collect();

        MockRasterSource {
            params: MockRasterSourceParams {
                data: mock_data,
                result_descriptor: RasterResultDescriptor::new(
                    RasterDataType::U8,
                    SpatialReference::epsg_4326().into(),
                    None,
                    SpatialGridDescriptor::source_from_parts(
                        tileing_strategy.geo_transform,
                        query_rect.spatial_query().grid_bounds(),
                    ),
                    RasterBandDescriptors::new_single_band(),
                ),
            },
        }
        .boxed()
    }

    let qrect = RasterQueryRectangle::new_with_grid_bounds(
        GridBoundingBox2D::new([-18000, -9000], [17999, 8999]).unwrap(),
        TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        BandSelection::first(),
    );
    let tiling_spec = TilingSpecification::new([512, 512].into());

    let qrects = vec![("World in 36000x18000 pixels", qrect)];
    let tiling_specs = vec![tiling_spec];

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
    let tiling_origin = Coordinate2D::new(0., 0.);

    let qrect = RasterQueryRectangle::new_with_grid_bounds(
        GridBoundingBox2D::new([-9000, -18000], [8999, 17999]).unwrap(),
        TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        BandSelection::first(),
    );

    let qrects = [("World in 72000x36000 pixels", qrect)];
    let tiling_specs = [TilingSpecification::new([512, 512].into()),
        TilingSpecification::new([1024, 1024].into()),
        TilingSpecification::new([2048, 2048].into()),
        TilingSpecification::new([4096, 4096].into()),
        TilingSpecification::new([9000, 9000].into()),
        TilingSpecification::new([18000, 18000].into())];

    #[allow(clippy::needless_pass_by_value)] // must match signature
    fn operator_builder(
        tiling_spec: TilingSpecification,
        query_rect: RasterQueryRectangle,
    ) -> Box<dyn RasterOperator> {
        let tileing_strategy = TilingStrategy::new(
            tiling_spec.tile_size_in_pixels,
            GeoTransform::new(Coordinate2D::new(0.0, 0.), 0.01, -0.01),
        );
        let tile_iter = tileing_strategy
            .tile_information_iterator_from_grid_bounds(query_rect.spatial_query().grid_bounds());

        let mock_data = tile_iter
            .enumerate()
            .map(|(id, tile_info)| {
                let data = Grid2D::new(
                    tiling_spec.tile_size_in_pixels,
                    vec![(id % 255) as u8; tile_info.tile_size_in_pixels.number_of_elements()],
                )
                .unwrap();
                RasterTile2D::new_with_tile_info(
                    query_rect.time_interval,
                    tile_info,
                    0,
                    data.into(),
                    CacheHint::default(),
                )
            })
            .collect();

        let mock_raster_operator = MockRasterSource {
            params: MockRasterSourceParams {
                data: mock_data,
                result_descriptor: RasterResultDescriptor::new(
                    RasterDataType::U8,
                    SpatialReference::epsg_4326().into(),
                    None,
                    SpatialGridDescriptor::source_from_parts(
                        tileing_strategy.geo_transform,
                        query_rect.spatial_query().grid_bounds(),
                    ),
                    RasterBandDescriptors::new_single_band(),
                ),
            },
        };

        Expression {
            params: ExpressionParams {
                expression: "A+B".to_string(),
                output_type: RasterDataType::U8,
                output_band: None,
                map_no_data: false,
            },
            sources: SingleRasterSource {
                raster: RasterStacker {
                    params: RasterStackerParams {
                        rename_bands: RenameBands::Default,
                    },
                    sources: MultipleRasterSources {
                        rasters: vec![
                            mock_raster_operator.clone().boxed(),
                            mock_raster_operator.boxed(),
                        ],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
    }

    let qrect = RasterQueryRectangle::new_with_grid_bounds(
        GridBoundingBox2D::new([-18000, -9000], [17999, 8999]).unwrap(),
        TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        BandSelection::first(),
    );

    let qrects = vec![("World in 72000x36000 pixels", qrect)];
    let tiling_specs = vec![
        TilingSpecification::new([512, 512].into()),
        TilingSpecification::new([1024, 1024].into()),
        TilingSpecification::new([2048, 2048].into()),
        TilingSpecification::new([4096, 4096].into()),
        TilingSpecification::new([9000, 9000].into()),
        TilingSpecification::new([18000, 18000].into()),
    ];

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

/*

fn bench_mock_source_operator_with_identity_reprojection(bench_collector: &mut BenchmarkCollector) {
    let tiling_origin = Coordinate2D::new(0., 0.);

    let qrect = RasterQueryRectangle::new_with_grid_bounds(
        GridBoundingBox2D::new([-9000, -18000], [8999, 17999]).unwrap(),
        TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        BandSelection::first(),
    );

    let qrects = vec![("World in 36000x18000 pixels", qrect)];
    let tiling_specs = vec![
        TilingSpecification::new([512, 512].into()),
        TilingSpecification::new([1024, 1024].into()),
        TilingSpecification::new([2048, 2048].into()),
        TilingSpecification::new([4096, 4096].into()),
        TilingSpecification::new([9000, 9000].into()),
        TilingSpecification::new([18000, 18000].into()),
    ];

    #[allow(clippy::needless_pass_by_value)] // must match signature
    fn operator_builder(
        tiling_spec: TilingSpecification,
        query_rect: RasterQueryRectangle,
    ) -> Box<dyn RasterOperator> {
        let tileing_strategy = TilingStrategy::new(
            tiling_spec.tile_size_in_pixels,
            GeoTransform::new(Coordinate2D::new(0.0, 0.), 0.01, -0.01),
        );
        let tile_iter = tileing_strategy
            .tile_information_iterator_from_grid_bounds(query_rect.spatial_query().grid_bounds());
        let mock_data = tile_iter
            .enumerate()
            .map(|(id, tile_info)| {
                let data = Grid2D::new(
                    tiling_spec.tile_size_in_pixels,
                    vec![(id % 255) as u8; tile_info.tile_size_in_pixels.number_of_elements()],
                )
                .unwrap();
                RasterTile2D::new_with_tile_info(
                    query_rect.time_interval,
                    tile_info,
                    0,
                    data.into(),
                    CacheHint::default(),
                )
            })
            .collect();

        let mock_raster_operator = MockRasterSource {
            params: MockRasterSourceParams {
                data: mock_data,
                result_descriptor: RasterResultDescriptor::new(
                    RasterDataType::U8,
                    SpatialReference::epsg_4326().into(),
                    None,
                    tileing_strategy.geo_transform,
                    query_rect.spatial_query().grid_bounds(),
                    RasterBandDescriptors::new_single_band(),
                ),
            },
        };

        Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: SpatialReference::epsg_4326(),
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource::from(mock_raster_operator.boxed()),
        }
        .boxed()
    }

    let qrect = RasterQueryRectangle {
        spatial_bounds: SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap(),
        time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        spatial_resolution: SpatialResolution::new(0.01, 0.01).unwrap(),
        attributes: BandSelection::first(),
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
    let qrect = SpatialPartition2D::new(
        (-20_037_508.342_789_244, 20_048_966.104_014_594).into(),
        (20_037_508.342_789_244, -20_048_966.104_014_594).into(),
    )
    .unwrap();

    let qtime = TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap();
    let qband = BandSelection::first();

    let tiling_spec = TilingSpecification::new([512, 512].into());

    let qrects = vec![("World in 36000x18000 pixels", qrect)];
    let tiling_specs = vec![tiling_spec];

    #[allow(clippy::needless_pass_by_value)] // must match signature
    fn operator_builder(
        tiling_spec: TilingSpecification,
        query_rect: RasterQueryRectangle,
    ) -> Box<dyn RasterOperator> {
        // FIXME: The query origin must match the tiling strategy's origin for now. Also use grid bounds not spatial bounds.

        let tileing_strategy = TilingStrategy::new(
            tiling_spec.tile_size_in_pixels,
            GeoTransform::new(Coordinate2D::new(0., 0.), 0.01, -0.01),
        );

        let tile_iter = tileing_strategy
            .tile_information_iterator_from_grid_bounds(query_rect.spatial_query().grid_bounds());
        let mock_data = tile_iter
            .enumerate()
            .map(|(id, tile_info)| {
                let data = Grid2D::new(
                    tiling_spec.tile_size_in_pixels,
                    vec![(id % 255) as u8; tile_info.tile_size_in_pixels.number_of_elements()],
                )
                .unwrap();
                RasterTile2D::new_with_tile_info(
                    query_rect.time_interval,
                    tile_info,
                    0,
                    data.into(),
                    CacheHint::default(),
                )
            })
            .collect();
        let mock_raster_operator = MockRasterSource {
            params: MockRasterSourceParams {
                data: mock_data,
                result_descriptor: RasterResultDescriptor::new(
                    RasterDataType::U8,
                    SpatialReference::epsg_4326().into(),
                    None,
                    tileing_strategy.geo_transform,
                    query_rect.spatial_query().grid_bounds(),
                    RasterBandDescriptors::new_single_band(),
                ),
            },
        };

        Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: SpatialReference::epsg_4326(),
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource::from(mock_raster_operator.boxed()),
        }
        .boxed()
    }

    let qrect = RasterQueryRectangle {
        spatial_bounds: SpatialPartition2D::new(
            (-20_037_508.342_789_244, 20_048_966.104_014_594).into(),
            (20_037_508.342_789_244, -20_048_966.104_014_594).into(),
        )
        .unwrap(),
        time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        spatial_resolution: SpatialResolution::new(1050., 2100.).unwrap(),
        attributes: BandSelection::first(),
    };
    let tiling_spec = TilingSpecification::new((0., 0.).into(), [512, 512].into());

    let qrects = vec![("World in 36000x18000 pixels", qrect)];
    let tiling_specs = vec![tiling_spec];

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
    let tiling_origin = Coordinate2D::new(0., 0.);

    let qrects = vec![
        (
            "World in 36000x18000 pixels",
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap(),
                SpatialResolution::new(0.01, 0.01).unwrap(),
                tiling_origin,
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
                BandSelection::first(),
            ),
        ),
        (
            "World in 72000x36000 pixels",
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap(),
                SpatialResolution::new(0.005, 0.005).unwrap(),
                tiling_origin,
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
                BandSelection::first(),
            ),
        ),
    ];

    let tiling_specs = vec![
        TilingSpecification::new(tiling_origin, [32, 32].into()),
        TilingSpecification::new(tiling_origin, [64, 64].into()),
        TilingSpecification::new(tiling_origin, [128, 128].into()),
        TilingSpecification::new(tiling_origin, [256, 256].into()),
        TilingSpecification::new(tiling_origin, [512, 512].into()),
        // TilingSpecification::new((0., 0.).into(), [600, 600].into()),
        // TilingSpecification::new((0., 0.).into(), [900, 900].into()),
        TilingSpecification::new(tiling_origin, [1024, 1024].into()),
        TilingSpecification::new(tiling_origin, [2048, 2048].into()),
        TilingSpecification::new(tiling_origin, [4096, 4096].into()),
        // TilingSpecification::new((0., 0.).into(), [9000, 9000].into()),
    ];

    let id: DataId = DatasetId::new().into();
    let name = NamedData::with_system_name("world");
    let meta_data = create_ndvi_meta_data();

    let gdal_operator = GdalSource {
        params: GdalSourceParameters::new(name.clone()),
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
            mex.add_meta_data(id.clone(), name.clone(), meta_data.box_clone());
            mex
        },
        [ChunkByteSize::MAX],
        [8],
    )
    .run_all_benchmarks(bench_collector);
}

fn bench_gdal_source_operator_with_expression_tile_size(bench_collector: &mut BenchmarkCollector) {
    let tiling_origin = Coordinate2D::new(0., 0.);

    let qrects = vec![(
        "World in 36000x18000 pixels",
        RasterQueryRectangle::with_partition_and_resolution_and_origin(
            SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap(),
            SpatialResolution::new(0.01, 0.01).unwrap(),
            tiling_origin,
            TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            BandSelection::first(),
        ),
    )];

    let tiling_specs = vec![
        // TilingSpecification::new((0., 0.).into(), [32, 32].into()),
        TilingSpecification::new(tiling_origin, [64, 64].into()),
        TilingSpecification::new(tiling_origin, [128, 128].into()),
        TilingSpecification::new(tiling_origin, [256, 256].into()),
        TilingSpecification::new(tiling_origin, [512, 512].into()),
        // TilingSpecification::new((0., 0.).into(), [600, 600].into()),
        // TilingSpecification::new((0., 0.).into(), [900, 900].into()),
        TilingSpecification::new(tiling_origin, [1024, 1024].into()),
        TilingSpecification::new(tiling_origin, [2048, 2048].into()),
        TilingSpecification::new(tiling_origin, [4096, 4096].into()),
        // TilingSpecification::new((0., 0.).into(), [9000, 9000].into()),
    ];

    let id: DataId = DatasetId::new().into();
    let name = NamedData::with_system_name("world");
    let meta_data = create_ndvi_meta_data();

    let gdal_operator = GdalSource {
        params: GdalSourceParameters::new(name.clone()),
    };

    let expression_operator = Expression {
        params: ExpressionParams {
            expression: "A+B".to_string(),
            output_type: RasterDataType::U8,
            output_band: None,
            map_no_data: false,
        },
        sources: SingleRasterSource {
            raster: RasterStacker {
                params: RasterStackerParams {
                    rename_bands: RenameBands::Default,
                },
                sources: MultipleRasterSources {
                    rasters: vec![gdal_operator.clone().boxed(), gdal_operator.boxed()],
                },
            }
            .boxed(),
        },
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
            mex.add_meta_data(id.clone(), name.clone(), meta_data.box_clone());
            mex
        },
        [ChunkByteSize::MAX],
        [8],
    )
    .run_all_benchmarks(bench_collector);
}

fn bench_gdal_source_operator_with_identity_reprojection(bench_collector: &mut BenchmarkCollector) {
    let tiling_origin = Coordinate2D::new(0., 0.);

    let qrects = vec![(
        "World in 36000x18000 pixels",
        RasterQueryRectangle::with_partition_and_resolution_and_origin(
            SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap(),
            SpatialResolution::new(0.01, 0.01).unwrap(),
            tiling_origin,
            TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            BandSelection::first(),
        ),
    )];

    let tiling_specs = vec![
        // TilingSpecification::new((0., 0.).into(), [32, 32].into()),
        // TilingSpecification::new((0., 0.).into(), [64, 64].into()),
        // TilingSpecification::new((0., 0.).into(), [128, 128].into()),
        TilingSpecification::new(tiling_origin, [256, 256].into()),
        TilingSpecification::new(tiling_origin, [512, 512].into()),
        // TilingSpecification::new((0., 0.).into(), [600, 600].into()),
        // TilingSpecification::new((0., 0.).into(), [900, 900].into()),
        TilingSpecification::new(tiling_origin, [1024, 1024].into()),
        TilingSpecification::new(tiling_origin, [2048, 2048].into()),
        TilingSpecification::new(tiling_origin, [4096, 4096].into()),
        // TilingSpecification::new((0., 0.).into(), [9000, 9000].into()),
    ];

    let id: DataId = DatasetId::new().into();
    let name = NamedData::with_system_name("world");
    let meta_data = create_ndvi_meta_data();

    let gdal_operator = GdalSource {
        params: GdalSourceParameters::new(name.clone()),
    };

    let projection_operator = Reprojection {
        params: ReprojectionParams {
            target_spatial_reference: SpatialReference::epsg_4326(),
            derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
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
            mex.add_meta_data(id.clone(), name.clone(), meta_data.box_clone());
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
    let tiling_origin = Coordinate2D::new(0., 0.);

    let qrects = vec![(
        "World in EPSG:3857 ~ 40000 x 20000 px",
        RasterQueryRectangle::with_partition_and_resolution_and_origin(
            SpatialPartition2D::new(
                (-20_037_508.342_789_244, 20_048_966.104_014_594).into(),
                (20_037_508.342_789_244, -20_048_966.104_014_594).into(),
            )
            .unwrap(),
            SpatialResolution::new(1050., 2100.).unwrap(),
            tiling_origin,
            TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            BandSelection::first(),
        ),
    )];

    let tiling_specs = vec![
        // TilingSpecification::new((0., 0.).into(), [32, 32].into()),
        // TilingSpecification::new((0., 0.).into(), [64, 64].into()),
        // TilingSpecification::new((0., 0.).into(), [128, 128].into()),
        // TilingSpecification::new((0., 0.).into(), [256, 256].into()),
        TilingSpecification::new(tiling_origin, [512, 512].into()),
        // TilingSpecification::new((0., 0.).into(), [600, 600].into()),
        // TilingSpecification::new((0., 0.).into(), [900, 900].into()),
        TilingSpecification::new(tiling_origin, [1024, 1024].into()),
        TilingSpecification::new(tiling_origin, [2048, 2048].into()),
        TilingSpecification::new(tiling_origin, [4096, 4096].into()),
        // TilingSpecification::new((0., 0.).into(), [9000, 9000].into()),
    ];

    let id: DataId = DatasetId::new().into();
    let name = NamedData::with_system_name("world");
    let meta_data = create_ndvi_meta_data();

    let gdal_operator = GdalSource {
        params: GdalSourceParameters::new(name.clone()),
    };

    let projection_operator = Reprojection {
        params: ReprojectionParams {
            target_spatial_reference: SpatialReference::new(
                geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
                3857,
            ),
            derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
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
            mex.add_meta_data(id.clone(), name.clone(), meta_data.box_clone());
            mex
        },
        [ChunkByteSize::MAX],
        [1, 4, 8, 16, 32],
    )
    .run_all_benchmarks(bench_collector);
}

*/

fn main() {
    /*
    let mut bench_collector = BenchmarkCollector::default();

    bench_mock_source_operator(&mut bench_collector);
    bench_mock_source_operator_with_expression(&mut bench_collector);
    bench_mock_source_operator_with_identity_reprojection(&mut bench_collector);
    bench_mock_source_operator_with_4326_to_3857_reprojection(&mut bench_collector);
    bench_gdal_source_operator_tile_size(&mut bench_collector);
    bench_gdal_source_operator_with_expression_tile_size(&mut bench_collector);
    bench_gdal_source_operator_with_identity_reprojection(&mut bench_collector);
    bench_gdal_source_operator_with_4326_to_3857_reprojection(&mut bench_collector);
     */
}
