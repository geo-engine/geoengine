#![feature(bench_black_box)]
use std::hint::black_box;
use std::time::{Instant, Duration};

use futures::TryStreamExt;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::primitives::{
    Measurement, QueryRectangle, RasterQueryRectangle, SpatialPartitioned,
};
use geoengine_datatypes::raster::{Grid2D, RasterDataType, GridShape2D};
use geoengine_datatypes::spatial_reference::SpatialReference;

use geoengine_datatypes::util::Identifier;
use geoengine_datatypes::{
    dataset::InternalDatasetId,
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

pub struct BenchSetup<Q, T, F, C, B, O> {
    bench_id: &'static str,
    named_querys: Q,
    tiling_specs: T,
    operator_builder: O,
    context_builder: F,
    chunk_byte_size: B,
    num_threads: C,
}

impl<'a, Q, T, F, C, B, O> BenchSetup<Q, T, F, C, B, O>
where
    F: Fn(TilingSpecification, usize) -> MockExecutionContext,
    C: IntoIterator<Item = &'a usize> + Clone,
    Q: IntoIterator<Item = &'a (&'static str, RasterQueryRectangle)> + Clone,
    T: IntoIterator<Item = &'a TilingSpecification> + Clone,
    B: IntoIterator<Item = &'a ChunkByteSize> + Clone,
    O: Fn(TilingSpecification, QueryRectangle<SpatialPartition2D>) -> Box<dyn RasterOperator>,
{
    #[inline(never)]
    fn bench_raster_operator(&self) {
        let run_time = tokio::runtime::Runtime::new().unwrap();

        for current_threads in self.num_threads.clone().into_iter() {
            for tiling_spec in self.tiling_specs.clone().into_iter() {
                let exe_ctx = (self.context_builder)(*tiling_spec, *current_threads);

                for current_cbs in self.chunk_byte_size.clone().into_iter() {
                    let ctx = exe_ctx.mock_query_context(*current_cbs);

                    for (ref qrect_name, qrect) in self.named_querys.clone().into_iter() {
                        let operator = (self.operator_builder)(*tiling_spec, *qrect);
                        let init_start = Instant::now();
                        let initialized_operator =
                            run_time.block_on(async { operator.initialize(&exe_ctx).await });
                        let init_elapsed = init_start.elapsed();
                        // println!(
                        //    "Initialized context \"{}\" | operator \"\" | in {} s  ({} ns)",
                        //    context_name,
                        //    init_elapsed.as_secs_f32(),
                        //    init_elapsed.as_nanos()
                        //);

                        let initialized_queryprocessor =
                            initialized_operator.unwrap().query_processor().unwrap();

                        call_on_generic_raster_processor!(initialized_queryprocessor, op => { run_time.block_on(async {
                                let start_query = Instant::now();
                                // query the operator
                                let query = op
                                    .raster_query(*qrect, &ctx)
                                    .await
                                    .unwrap();
                                let query_elapsed = start_query.elapsed();

                                let start = Instant::now();
                                // drain the stream
                                // TODO: build a custom drain that does not collect all the tiles
                                let tile_count_res: Result<usize,_> = query.try_fold(0, |accu, tile|  async move {
                                    black_box({
                                        let _ = tile;
                                        Ok(accu + 1)
                                    })

                                }).await;

                                let elapsed = start.elapsed();
                                let number_of_tiles = tile_count_res.unwrap();

                                // count elements in a black_box to avoid compiler optimization



                                WorkflowBenchResult{
                                    bench_id: self.bench_id.to_owned(),
                                    num_threads: *current_threads,
                                    chunk_byte_size: *current_cbs,
                                    qrect_name: qrect_name.to_string(),
                                    tile_size: tiling_spec.tile_size_in_pixels,
                                    number_of_tiles,
                                    init_time: init_elapsed,
                                    query_time: query_elapsed,
                                    stream_time: elapsed,
                                }
                            });
                        });
                    }
                }
            }
        }
    }

    pub fn new(
        bench_id: &'static str,
        named_querys: Q,
        tiling_specs: T,
        operator_builder: O,
        context_builder: F,
        chunk_byte_size: B,
        num_threads: C,
    ) -> Self {
        Self {
            bench_id,
            named_querys,
            tiling_specs,
            operator_builder,
            context_builder,
            chunk_byte_size,
            num_threads,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct WorkflowBenchResult {
    pub bench_id: String,
    pub num_threads: usize,
    pub chunk_byte_size: ChunkByteSize,
    pub qrect_name: String,
    pub tile_size: GridShape2D,
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
where S: Serializer,
{
    serializer.serialize_u128(duration.as_millis())
}

fn bench_mock_source_operator() {
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
        let no_data_value = Some(42);
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
                    no_data_value,
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
                    no_data_value: no_data_value.map(|v| v as f64),
                },
            },
        }
        .boxed()
    }

    BenchSetup::new(
        "mock_source",
        &qrects,
        &tiling_specs,
        operator_builder,
        |ts, num_threads| {
            MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads)
        },
        &[ChunkByteSize::MAX],
        &[4, 8, 16, 32],
    )
    .bench_raster_operator();
}

fn bench_mock_source_operator_with_expression() {
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
        let no_data_value = Some(42);
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
                    no_data_value,
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
                    no_data_value: no_data_value.map(|v| v as f64),
                },
            },
        };

        Expression {
            params: ExpressionParams {
                expression: "A+B".to_string(),
                output_type: RasterDataType::U8,
                output_no_data_value: 0., //  cast no_data_valuee to f64
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

    BenchSetup::new(
        "mock_source_with_expression_a+b",
        &qrects,
        &tiling_specs,
        operator_builder,
        |ts, num_threads| {
            MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads)
        },
        &[ChunkByteSize::MAX],
        &[1, 2, 4, 8, 16, 32],
    )
    .bench_raster_operator();
}

fn bench_mock_source_operator_with_identity_reprojection() {
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
        let no_data_value = Some(42);
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
                    no_data_value,
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
                    no_data_value: no_data_value.map(|v| v as f64),
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

    BenchSetup::new(
        "mock_source_512px_with_identity_reprojection",
        &qrects,
        &tiling_specs,
        operator_builder,
        |ts, num_threads| {
            MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads)
        },
        &[ChunkByteSize::MAX],
        &[4, 8, 16, 32],
    )
    .bench_raster_operator();
}

fn bench_mock_source_operator_with_4326_to_3857_reprojection() {
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
        let no_data_value = Some(42);
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
                    no_data_value,
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
                    no_data_value: no_data_value.map(|v| v as f64),
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

    BenchSetup::new(
        "mock_source projection 4326 -> 3857",
        &qrects,
        &tiling_specs,
        operator_builder,
        |ts, num_threads| {
            MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads)
        },
        &[ChunkByteSize::MAX],
        &[4, 8, 16, 32],
    )
    .bench_raster_operator();
}

fn bench_gdal_source_operator_tile_size() {
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

    let id: DatasetId = InternalDatasetId::new().into();
    let meta_data = create_ndvi_meta_data();

    let gdal_operator = GdalSource {
        params: GdalSourceParameters {
            dataset: id.clone(),
        },
    }
    .boxed();

    BenchSetup::new(
        "gdal_source",
        &qrects,
        &tiling_specs,
        |_, _| gdal_operator.clone(),
        |ts, num_threads| {
            let mut mex =
                MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads);
            mex.add_meta_data(id.clone(), meta_data.box_clone());
            mex
        },
        &[ChunkByteSize::MAX],
        &[8],
    )
    .bench_raster_operator();
}

fn bench_gdal_source_operator_with_expression_tile_size() {
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

    let id: DatasetId = InternalDatasetId::new().into();
    let meta_data = create_ndvi_meta_data();

    let gdal_operator = GdalSource {
        params: GdalSourceParameters {
            dataset: id.clone(),
        },
    };

    let expression_operator = Expression {
        params: ExpressionParams {
            expression: "A+B".to_string(),
            output_type: RasterDataType::U8,
            output_no_data_value: 0., //  cast no_data_valuee to f64
            output_measurement: Some(Measurement::Unitless),
            map_no_data: false,
        },
        sources: ExpressionSources::new_a_b(gdal_operator.clone().boxed(), gdal_operator.boxed()),
    }
    .boxed();

    BenchSetup::new(
        "expression a+b",
        &qrects,
        &tiling_specs,
        |_, _| expression_operator.clone(),
        |ts, num_threads| {
            let mut mex =
                MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads);
            mex.add_meta_data(id.clone(), meta_data.box_clone());
            mex
        },
        &[ChunkByteSize::MAX],
        &[8],
    )
    .bench_raster_operator();
}

fn bench_gdal_source_operator_with_identity_reprojection() {
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

    let id: DatasetId = InternalDatasetId::new().into();
    let meta_data = create_ndvi_meta_data();

    let gdal_operator = GdalSource {
        params: GdalSourceParameters {
            dataset: id.clone(),
        },
    };

    let projection_operator = Reprojection {
        params: ReprojectionParams {
            target_spatial_reference: SpatialReference::epsg_4326(),
        },
        sources: SingleRasterOrVectorSource::from(gdal_operator.boxed()),
    }
    .boxed();

    BenchSetup::new(
        "gdal source + projection identity",
        &qrects,
        &tiling_specs,
        |_, _| projection_operator.clone(),
        |ts, num_threads| {
            let mut mex =
                MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads);
            mex.add_meta_data(id.clone(), meta_data.box_clone());
            mex
        },
        &[ChunkByteSize::MAX],
        &[8],
    );
}

fn bench_gdal_source_operator_with_4326_to_3857_reprojection() {
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

    let id: DatasetId = InternalDatasetId::new().into();
    let meta_data = create_ndvi_meta_data();

    let gdal_operator = GdalSource {
        params: GdalSourceParameters {
            dataset: id.clone(),
        },
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

    BenchSetup::new(
        "projection 4326 -> 3857",
        &qrects,
        &tiling_specs,
        |_, _| projection_operator.clone(),
        |ts, num_threads| {
            let mut mex =
                MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads);
            mex.add_meta_data(id.clone(), meta_data.box_clone());
            mex
        },
        &[ChunkByteSize::MAX],
        &[1, 4, 8, 16, 32],
    );
}

fn main() {
    println!("Bench_name, num_threads, chunk_byte_size, query_name, tilesize_x, tilesize_y, init_time (ns), query_time (ns), tiles_produced, pixels_produced, stream_collect_time (ns) ");

    bench_mock_source_operator();
    bench_mock_source_operator_with_expression();
    bench_mock_source_operator_with_identity_reprojection();
    bench_mock_source_operator_with_4326_to_3857_reprojection();
    bench_gdal_source_operator_tile_size();
    bench_gdal_source_operator_with_expression_tile_size();
    bench_gdal_source_operator_with_identity_reprojection();
    bench_gdal_source_operator_with_4326_to_3857_reprojection();
}
