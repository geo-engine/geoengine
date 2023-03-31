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

use geoengine_operators::util::workflow_bencher::{
    BenchmarkRunner, WorkflowBenchmarkCollector, WorkflowBenchmarkResult, WorkflowMultiBenchmark,
    WorkflowSingleBenchmark,
};

use geoengine_operators::util::workflow_bencher::workflow_bencher_main;

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

geoengine_operators::util::workflow_bencher::workflow_bencher_main!(bench_mock_source_operator);
