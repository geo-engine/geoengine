use geoengine_datatypes::dataset::{DataId, DatasetId};
use geoengine_datatypes::primitives::{Measurement, RasterQueryRectangle, SpatialPartitioned};
use geoengine_datatypes::raster::{Grid2D, RasterDataType};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_datatypes::util::Identifier;
use geoengine_datatypes::{
    primitives::{SpatialPartition2D, SpatialResolution, TimeInterval},
    raster::{GridSize, RasterTile2D, TilingSpecification},
};
use geoengine_operators::engine::{MetaData, RasterResultDescriptor, SingleRasterOrVectorSource};
use geoengine_operators::mock::{MockRasterSource, MockRasterSourceParams};
use geoengine_operators::processing::{
    Expression, ExpressionParams, ExpressionSources, Reprojection, ReprojectionParams,
};
use geoengine_operators::source::GdalSource;
use geoengine_operators::util::workflow_bencher::{
    BenchmarkRunner, WorkflowBenchmarkCollector, WorkflowMultiBenchmark, WorkflowSingleBenchmark,
};
use geoengine_operators::{
    engine::{ChunkByteSize, MockExecutionContext, RasterOperator},
    source::GdalSourceParameters,
    util::gdal::create_ndvi_meta_data,
};

fn bench_mock_source_operator_with_identity_reprojection(
    bench_collector: &mut WorkflowBenchmarkCollector,
) {
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

geoengine_operators::workflow_bencher_main!(bench_mock_source_operator_with_identity_reprojection);
