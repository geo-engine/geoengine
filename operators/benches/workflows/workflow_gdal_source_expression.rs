use geoengine_datatypes::dataset::{DataId, DatasetId};
use geoengine_datatypes::primitives::{Measurement, RasterQueryRectangle};
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::util::Identifier;
use geoengine_datatypes::{
    primitives::{SpatialPartition2D, SpatialResolution, TimeInterval},
    raster::TilingSpecification,
};
use geoengine_operators::engine::MetaData;
use geoengine_operators::processing::{Expression, ExpressionParams, ExpressionSources};
use geoengine_operators::source::GdalSource;
use geoengine_operators::util::workflow_bencher::{
    BenchmarkRunner, WorkflowBenchmarkCollector, WorkflowMultiBenchmark,
};
use geoengine_operators::{
    engine::{ChunkByteSize, MockExecutionContext, RasterOperator},
    source::GdalSourceParameters,
    util::gdal::create_ndvi_meta_data,
};

fn bench_gdal_source_operator_with_expression_tile_size(
    bench_collector: &mut WorkflowBenchmarkCollector,
) {
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

geoengine_operators::workflow_bencher_main!(bench_gdal_source_operator_with_expression_tile_size);
