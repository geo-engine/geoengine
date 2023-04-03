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

fn bench_gdal_source_operator_with_4326_to_3857_reprojection(
    bench_collector: &mut WorkflowBenchmarkCollector,
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

geoengine_operators::workflow_bencher_main!(
    bench_gdal_source_operator_with_4326_to_3857_reprojection
);
