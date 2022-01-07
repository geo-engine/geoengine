#![feature(bench_black_box)]
use std::hint::black_box;
use std::time::Instant;

use futures::StreamExt;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::primitives::Measurement;
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_datatypes::util::Identifier;
use geoengine_datatypes::{
    dataset::InternalDatasetId,
    primitives::{SpatialPartition2D, SpatialResolution, TimeInterval},
    raster::{GridSize, RasterTile2D, TilingSpecification},
};
use geoengine_operators::engine::{MetaData, SingleRasterOrVectorSource};
use geoengine_operators::processing::{
    Expression, ExpressionParams, ExpressionSources, Reprojection, ReprojectionParams,
};
use geoengine_operators::source::GdalSource;
use geoengine_operators::{
    engine::{
        ChunkByteSize, MockExecutionContext, RasterOperator, RasterQueryProcessor,
        RasterQueryRectangle,
    },
    source::GdalSourceParameters,
    util::gdal::create_ndvi_meta_data,
};

#[inline(never)]
fn bench_raster_operator<'a, Q, T, F, C, B>(
    bench_id: &'static str,
    named_querys: Q,
    tiling_specs: T,
    named_operator: Box<dyn RasterOperator>,
    context_builder: F,
    chunk_byte_size: B,
    run_time: &tokio::runtime::Runtime,
    num_threads: C,
) where
    F: Fn(TilingSpecification, usize) -> MockExecutionContext,
    C: IntoIterator<Item = &'a usize> + Clone,
    Q: IntoIterator<Item = &'a (&'static str, RasterQueryRectangle)> + Clone,
    T: IntoIterator<Item = &'a TilingSpecification> + Clone,
    B: IntoIterator<Item = &'a ChunkByteSize> + Clone,
{
    for current_threads in num_threads.into_iter() {
        for tiling_spec in tiling_specs.clone().into_iter() {
            let exe_ctx = (context_builder)(*tiling_spec, *current_threads);
            for current_cbs in chunk_byte_size.clone().into_iter() {
                let ctx = exe_ctx.mock_query_context(*current_cbs);

                // let init_start = Instant::now();
                let initialized_operator =
                    run_time.block_on(async { named_operator.clone().initialize(&exe_ctx).await });

                // let init_elapsed = init_start.elapsed();
                // println!(
                //    "Initialized context \"{}\" | operator \"\" | in {} s  ({} ns)",
                //    context_name,
                //    init_elapsed.as_secs_f32(),
                //    init_elapsed.as_nanos()
                //);

                let initialized_queryprocessor = initialized_operator
                    .unwrap()
                    .query_processor()
                    .unwrap()
                    .get_u8()
                    .unwrap();

                for (qrect_name, qrect) in named_querys.clone().into_iter() {
                    run_time.block_on(async {
                        let start_query = Instant::now();
                        // query the operator
                        let query = initialized_queryprocessor
                            .raster_query(*qrect, &ctx)
                            .await
                            .unwrap();
                        let query_elapsed = start_query.elapsed();

                        let start = Instant::now();
                        // drain the stream
                        // TODO: build a custom drain that does not collect all the tiles
                        let res: Vec<Result<RasterTile2D<_>, _>> = query.collect().await;

                        let elapsed = start.elapsed();

                        // count elements in a black_box to avoid compiler optimization
                        let number_of_tiles =
                            black_box(res.into_iter().map(Result::unwrap).count());

                        println!(
                            "{}, {}, {}, {}, {}, {}, {}, {}, {}, {}",
                            bench_id,
                            current_threads,
                            current_cbs.bytes(),
                            qrect_name,
                            tiling_spec.tile_size_in_pixels.axis_size_y(),
                            tiling_spec.tile_size_in_pixels.axis_size_x(),
                            query_elapsed.as_nanos(),
                            number_of_tiles,
                            number_of_tiles as u128
                                * tiling_spec.tile_size_in_pixels.number_of_elements() as u128,
                            elapsed.as_nanos()
                        );
                    });
                }
            }
        }
    }
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

    let run_time = tokio::runtime::Runtime::new().unwrap();

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

    bench_raster_operator(
        "gdal_source",
        &qrects,
        &tiling_specs,
        gdal_operator,
        |ts, num_threads| {
            let mut mex =
                MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads);
            mex.add_meta_data(id.clone(), meta_data.box_clone());
            mex
        },
        &[ChunkByteSize::MAX],
        &run_time,
        &[8],
    );
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

    let run_time = tokio::runtime::Runtime::new().unwrap();

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
        },
        sources: ExpressionSources::new_a_b(gdal_operator.clone().boxed(), gdal_operator.boxed()),
    }
    .boxed();

    bench_raster_operator(
        "expression a+b",
        &qrects,
        &tiling_specs,
        expression_operator,
        |ts, num_threads| {
            let mut mex =
                MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads);
            mex.add_meta_data(id.clone(), meta_data.box_clone());
            mex
        },
        &[ChunkByteSize::MAX],
        &run_time,
        &[8],
    );
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

    let run_time = tokio::runtime::Runtime::new().unwrap();

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
    };

    bench_raster_operator(
        "projection identity",
        &qrects,
        &tiling_specs,
        projection_operator.boxed(),
        |ts, num_threads| {
            let mut mex =
                MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads);
            mex.add_meta_data(id.clone(), meta_data.box_clone());
            mex
        },
        &[ChunkByteSize::MAX],
        &run_time,
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

    let run_time = tokio::runtime::Runtime::new().unwrap();

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
    };

    bench_raster_operator(
        "projection 4326 -> 3857",
        &qrects,
        &tiling_specs,
        projection_operator.boxed(),
        |ts, num_threads| {
            let mut mex =
                MockExecutionContext::new_with_tiling_spec_and_thread_count(ts, num_threads);
            mex.add_meta_data(id.clone(), meta_data.box_clone());
            mex
        },
        &[ChunkByteSize::MAX],
        &run_time,
        &[1, 4, 8, 16, 32],
    );
}

fn main() {
    println!("Bench_name, num_threads, chunk_byte_size, query_name, tilesize_x, tilesize_y, query_time (ns), tiles_produced, pixels_produced, stream_collect_time (ns) ");

    bench_gdal_source_operator_tile_size();
    bench_gdal_source_operator_with_expression_tile_size();
    bench_gdal_source_operator_with_identity_reprojection();
    bench_gdal_source_operator_with_4326_to_3857_reprojection();
}
