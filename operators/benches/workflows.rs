#![feature(bench_black_box)]
use std::hint::black_box;
use std::time::Instant;

use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{Coordinate2D, SpatialPartition2D, SpatialResolution, TimeInterval},
    raster::{GeoTransform, Grid2D, GridOrEmpty2D, Pixel, RasterTile2D, TilingSpecification},
    util::test::TestDefault,
};
use geoengine_operators::{
    engine::{
        ChunkByteSize, MockQueryContext, QueryContext, RasterQueryProcessor, RasterQueryRectangle,
    },
    mock::MockRasterSourceProcessor,
    source::{GdalMetaDataRegular, GdalSourceProcessor},
    util::gdal::create_ndvi_meta_data,
};

fn setup_gdal_source(meta_data: GdalMetaDataRegular, tile_size: usize) -> GdalSourceProcessor<u8> {
    let tiling_specification =
        TilingSpecification::new(Coordinate2D::default(), [tile_size, tile_size].into());

    GdalSourceProcessor::<u8> {
        tiling_specification,
        meta_data: Box::new(meta_data),
        phantom_data: Default::default(),
    }
}

fn setup_mock_source(tile_size: usize) -> MockRasterSourceProcessor<u8> {
    let no_data_value = Some(0);
    let grid: GridOrEmpty2D<u8> = Grid2D::new(
        [tile_size, tile_size].into(),
        vec![42; tile_size * tile_size],
        no_data_value,
    )
    .unwrap()
    .into();
    let geo_transform = GeoTransform::test_default();

    let time = TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap();

    MockRasterSourceProcessor {
        data: vec![
            RasterTile2D::new(time, [-1, -1].into(), geo_transform, grid.clone()),
            RasterTile2D::new(time, [-1, 0].into(), geo_transform, grid.clone()),
            RasterTile2D::new(time, [-1, 1].into(), geo_transform, grid.clone()),
            RasterTile2D::new(time, [0, -1].into(), geo_transform, grid.clone()),
            RasterTile2D::new(time, [0, 0].into(), geo_transform, grid.clone()),
            RasterTile2D::new(time, [0, 1].into(), geo_transform, grid.clone()),
            RasterTile2D::new(time, [1, -1].into(), geo_transform, grid.clone()),
            RasterTile2D::new(time, [1, 0].into(), geo_transform, grid.clone()),
            RasterTile2D::new(time, [1, 1].into(), geo_transform, grid.clone()),
        ],
    }
}

fn bench_raster_processor<T: Pixel, S: RasterQueryProcessor<RasterType = T>, C: QueryContext>(
    list_of_named_querys: &[(&str, RasterQueryRectangle)],
    named_tile_producing_operator: (&str, S),
    ctx: &C,
    run_time: &tokio::runtime::Runtime,
) {
    let (operator_name, operator) = named_tile_producing_operator;
    for &(qrect_name, qrect) in list_of_named_querys {
        run_time.block_on(async {
            let start = Instant::now();

            let query = operator.raster_query(qrect, ctx).await.unwrap();

            let res: Vec<Result<RasterTile2D<T>, _>> = query.collect().await;
            let number_of_tiles = black_box(res.into_iter().map(Result::unwrap).count());

            let elapsed = start.elapsed();
            println!(
                "Bench \"{}\" with query \"{}\" produced {} tiles in {}s | {}ns",
                operator_name,
                qrect_name,
                number_of_tiles,
                elapsed.as_secs_f32(),
                elapsed.as_nanos()
            );
        });
    }
}

fn main() {
    let qrects = vec![
        (
            "1 tile",
            RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new((0., 60.).into(), (60., 0.).into())
                    .unwrap(),
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::zero_point_one(),
            },
        ),
        (
            "2 tiles",
            RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new((0., 50.).into(), (60., -10.).into())
                    .unwrap(),
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::zero_point_one(),
            },
        ),
        (
            "4 tiles",
            RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new((-5., 50.).into(), (55., -10.).into())
                    .unwrap(),
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::zero_point_one(),
            },
        ),
        (
            "2 tiles, 2 no-data tiles",
            RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new((130., 120.).into(), (190., 60.).into())
                    .unwrap(),
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::zero_point_one(),
            },
        ),
        (
            "empty tiles",
            RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new((-5., 50.).into(), (55., -10.).into())
                    .unwrap(),
                time_interval: TimeInterval::new(1_000_000_000_000, 1_000_000_000_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::zero_point_one(),
            },
        ),
    ];

    let run_time = tokio::runtime::Runtime::new().unwrap();
    let ctx = MockQueryContext::with_chunk_size_and_thread_count(ChunkByteSize::MAX, 8);

    bench_raster_processor(
        &qrects,
        ("mock_source_600", setup_mock_source(600)),
        &ctx,
        &run_time,
    );
    bench_raster_processor(
        &qrects,
        ("mock_source_512", setup_mock_source(512)),
        &ctx,
        &run_time,
    );
    bench_raster_processor(
        &qrects,
        ("mock_source_1024", setup_mock_source(1024)),
        &ctx,
        &run_time,
    );
    bench_raster_processor(
        &qrects,
        (
            "gdal_source_600",
            setup_gdal_source(create_ndvi_meta_data(), 600),
        ),
        &ctx,
        &run_time,
    );
    bench_raster_processor(
        &qrects,
        (
            "gdal_source_512",
            setup_gdal_source(create_ndvi_meta_data(), 512),
        ),
        &ctx,
        &run_time,
    );

    bench_raster_processor(
        &qrects,
        (
            "gdal_source_1024",
            setup_gdal_source(create_ndvi_meta_data(), 1024),
        ),
        &ctx,
        &run_time,
    );
}
