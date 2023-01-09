use std::time::Instant;
use std::{hint::black_box, marker::PhantomData};

use futures::StreamExt;
use geoengine_datatypes::primitives::Coordinate2D;
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval},
    raster::{
        GeoTransform, Grid2D, GridOrEmpty2D, GridSize, Pixel, RasterTile2D, TilingSpecification,
    },
    util::test::TestDefault,
};
use geoengine_operators::{
    engine::{ChunkByteSize, MockQueryContext, QueryContext, RasterQueryProcessor},
    mock::MockRasterSourceProcessor,
    source::{GdalMetaDataRegular, GdalSourceProcessor},
    util::gdal::create_ndvi_meta_data,
};

fn setup_gdal_source(
    meta_data: GdalMetaDataRegular,
    tiling_specification: TilingSpecification,
) -> GdalSourceProcessor<u8> {
    GdalSourceProcessor::<u8> {
        tiling_specification,
        meta_data: Box::new(meta_data),
        _phantom_data: PhantomData,
    }
}

fn setup_mock_source(tiling_spec: TilingSpecification) -> MockRasterSourceProcessor<u8> {
    let grid: GridOrEmpty2D<u8> = Grid2D::new(
        tiling_spec.tile_size_in_pixels,
        vec![42; tiling_spec.tile_size_in_pixels.number_of_elements()],
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
            RasterTile2D::new(time, [1, 1].into(), geo_transform, grid),
        ],
        tiling_specification: tiling_spec,
    }
}

#[inline(never)]
fn bench_raster_processor<
    T: Pixel,
    F: Fn(TilingSpecification) -> S,
    S: RasterQueryProcessor<RasterType = T>,
    C: QueryContext,
>(
    bench_id: &'static str,
    list_of_named_querys: &[(&str, RasterQueryRectangle)],
    list_of_tiling_specs: &[TilingSpecification],
    tile_producing_operator_builderr: F,
    ctx: &C,
    run_time: &tokio::runtime::Runtime,
) {
    for tiling_spec in list_of_tiling_specs {
        let operator = (tile_producing_operator_builderr)(*tiling_spec);

        for &(qrect_name, qrect) in list_of_named_querys {
            run_time.block_on(async {
                // query the operator
                let start_query = Instant::now();
                let query = operator.raster_query(qrect, ctx).await.unwrap();
                let query_elapsed = start_query.elapsed();

                let start = Instant::now();
                // drain the stream
                let res: Vec<Result<RasterTile2D<T>, _>> = query.collect().await;

                let elapsed = start.elapsed();

                // count elements in a black_box to avoid compiler optimization
                let number_of_tiles = black_box(res.into_iter().map(Result::unwrap).count());

                println!(
                    "{}, {}, {}, {}, {}, {}, {}, {}",
                    bench_id,
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

fn bench_no_data_tiles() {
    let tiling_origin = Coordinate2D::new(0., 0.);
    let spatial_resolution = SpatialResolution::zero_point_one();

    let qrects = vec![
        (
            "1 tile",
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                SpatialPartition2D::new((0., 60.).into(), (60., 0.).into()).unwrap(),
                spatial_resolution,
                tiling_origin,
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            ),
        ),
        (
            "2 tiles",
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                SpatialPartition2D::new((0., 50.).into(), (60., -10.).into()).unwrap(),
                spatial_resolution,
                tiling_origin,
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            ),
        ),
        (
            "4 tiles",
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                SpatialPartition2D::new((-5., 50.).into(), (55., -10.).into()).unwrap(),
                spatial_resolution,
                tiling_origin,
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            ),
        ),
        (
            "2 tiles, 2 no-data tiles",
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                SpatialPartition2D::new((130., 120.).into(), (190., 60.).into()).unwrap(),
                spatial_resolution,
                tiling_origin,
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            ),
        ),
        (
            "empty tiles",
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                SpatialPartition2D::new((-5., 50.).into(), (55., -10.).into()).unwrap(),
                spatial_resolution,
                tiling_origin,
                TimeInterval::new(1_000_000_000_000, 1_000_000_000_000 + 1000).unwrap(),
            ),
        ),
    ];

    let tiling_specs = vec![TilingSpecification::new(tiling_origin, [600, 600].into())];

    let run_time = tokio::runtime::Runtime::new().unwrap();
    let ctx = MockQueryContext::with_chunk_size_and_thread_count(ChunkByteSize::MAX, 8);

    bench_raster_processor(
        "no_data_tiles",
        &qrects,
        &tiling_specs,
        setup_mock_source,
        &ctx,
        &run_time,
    );
    bench_raster_processor(
        "no_data_tiles",
        &qrects,
        &tiling_specs,
        |ts| setup_gdal_source(create_ndvi_meta_data(), ts),
        &ctx,
        &run_time,
    );
}

fn bench_tile_size() {
    let tiling_origin = Coordinate2D::new(0., 0.);

    let qrects = vec![(
        "World in 36000x18000 pixels",
        RasterQueryRectangle::with_partition_and_resolution_and_origin(
            SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap(),
            SpatialResolution::new(0.01, 0.01).unwrap(),
            tiling_origin,
            TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        ),
    )];

    let run_time = tokio::runtime::Runtime::new().unwrap();
    let ctx = MockQueryContext::with_chunk_size_and_thread_count(ChunkByteSize::MAX, 8);

    let tiling_specs = vec![
        TilingSpecification::new(tiling_origin, [32, 32].into()),
        TilingSpecification::new(tiling_origin, [64, 64].into()),
        TilingSpecification::new(tiling_origin, [128, 128].into()),
        TilingSpecification::new(tiling_origin, [256, 256].into()),
        TilingSpecification::new(tiling_origin, [512, 512].into()),
        TilingSpecification::new(tiling_origin, [600, 600].into()),
        TilingSpecification::new(tiling_origin, [900, 900].into()),
        TilingSpecification::new(tiling_origin, [1024, 1024].into()),
        TilingSpecification::new(tiling_origin, [2048, 2048].into()),
        TilingSpecification::new(tiling_origin, [4096, 4096].into()),
        TilingSpecification::new(tiling_origin, [9000, 9000].into()),
    ];

    bench_raster_processor(
        "tile_size",
        &qrects,
        &tiling_specs,
        |ts| setup_gdal_source(create_ndvi_meta_data(), ts),
        &ctx,
        &run_time,
    );
}

fn main() {
    println!("Bench_name, query_name, tilesize_x, tilesize_y, query_time (ns), tiles_produced, pixels_produced, stream_collect_time (ns) ");

    bench_no_data_tiles();
    bench_tile_size();
}
