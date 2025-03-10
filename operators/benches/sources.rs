#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::print_stderr)] // okay in benchmarks

use futures::StreamExt;
use geoengine_datatypes::primitives::{BandSelection, CacheHint};
use geoengine_datatypes::raster::{
    BoundedGrid, GridBoundingBox2D, GridShapeAccess, RasterDataType,
};
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, TimeInterval},
    raster::{
        GeoTransform, Grid2D, GridOrEmpty2D, GridSize, Pixel, RasterTile2D, TilingSpecification,
    },
    util::test::TestDefault,
};
use geoengine_operators::engine::RasterResultDescriptor;
use geoengine_operators::{
    engine::{ChunkByteSize, MockQueryContext, RasterQueryProcessor},
    mock::MockRasterSourceProcessor,
    source::{GdalMetaDataRegular, GdalSourceProcessor},
    util::gdal::create_ndvi_meta_data,
};
use std::time::Instant;
use std::{hint::black_box, marker::PhantomData};

fn setup_gdal_source(
    meta_data: GdalMetaDataRegular,
    tiling_specification: TilingSpecification,
) -> GdalSourceProcessor<u8> {
    GdalSourceProcessor::<u8> {
        produced_result_descriptor: meta_data.result_descriptor.clone(),
        tiling_specification,
        overview_level: 0,
        meta_data: Box::new(meta_data),
        original_resolution_spatial_grid: None,
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
    let grid_bounds = grid.grid_shape().bounding_box();
    let grid_bounds = GridBoundingBox2D::new(
        [
            grid_bounds.y_min() - grid.axis_size_y() as isize,
            grid_bounds.x_min() - grid.axis_size_x() as isize,
        ],
        [
            grid_bounds.y_min() + 2 * grid.axis_size_y() as isize,
            grid_bounds.x_min() + 2 * grid.axis_size_x() as isize,
        ],
    )
    .unwrap();

    let time = TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap();

    MockRasterSourceProcessor {
        result_descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
            RasterDataType::U8,
            1,
            grid_bounds,
            geo_transform,
        ),
        data: vec![
            RasterTile2D::new(
                time,
                [-1, -1].into(),
                0,
                geo_transform,
                grid.clone(),
                CacheHint::default(),
            ),
            RasterTile2D::new(
                time,
                [-1, 0].into(),
                0,
                geo_transform,
                grid.clone(),
                CacheHint::default(),
            ),
            RasterTile2D::new(
                time,
                [-1, 1].into(),
                0,
                geo_transform,
                grid.clone(),
                CacheHint::default(),
            ),
            RasterTile2D::new(
                time,
                [0, -1].into(),
                0,
                geo_transform,
                grid.clone(),
                CacheHint::default(),
            ),
            RasterTile2D::new(
                time,
                [0, 0].into(),
                0,
                geo_transform,
                grid.clone(),
                CacheHint::default(),
            ),
            RasterTile2D::new(
                time,
                [0, 1].into(),
                0,
                geo_transform,
                grid.clone(),
                CacheHint::default(),
            ),
            RasterTile2D::new(
                time,
                [1, -1].into(),
                0,
                geo_transform,
                grid.clone(),
                CacheHint::default(),
            ),
            RasterTile2D::new(
                time,
                [1, 0].into(),
                0,
                geo_transform,
                grid.clone(),
                CacheHint::default(),
            ),
            RasterTile2D::new(
                time,
                [1, 1].into(),
                0,
                geo_transform,
                grid,
                CacheHint::default(),
            ),
        ],
        tiling_specification: tiling_spec,
    }
}

#[inline(never)]
fn bench_raster_processor<
    T: Pixel,
    F: Fn(TilingSpecification) -> S,
    S: RasterQueryProcessor<RasterType = T>,
>(
    bench_id: &'static str,
    list_of_named_querys: &[(&str, RasterQueryRectangle)],
    list_of_tiling_specs: &[TilingSpecification],
    tile_producing_operator_builderr: F,
    run_time: &tokio::runtime::Runtime,
) {
    for tiling_spec in list_of_tiling_specs {
        let ctx =
            MockQueryContext::with_chunk_size_and_thread_count(ChunkByteSize::MAX, *tiling_spec, 8);

        let operator = (tile_producing_operator_builderr)(*tiling_spec);

        for &(qrect_name, ref qrect) in list_of_named_querys {
            run_time.block_on(async {
                // query the operator
                let start_query = Instant::now();
                let query = operator.raster_query(qrect.clone(), &ctx).await.unwrap();
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
    let qrects = vec![
        (
            "1 tile",
            RasterQueryRectangle::new_with_grid_bounds(
                GridBoundingBox2D::new([-60, 0], [-1, 59]).unwrap(),
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
                BandSelection::first(),
            ),
        ),
        (
            "2 tiles",
            RasterQueryRectangle::new_with_grid_bounds(
                GridBoundingBox2D::new([-50, 0], [9, 59]).unwrap(),
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
                BandSelection::first(),
            ),
        ),
        (
            "4 tiles",
            RasterQueryRectangle::new_with_grid_bounds(
                GridBoundingBox2D::new([-55, -5], [9, 54]).unwrap(),
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
                BandSelection::first(),
            ),
        ),
        (
            "2 tiles, 2 no-data tiles",
            RasterQueryRectangle::new_with_grid_bounds(
                GridBoundingBox2D::new([-120, 130], [59, 189]).unwrap(),
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
                BandSelection::first(),
            ),
        ),
        (
            "empty tiles",
            RasterQueryRectangle::new_with_grid_bounds(
                GridBoundingBox2D::new([-50, -5], [-9, 54]).unwrap(),
                TimeInterval::new(1_000_000_000_000, 1_000_000_000_000 + 1000).unwrap(),
                BandSelection::first(),
            ),
        ),
    ];

    let tiling_specs = vec![TilingSpecification::new([600, 600].into())];

    let run_time = tokio::runtime::Runtime::new().unwrap();

    bench_raster_processor(
        "no_data_tiles",
        &qrects,
        &tiling_specs,
        setup_mock_source,
        &run_time,
    );
    bench_raster_processor(
        "no_data_tiles",
        &qrects,
        &tiling_specs,
        |ts| setup_gdal_source(create_ndvi_meta_data(), ts),
        &run_time,
    );
}

fn bench_tile_size() {
    let qrects = vec![(
        "World in 36000x18000 pixels",
        RasterQueryRectangle::new_with_grid_bounds(
            GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
            TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            BandSelection::first(),
        ),
    )];

    let run_time = tokio::runtime::Runtime::new().unwrap();

    let tiling_specs = vec![
        TilingSpecification::new([32, 32].into()),
        TilingSpecification::new([64, 64].into()),
        TilingSpecification::new([128, 128].into()),
        TilingSpecification::new([256, 256].into()),
        TilingSpecification::new([512, 512].into()),
        TilingSpecification::new([600, 600].into()),
        TilingSpecification::new([900, 900].into()),
        TilingSpecification::new([1024, 1024].into()),
        TilingSpecification::new([2048, 2048].into()),
        TilingSpecification::new([4096, 4096].into()),
        TilingSpecification::new([9000, 9000].into()),
    ];

    bench_raster_processor(
        "tile_size",
        &qrects,
        &tiling_specs,
        |ts| setup_gdal_source(create_ndvi_meta_data(), ts),
        &run_time,
    );
}

fn main() {
    println!("Bench_name, query_name, tilesize_x, tilesize_y, query_time (ns), tiles_produced, pixels_produced, stream_collect_time (ns) ");

    bench_no_data_tiles();
    bench_tile_size();
}
