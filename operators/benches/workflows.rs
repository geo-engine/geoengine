use geoengine_datatypes::{
    primitives::{BoundingBox2D, Coordinate2D, SpatialResolution, TimeInterval},
    raster::TilingSpecification,
};
use geoengine_operators::{
    engine::{MockQueryContext, QueryRectangle, RasterQueryProcessor},
    source::GdalSourceProcessor,
    util::{gdal::create_ndvi_meta_data, raster_stream_to_png::raster_stream_to_png_bytes},
};

use criterion::*;

async fn tiles_to_png(query: QueryRectangle, tile_size: usize) -> Vec<u8> {
    let ctx = MockQueryContext::default();
    let tiling_specification =
        TilingSpecification::new(Coordinate2D::default(), [tile_size, tile_size].into());

    let gdal_source = GdalSourceProcessor::<u8> {
        tiling_specification,
        meta_data: Box::new(create_ndvi_meta_data()),
        phantom_data: Default::default(),
    };

    let image_bytes: Vec<u8> = raster_stream_to_png_bytes::<u8, _>(
        gdal_source.boxed(),
        query,
        ctx,
        600,
        600,
        None,
        None,
        Some(0),
    )
    .await
    .unwrap();

    image_bytes
}

/// This bench will query a single tile
fn bench_600px_1_tile_to_png(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let query = QueryRectangle {
        bbox: BoundingBox2D::new((0., 0.).into(), (60., 60.).into()).unwrap(),
        time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        spatial_resolution: SpatialResolution::zero_point_one(),
    };
    c.bench_function("bench_600px_1_tile_to_png", move |b| {
        b.to_async(&runtime)
            .iter(|| async { tiles_to_png(query, 600).await })
    });
}

/// This bench will query 2 tiles
fn bench_600px_2_tiles_to_png(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let query = QueryRectangle {
        bbox: BoundingBox2D::new((0., -10.).into(), (60., 50.).into()).unwrap(),
        time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        spatial_resolution: SpatialResolution::zero_point_one(),
    };
    c.bench_function("bench_600px_2_tiles_to_png", move |b| {
        b.to_async(&runtime)
            .iter(|| async { tiles_to_png(query, 600).await })
    });
}

/// This bench will query 4 tiles
fn bench_600px_4_tiles_to_png(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let query = QueryRectangle {
        bbox: BoundingBox2D::new((-5., -10.).into(), (55., 50.).into()).unwrap(),
        time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        spatial_resolution: SpatialResolution::zero_point_one(),
    };
    c.bench_function("bench_600px_4_tiles_to_png", move |b| {
        b.to_async(&runtime)
            .iter(|| async { tiles_to_png(query, 600).await })
    });
}

/*
/// This bench will query 2 tiles and 2 no_data tiles by overlapping the opper right coordinate
fn bench_600px_2_tile_2_no_data_tiles_to_png(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let query = QueryRectangle {
        bbox: BoundingBox2D::new((130., 60.).into(), (190., 120.).into()).unwrap(),
        time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        spatial_resolution: SpatialResolution::zero_point_one(),
    };
    c.bench_function("bench_600px_2_tile_2_no_data_tiles_to_png", move |b| {
        b.to_async(&runtime)
            .iter(|| async { tiles_to_png(query, 600).await })
    });
}
*/

/// This bench will query no tiles
fn bench_600px_empty_to_png(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let query = QueryRectangle {
        bbox: BoundingBox2D::new((-5., -10.).into(), (55., 50.).into()).unwrap(),
        time_interval: TimeInterval::new(1_000_000_000_000, 1_000_000_000_000 + 1000).unwrap(),
        spatial_resolution: SpatialResolution::zero_point_one(),
    };
    c.bench_function("bench_600px_empty_to_png", move |b| {
        b.to_async(&runtime)
            .iter(|| async { tiles_to_png(query, 600).await })
    });
}

criterion_group!(
    benches,
    bench_600px_1_tile_to_png,
    bench_600px_2_tiles_to_png,
    bench_600px_4_tiles_to_png,
    bench_600px_2_tile_2_no_data_tiles_to_png,
    bench_600px_empty_to_png
);
criterion_main!(benches);
