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

async fn tiles_to_png(query: QueryRectangle) -> Vec<u8> {
    let ctx = MockQueryContext::default();
    let tiling_specification = TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

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

fn bench_tiles_to_png(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let query = QueryRectangle {
        bbox: BoundingBox2D::new((-10., 20.).into(), (50., 80.).into()).unwrap(),
        time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        spatial_resolution: SpatialResolution::zero_point_one(),
    };
    c.bench_function("bench_tiles_to_png", move |b| {
        b.to_async(&runtime)
            .iter(|| async { tiles_to_png(query).await })
    });
}

fn bench_empty_tiles_to_png(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let query = QueryRectangle {
        bbox: BoundingBox2D::new((-10., 20.).into(), (50., 80.).into()).unwrap(),
        time_interval: TimeInterval::new(1_000_000_000_000, 1_000_000_000_000 + 1000).unwrap(),
        spatial_resolution: SpatialResolution::zero_point_one(),
    };
    c.bench_function("bench_empty_tiles_to_png", move |b| {
        b.to_async(&runtime)
            .iter(|| async { tiles_to_png(query).await })
    });
}

criterion_group!(benches, bench_tiles_to_png, bench_empty_tiles_to_png);
criterion_main!(benches);
