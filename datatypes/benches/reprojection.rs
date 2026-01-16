use criterion::{Criterion, criterion_group, criterion_main};
use geoengine_datatypes::util::mixed_projector::{
    MixedAreaOfUseProvider, MixedCoordinateProjector,
};
use std::hint::black_box;

use geoengine_datatypes::operations::reproject::CoordinateProjection;
use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D};
use geoengine_datatypes::spatial_reference::{AreaOfUseProvider, SpatialReference};
use geoengine_datatypes::util::geodesy_projector::Transformer;

fn bench_proj_reproject_batch(c: &mut Criterion) {
    let from = SpatialReference::epsg_4326();
    let to = SpatialReference::new(
        geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
        3857,
    );

    let projector = MixedCoordinateProjector::from_known_srs(from, to).unwrap();

    let coords: Vec<Coordinate2D> = (0..1000)
        .map(|i| {
            let frac = i as f64 / 1000.0;
            Coordinate2D {
                x: frac * 360.0 - 180.0,
                y: frac * 180.0 - 90.0,
            }
        })
        .collect();

    c.bench_function("reproject_1000_coordinates", |b| {
        b.iter(|| black_box(projector.project_coordinates(black_box(&coords))))
    });
}
fn bench_geodesy_reproject_batch(c: &mut Criterion) {
    let from = SpatialReference::epsg_4326();
    let to = SpatialReference::new(
        geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
        3857,
    );
    let projector = Transformer::from_known_srs(from, to).unwrap();

    let coords: Vec<Coordinate2D> = (0..1000)
        .map(|i| {
            let frac = i as f64 / 1000.0;
            Coordinate2D {
                x: frac * 360.0 - 180.0,
                y: frac * 180.0 - 90.0,
            }
        })
        .collect();

    c.bench_function("reproject_1000_coordinates", |b| {
        b.iter(|| black_box(projector.project_coordinates(black_box(&coords))))
    });
}

fn bench_proj_projector_creation(c: &mut Criterion) {
    let from = SpatialReference::epsg_4326();
    let to = SpatialReference::new(
        geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
        3857,
    );
    c.bench_function("create_projector", |b| {
        b.iter(|| {
            black_box(MixedCoordinateProjector::from_known_srs(
                black_box(from),
                black_box(to),
            ))
        })
    });
}
fn bench_geodesy_projector_creation(c: &mut Criterion) {
    let from = SpatialReference::epsg_4326();
    let to = SpatialReference::new(
        geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
        3857,
    );
    c.bench_function("create_projector", |b| {
        b.iter(|| black_box(Transformer::from_known_srs(black_box(from), black_box(to))))
    });
}

fn bench_area_of_use(c: &mut Criterion) {
    let def = SpatialReference::epsg_4326();

    let projector = MixedAreaOfUseProvider::new_known_crs(def).unwrap();

    c.bench_function("area_of_use", |b| {
        b.iter(|| black_box(projector.area_of_use::<BoundingBox2D>()))
    });
}

fn bench_area_of_use_projected(c: &mut Criterion) {
    let def = SpatialReference::epsg_4326();

    let projector = MixedAreaOfUseProvider::new_known_crs(def).unwrap();

    c.bench_function("area_of_use_projected", |b| {
        b.iter(|| black_box(projector.area_of_use_projected::<BoundingBox2D>()))
    });
}

criterion_group!(
    reprojection_benches,
    bench_geodesy_reproject_batch,
    bench_proj_reproject_batch
);
criterion_group!(
    projector_creation_benches,
    bench_geodesy_projector_creation,
    bench_proj_projector_creation
);
criterion_group!(
    area_of_use_benches,
    bench_area_of_use,
    bench_area_of_use_projected
);
criterion_main!(
    reprojection_benches,
    projector_creation_benches,
    area_of_use_benches
);
