use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

// Replace 'your_spatial_crate' with your actual crate name
use geoengine_datatypes::primitives::Coordinate2D;
use geoengine_datatypes::spatial_reference::{
    CoordinateProjection, GeodesyCoordinateProjector, ProjCoordinateProjector, SpatialReference,
    SpatialReferenceAuthority,
};

// Helper function to generate a dynamic set of dummy coordinates
fn generate_dummy_coordinates(count: usize) -> Vec<Coordinate2D> {
    (0..count)
        .map(|i| Coordinate2D {
            x: 13.404954 + (i as f64 * 0.0001), // Incremental Lon (around Berlin)
            y: 52.520008 + (i as f64 * 0.0001), // Incremental Lat
        })
        .collect()
}

fn bench_coordinate_projections(c: &mut Criterion) {
    // Define the distinct well-known coordinate systems to test
    let test_cases = vec![
        ("EPSG:4326 to EPSG:3857 (Web Mercator)", 4326, 3857),
        ("EPSG:4326 to EPSG:25832 (UTM 32N)", 4326, 25832),
    ];

    // Input collection sizes to see how performance scales linearly or otherwise
    let input_sizes = [10, 100, 1000, 10000];

    for (srs_label, from_code, to_code) in test_cases {
        // Create a distinct benchmark group for each SRS pair
        let mut group = c.benchmark_group(format!("Projection/{}", srs_label));

        for &size in &input_sizes {
            let coords = generate_dummy_coordinates(size);

            // 1. Benchmark ProjCoordinateProjector
            // We instantiate the projector OUTSIDE the b.iter loop so we only measure transformation speed.
            let proj_instance = ProjCoordinateProjector::from_known_srs(
                SpatialReference::new(SpatialReferenceAuthority::Epsg, from_code),
                SpatialReference::new(SpatialReferenceAuthority::Epsg, to_code),
            )
            .expect("Failed to initialize ProjCoordinateProjector");

            group.bench_with_input(
                BenchmarkId::new("ProjCrate", size),
                &coords,
                |b, input_coords| {
                    b.iter(|| {
                        let _r =
                            criterion::black_box(proj_instance.project_coordinates(input_coords));
                    });
                },
            );

            // 2. Benchmark GeodesyCoordinateProjector
            let geodesy_instance = GeodesyCoordinateProjector::from_known_srs(
                SpatialReference::new(SpatialReferenceAuthority::Epsg, from_code),
                SpatialReference::new(SpatialReferenceAuthority::Epsg, to_code),
            )
            .expect("Failed to initialize GeodesyCoordinateProjector");

            group.bench_with_input(
                BenchmarkId::new("GeodesyCrate", size),
                &coords,
                |b, input_coords| {
                    b.iter(|| {
                        let _r = criterion::black_box(
                            geodesy_instance.project_coordinates(input_coords),
                        );
                    });
                },
            );
        }
        group.finish();
    }
}

fn bench_projection_setup(c: &mut Criterion) {
    let test_cases = vec![
        ("EPSG:4326 to EPSG:3857 (Web Mercator)", 4326, 3857),
        ("EPSG:4326 to EPSG:25832 (UTM 32N)", 4326, 25832),
    ];

    for (srs_label, from_code, to_code) in test_cases {
        // Grouping benchmarks by the SRS pair to easily compare backends
        let mut group = c.benchmark_group(format!("Setup Costs/{}", srs_label));

        // Benchmark ProjCoordinateProjector Instantiation
        group.bench_function("ProjCrate Setup", |b| {
            b.iter(|| {
                let _ = criterion::black_box(ProjCoordinateProjector::from_known_srs(
                    SpatialReference::new(SpatialReferenceAuthority::Epsg, from_code),
                    SpatialReference::new(SpatialReferenceAuthority::Epsg, to_code),
                ))
                .expect("Failed to initialize ProjCoordinateProjector");
            });
        });

        // Benchmark GeodesyCoordinateProjector Instantiation
        group.bench_function("GeodesyCrate Setup", |b| {
            b.iter(|| {
                let _ = criterion::black_box(GeodesyCoordinateProjector::from_known_srs(
                    SpatialReference::new(SpatialReferenceAuthority::Epsg, from_code),
                    SpatialReference::new(SpatialReferenceAuthority::Epsg, to_code),
                ))
                .expect("Failed to initialize GeodesyCoordinateProjector");
            });
        });

        group.finish();
    }
}

criterion_group!(execution_benches, bench_coordinate_projections);
criterion_group!(setup_benches, bench_projection_setup);

criterion_main!(execution_benches, setup_benches);
