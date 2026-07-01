use criterion::{Criterion, criterion_group, criterion_main};

// Replace 'your_spatial_crate' with your actual crate name
use geoengine_datatypes::{
    primitives::BoundingBox2D,
    spatial_reference::{
        AreaOfUseProvider, ProjAreaOfUseProvider, SpatialReference, SpatialReferenceAuthority,
        StaticEpsgAreaProvider,
    },
};

fn get_test_cases() -> Vec<(&'static str, u32)> {
    vec![
        ("EPSG:4326 (Global WGS84)", 4326),
        ("EPSG:25832 (UTM Zone 32N / Regional)", 25832),
    ]
}

fn bench_area_execution(c: &mut Criterion) {
    for (srs_label, code) in get_test_cases() {
        // Setup Proj provider outside the loop
        let proj_provider = ProjAreaOfUseProvider::new_known_crs(SpatialReference::new(
            SpatialReferenceAuthority::Epsg,
            code,
        ))
        .expect("Failed to initialize ProjAreaOfUseProvider");

        // Setup Static provider outside the loop
        let static_provider = StaticEpsgAreaProvider::new_known_crs(SpatialReference::new(
            SpatialReferenceAuthority::Epsg,
            code,
        ))
        .expect("Failed to initialize StaticEpsgAreaProvider");
        {
            let mut group = c.benchmark_group(format!("Area of Use (Standard)/{}", srs_label));

            group.bench_function("ProjProvider - area_of_use", |b| {
                b.iter(|| {
                    // Using turbofish syntax to specify BoundingBox2D as the generic target
                    let _res = criterion::black_box(proj_provider.area_of_use::<BoundingBox2D>());
                });
            });

            group.bench_function("StaticProvider - area_of_use", |b| {
                b.iter(|| {
                    let _res = criterion::black_box(static_provider.area_of_use::<BoundingBox2D>());
                });
            });
            group.finish();
        }

        {
            let mut group = c.benchmark_group(format!("Area of Use (Projected)/{}", srs_label));

            group.bench_function("ProjProvider - area_of_use_projected", |b| {
                b.iter(|| {
                    let _res = criterion::black_box(
                        proj_provider.area_of_use_projected::<BoundingBox2D>(),
                    );
                });
            });

            group.bench_function("StaticProvider - area_of_use_projected", |b| {
                b.iter(|| {
                    let _res = criterion::black_box(
                        static_provider.area_of_use_projected::<BoundingBox2D>(),
                    );
                });
            });
            group.finish();
        }
    }
}

fn bench_area_setup(c: &mut Criterion) {
    for (srs_label, code) in get_test_cases() {
        let mut group = c.benchmark_group(format!("Area Setup/{}", srs_label));

        // Benchmark Proj Provider initialization
        group.bench_function("ProjProvider Setup", |b| {
            b.iter(|| {
                let _provider = criterion::black_box(ProjAreaOfUseProvider::new_known_crs(
                    SpatialReference::new(SpatialReferenceAuthority::Epsg, code),
                ));
            });
        });

        // Benchmark Static Provider initialization
        group.bench_function("StaticProvider Setup", |b| {
            b.iter(|| {
                let _provider = criterion::black_box(StaticEpsgAreaProvider::new_known_crs(
                    SpatialReference::new(SpatialReferenceAuthority::Epsg, code),
                ));
            });
        });

        group.finish();
    }
}

// --- CRITERION REGISTRATION ---

criterion_group!(area_execution_benches, bench_area_execution);
criterion_group!(area_setup_benches, bench_area_setup);

criterion_main!(area_execution_benches, area_setup_benches);
