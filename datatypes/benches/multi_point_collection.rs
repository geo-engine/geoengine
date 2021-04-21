use criterion::{black_box, criterion_group, criterion_main, Criterion};
use geoengine_datatypes::collections::{
    BuilderProvider, GeoFeatureCollectionRowBuilder, MultiPointCollection,
};
use geoengine_datatypes::primitives::{
    Coordinate2D, FeatureDataType, FeatureDataValue, TimeInterval,
};

fn multi_point_collection_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("MultiPointCollection");

    group.bench_function("Builder Plain 100", |b| {
        b.iter(|| {
            let mut builder = MultiPointCollection::builder().finish_header();
            for i in 0..100 {
                builder
                    .push_geometry(Coordinate2D::new(i as f64, i as f64).into())
                    .unwrap();
                builder
                    .push_time_interval(TimeInterval::new_unchecked(i, i + 1))
                    .unwrap();
                builder.finish_row();
            }
            black_box(builder.build())
        })
    });

    group.bench_function("Builder with Number 100", |b| {
        b.iter(|| {
            let mut builder = MultiPointCollection::builder();
            builder
                .add_column("number".into(), FeatureDataType::Float)
                .unwrap();
            let mut builder = builder.finish_header();
            for i in 0..100 {
                builder
                    .push_geometry(Coordinate2D::new(i as f64, i as f64).into())
                    .unwrap();
                builder
                    .push_time_interval(TimeInterval::new_unchecked(i, i + 1))
                    .unwrap();
                builder
                    .push_data("number", FeatureDataValue::Float(i as f64))
                    .unwrap();
                builder.finish_row();
            }
            black_box(builder.build())
        })
    });

    group.finish();
}

criterion_group!(benches, multi_point_collection_benchmarks);
criterion_main!(benches);
