use criterion::{black_box, criterion_group, criterion_main, Criterion};
use geoengine_datatypes::collections::MultiPointCollection;
use geoengine_datatypes::primitives::{FeatureDataType, FeatureDataValue, TimeInterval};

fn point_collection_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("PointCollection");

    group.bench_function("Builder Plain 100", |b| {
        b.iter(|| {
            let mut builder = MultiPointCollection::builder();
            for i in 0..100 {
                builder
                    .append_coordinate((i as f64, i as f64).into())
                    .unwrap();
                builder
                    .append_time_interval(TimeInterval::new_unchecked(i, i + 1))
                    .unwrap();
                builder.finish_row().unwrap();
            }
            black_box(builder.build())
        })
    });

    group.bench_function("Builder with Number 100", |b| {
        b.iter(|| {
            let mut builder = MultiPointCollection::builder();
            builder
                .add_field("number", FeatureDataType::Number)
                .unwrap();
            for i in 0..100 {
                builder
                    .append_coordinate((i as f64, i as f64).into())
                    .unwrap();
                builder
                    .append_time_interval(TimeInterval::new_unchecked(i, i + 1))
                    .unwrap();
                builder
                    .append_data("number", FeatureDataValue::Number(i as f64))
                    .unwrap();
                builder.finish_row().unwrap();
            }
            black_box(builder.build())
        })
    });

    group.finish();
}

criterion_group!(benches, point_collection_benchmarks);
criterion_main!(benches);
