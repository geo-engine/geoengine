#![allow(clippy::unwrap_used)] // okay in benchmarks

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use geoengine_datatypes::collections::{
    BuilderProvider, FeatureCollectionInfos, FeatureCollectionModifications,
    GeoFeatureCollectionRowBuilder, MultiPointCollection,
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
                builder.push_geometry(Coordinate2D::new(i as f64, i as f64).into());
                builder.push_time_interval(TimeInterval::new_unchecked(i, i + 1));
                builder.finish_row();
            }
            black_box(builder.build())
        });
    });

    group.bench_function("Builder with Number 100", |b| {
        b.iter(|| {
            let mut builder = MultiPointCollection::builder();
            builder
                .add_column("number".into(), FeatureDataType::Float)
                .unwrap();
            let mut builder = builder.finish_header();
            for i in 0..100 {
                builder.push_geometry(Coordinate2D::new(i as f64, i as f64).into());
                builder.push_time_interval(TimeInterval::new_unchecked(i, i + 1));
                builder
                    .push_data("number", FeatureDataValue::Float(i as f64))
                    .unwrap();
                builder.finish_row();
            }
            black_box(builder.build())
        });
    });

    group.bench_function("Filter multi point chunk", |b| {
        b.iter_batched(
            || {
                let mut builder = MultiPointCollection::builder();
                builder
                    .add_column("float".into(), FeatureDataType::Float)
                    .unwrap();
                builder
                    .add_column("int".into(), FeatureDataType::Int)
                    .unwrap();
                builder
                    .add_column("text".into(), FeatureDataType::Text)
                    .unwrap();
                let mut builder = builder.finish_header();
                for i in 0.. {
                    builder.push_geometry(Coordinate2D::new(i as f64, i as f64).into());
                    builder.push_time_interval(TimeInterval::new_unchecked(i, i + 1));

                    // add some nulls
                    if i % 10 == 0 {
                        builder.push_null("float").unwrap();
                        builder.push_null("int").unwrap();
                        builder.push_null("text").unwrap();
                    } else {
                        builder
                            .push_data("float", FeatureDataValue::Float(i as f64))
                            .unwrap();
                        builder.push_data("int", FeatureDataValue::Int(i)).unwrap();
                        builder
                            .push_data("text", FeatureDataValue::Text(i.to_string()))
                            .unwrap();
                    }

                    builder.finish_row();

                    // from settings-default.toml
                    if builder.estimate_memory_size() >= 1_048_576 {
                        break;
                    }
                }
                builder.build().unwrap()
            },
            |data| {
                let third = data.len() / 3;

                data.column_range_filter(
                    "float",
                    &[
                        FeatureDataValue::Float(0.)..=FeatureDataValue::Float(third as f64),
                        FeatureDataValue::Float(2. * third as f64)
                            ..=FeatureDataValue::Float(3. * third as f64),
                    ],
                    true,
                )
                .unwrap();

                black_box(data)
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, multi_point_collection_benchmarks);
criterion_main!(benches);
