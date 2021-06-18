use criterion::{black_box, criterion_group, criterion_main, Criterion};
use futures::StreamExt;
use geoengine_datatypes::collections::MultiPolygonCollection;
use geoengine_datatypes::primitives::{BoundingBox2D, MultiPoint, MultiPolygon, SpatialResolution};
use geoengine_datatypes::{collections::MultiPointCollection, primitives::TimeInterval};
use geoengine_operators::engine::{
    MockExecutionContext, MockQueryContext, QueryProcessor, QueryRectangle, VectorOperator,
};
use geoengine_operators::mock::MockFeatureCollectionSource;
use geoengine_operators::processing::{
    PointInPolygonFilter, PointInPolygonFilterParams, PointInPolygonFilterSource,
};
use geoengine_operators::util::Result;

async fn pip(num_threads: usize) {
    let points = MultiPointCollection::from_data(
        MultiPoint::many(vec![(1.0, 1.1), (2.0, 2.1), (3.0, 3.1)]).unwrap(),
        vec![
            TimeInterval::new(0, 1).unwrap(),
            TimeInterval::new(5, 6).unwrap(),
            TimeInterval::new(0, 5).unwrap(),
        ],
        Default::default(),
    )
    .unwrap();

    let point_source = MockFeatureCollectionSource::single(points.clone()).boxed();

    let polygon = MultiPolygon::new(vec![vec![vec![
        (0.0, 0.0).into(),
        (10.0, 0.0).into(),
        (10.0, 10.0).into(),
        (0.0, 10.0).into(),
        (0.0, 0.0).into(),
    ]]])
    .unwrap();

    let polygon_source = MockFeatureCollectionSource::single(
        MultiPolygonCollection::from_data(
            vec![polygon.clone(), polygon],
            vec![
                TimeInterval::new(0, 1).unwrap(),
                TimeInterval::new(1, 2).unwrap(),
            ],
            Default::default(),
        )
        .unwrap(),
    )
    .boxed();

    let operator = PointInPolygonFilter {
        params: PointInPolygonFilterParams {},
        sources: PointInPolygonFilterSource {
            points: point_source,
            polygons: polygon_source,
        },
    }
    .boxed()
    .initialize(&MockExecutionContext::default())
    .await
    .unwrap();

    let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

    let query_rectangle = QueryRectangle {
        bbox: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
        time_interval: TimeInterval::default(),
        spatial_resolution: SpatialResolution::zero_point_one(),
    };
    let ctx = MockQueryContext::with_chunk_size_and_thread_count(1024 * 1024, num_threads);

    let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

    query
        .map(Result::unwrap)
        .collect::<Vec<MultiPointCollection>>()
        .await;
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    for num_threads in [1, 2, 4, 8] {
        c.bench_function(&format!("{} thread", num_threads), |b| {
            b.to_async(&rt).iter(|| black_box(pip(num_threads)))
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
