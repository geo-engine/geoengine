use futures::StreamExt;
use geoengine_datatypes::collections::MultiPolygonCollection;
use geoengine_datatypes::primitives::{BoundingBox2D, MultiPoint, MultiPolygon, SpatialResolution};
use geoengine_datatypes::{collections::MultiPointCollection, primitives::TimeInterval};
use geoengine_operators::engine::QueryProcessor;
use geoengine_operators::engine::{
    MockExecutionContext, MockQueryContext, QueryRectangle, VectorOperator,
};
use geoengine_operators::mock::MockFeatureCollectionSource;
use geoengine_operators::processing::{
    PointInPolygonFilter, PointInPolygonFilterParams, PointInPolygonFilterSource,
};
use geoengine_operators::util::Result;
use std::time::Instant;

async fn pip(num_threads: usize) {
    let raw_points = vec![(1.0, 1.1); 10_000_000];
    let time = vec![TimeInterval::new(0, 1).unwrap(); 10_000_000];

    let points = MultiPointCollection::from_data(
        MultiPoint::many(raw_points).unwrap(),
        time,
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
        spatial_bounds: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
        time_interval: TimeInterval::default(),
        spatial_resolution: SpatialResolution::zero_point_one(),
    };
    let ctx = MockQueryContext::with_chunk_size_and_thread_count(1024 * 1024, num_threads);

    let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

    let res = query
        .map(Result::unwrap)
        .collect::<Vec<MultiPointCollection>>()
        .await;

    assert!(!res.is_empty());
}

#[tokio::main]
async fn main() {
    println!("num_threads,time");
    for num_threads in [1, 2, 4] {
        let start = Instant::now();
        pip(num_threads).await;
        println!("{},{:?}", num_threads, start.elapsed());
    }
}
