use futures::StreamExt;
use geo_rand::{GeoRand, GeoRandParameters};
use geoengine_datatypes::collections::{FeatureCollectionInfos, MultiPolygonCollection};
use geoengine_datatypes::primitives::{
    BoundingBox2D, MultiPoint, QueryRectangle, SpatialResolution,
};
use geoengine_datatypes::util::test::TestDefault;
use geoengine_datatypes::{collections::MultiPointCollection, primitives::TimeInterval};
use geoengine_operators::engine::{ChunkByteSize, QueryProcessor};
use geoengine_operators::engine::{MockExecutionContext, MockQueryContext, VectorOperator};
use geoengine_operators::mock::MockFeatureCollectionSource;
use geoengine_operators::processing::{
    PointInPolygonFilter, PointInPolygonFilterParams, PointInPolygonFilterSource,
};
use geoengine_operators::util::Result;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::time::Instant;

async fn pip(points: MultiPointCollection, polygons: MultiPolygonCollection, num_threads: usize) {
    let point_source = MockFeatureCollectionSource::single(points).boxed();

    let polygon_source = MockFeatureCollectionSource::single(polygons).boxed();

    let operator = PointInPolygonFilter {
        params: PointInPolygonFilterParams {},
        sources: PointInPolygonFilterSource {
            points: point_source,
            polygons: polygon_source,
        },
    }
    .boxed()
    .initialize(Default::default(), &MockExecutionContext::test_default())
    .await
    .unwrap();

    let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

    let query_rectangle = QueryRectangle {
        spatial_bounds: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
        time_interval: TimeInterval::default(),
        spatial_resolution: SpatialResolution::zero_point_one(),
    };
    let ctx = MockQueryContext::with_chunk_size_and_thread_count(ChunkByteSize::MAX, num_threads);

    let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

    let res = query
        .map(Result::unwrap)
        .collect::<Vec<MultiPointCollection>>()
        .await;

    assert!(!res.is_empty());
}

fn random_points<T: Rng>(rng: &mut T, num_points: usize) -> MultiPointCollection {
    let coordinates = (0..num_points)
        .into_iter()
        .map(|_| (rng.gen_range(0.0..100.0), rng.gen_range(0.0..100.0)))
        .collect::<Vec<_>>();

    let time = vec![TimeInterval::default(); num_points];

    MultiPointCollection::from_data(
        MultiPoint::many(coordinates).unwrap(),
        time,
        Default::default(),
    )
    .unwrap()
}

fn random_multi_polygons<T: Rng>(
    rng: &mut T,
    polygons_per_multi_polygon: usize,
    multi_polygons: usize,
) -> Vec<geo::MultiPolygon<f64>> {
    let params = GeoRandParameters {
        max_polygons_count: polygons_per_multi_polygon,
        max_polygon_vertices_count: 50,
        max_collisions_count: Some(1),
        min_x: 0.,
        min_y: 0.,
        max_x: 100.,
        max_y: 100.,
    };
    (0..multi_polygons)
        .into_iter()
        .map(|_| geo::MultiPolygon::<f64>::rand(rng, &params))
        .collect()
}

#[tokio::main]
async fn main() {
    const POLYGONS_PER_MULTIPOLYGON: usize = 10;
    const MULTI_POLYGONS: usize = 100;

    let mut rng = StdRng::seed_from_u64(1337);

    let points = random_points(&mut rng, 10_000_000);
    dbg!(points.len());

    let polygons = random_multi_polygons(&mut rng, POLYGONS_PER_MULTIPOLYGON, MULTI_POLYGONS);
    let polygons: MultiPolygonCollection = polygons.into();
    dbg!(polygons.len());

    println!("num_threads,time");
    for num_threads in [1, 2, 4] {
        let start = Instant::now();
        pip(points.clone(), polygons.clone(), num_threads).await;
        println!("{},{:?}", num_threads, start.elapsed());
    }
}
