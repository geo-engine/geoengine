#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::print_stderr)] // okay in benchmarks

use futures::StreamExt;
use geo::{Intersects, Translate};
use geoengine_datatypes::collections::{
    FeatureCollectionInfos, MultiPointCollection, MultiPolygonCollection,
};
use geoengine_datatypes::primitives::{
    BoundingBox2D, CacheHint, ColumnSelection, MultiPoint, TimeInterval, VectorQueryRectangle,
};
use geoengine_datatypes::util::test::TestDefault;
use geoengine_operators::engine::{
    ChunkByteSize, MockExecutionContext, QueryProcessor, VectorOperator, WorkflowOperatorPath,
};
use geoengine_operators::mock::MockFeatureCollectionSource;
use geoengine_operators::processing::{
    PointInPolygonFilter, PointInPolygonFilterParams, PointInPolygonFilterSource,
};
use geoengine_operators::util::Result;
use rand::prelude::StdRng;
use rand::{Rng, RngExt, SeedableRng};
use std::time::Instant;

async fn pip(points: MultiPointCollection, polygons: MultiPolygonCollection, num_threads: usize) {
    let point_source = MockFeatureCollectionSource::single(points).boxed();

    let polygon_source = MockFeatureCollectionSource::single(polygons).boxed();

    let exe_ctx = MockExecutionContext::test_default();

    let operator = PointInPolygonFilter {
        params: PointInPolygonFilterParams {},
        sources: PointInPolygonFilterSource {
            points: point_source,
            polygons: polygon_source,
        },
    }
    .boxed()
    .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
    .await
    .unwrap();

    let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

    let query_rectangle = VectorQueryRectangle::new(
        BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
        TimeInterval::default(),
        ColumnSelection::all(),
    );
    let ctx = exe_ctx
        .mock_query_context_with_chunk_size_and_thread_count(ChunkByteSize::MAX, num_threads);

    let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

    let res = query
        .map(Result::unwrap)
        .collect::<Vec<MultiPointCollection>>()
        .await;

    assert!(!res.is_empty());
}

fn random_points<T: Rng>(rng: &mut T, num_points: usize) -> MultiPointCollection {
    let coordinates = (0..num_points)
        .map(|_| (rng.random_range(0.0..100.0), rng.random_range(0.0..100.0)))
        .collect::<Vec<_>>();

    let time = vec![TimeInterval::default(); num_points];

    MultiPointCollection::from_data(
        MultiPoint::many(coordinates).unwrap(),
        time,
        Default::default(),
        CacheHint::default(),
    )
    .unwrap()
}

fn random_multi_polygons<T: Rng>(
    rng: &mut T,
    polygons_per_multi_polygon: usize,
    multi_polygons: usize,
) -> Vec<geo::MultiPolygon<f64>> {
    let params = RandomGeoParameters {
        max_polygons_count: polygons_per_multi_polygon,
        max_polygon_vertices_count: 50,
        max_collisions_count: Some(1),
        min_x: 0.,
        min_y: 0.,
        max_x: 100.,
        max_y: 100.,
    };
    (0..multi_polygons)
        .map(|_| random_multi_polygon(rng, &params))
        .collect()
}

fn random_multi_polygon(
    rng: &mut impl Rng,
    parameters: &RandomGeoParameters,
) -> geo::MultiPolygon<f64> {
    let mut polygons = Vec::with_capacity(parameters.max_polygons_count);
    let mut collisions_count = 0;

    'outer: while parameters
        .max_collisions_count
        .is_none_or(|max_collisions_count| collisions_count < max_collisions_count)
        && polygons.len() < parameters.max_polygons_count
    {
        let new_polygon = random_polygon(rng, parameters);

        if parameters.max_collisions_count.is_some() {
            for polygon in &polygons {
                if new_polygon.intersects(polygon) {
                    collisions_count += 1;
                    continue 'outer;
                }
            }
        }

        polygons.push(new_polygon);
    }

    geo::MultiPolygon(polygons)
}

fn random_polygon(rng: &mut impl Rng, parameters: &RandomGeoParameters) -> geo::Polygon<f64> {
    let bound_x1 = rng.random_range(parameters.min_x..parameters.max_x);
    let bound_y1 = rng.random_range(parameters.min_y..parameters.max_y);
    let bound_x2 = rng.random_range(parameters.min_x..parameters.max_x);
    let bound_y2 = rng.random_range(parameters.min_y..parameters.max_y);

    let (min_x, max_x) = if bound_x1 < bound_x2 {
        (bound_x1, bound_x2)
    } else {
        (bound_x2, bound_x1)
    };

    let (min_y, max_y) = if bound_y1 < bound_y2 {
        (bound_y1, bound_y2)
    } else {
        (bound_y2, bound_y1)
    };

    let translate_x = rng.random_range(parameters.min_x - min_x..parameters.max_x - max_x);
    let translate_y = rng.random_range(parameters.min_y - min_y..parameters.max_y - max_y);
    let vertices_count = rng.random_range(3..parameters.max_polygon_vertices_count);

    let point_parameters = RandomGeoParameters {
        min_x,
        min_y,
        max_x,
        max_y,
        ..*parameters
    };

    let points: Vec<_> = (0..vertices_count)
        .map(|_| random_point(rng, &point_parameters))
        .collect();

    geo::Polygon::new(points_to_contour(&points).unwrap(), Vec::new())
        .translate(translate_x, translate_y)
}

fn random_point(rng: &mut impl Rng, parameters: &RandomGeoParameters) -> geo::Point<f64> {
    geo::Point::new(
        rng.random_range(parameters.min_x..parameters.max_x),
        rng.random_range(parameters.min_y..parameters.max_y),
    )
}

fn points_to_contour(points: &[geo::Point]) -> Option<geo::LineString> {
    fn left_turn_test(point: &geo::Point, other_point: &geo::Point) -> bool {
        ((point.x() * other_point.y()) - (point.y() * other_point.x())) >= 0.
    }

    let first_point = *points.first()?;
    let (left_most, right_most) = points.iter().skip(1).fold(
        (first_point, first_point),
        |(left_most, right_most), &point| {
            (
                if point.x() < left_most.x() {
                    point
                } else {
                    left_most
                },
                if point.x() >= right_most.x() {
                    point
                } else {
                    right_most
                },
            )
        },
    );

    let (mut above_list, mut below_list): (Vec<geo::Point>, Vec<geo::Point>) = points
        .iter()
        .filter(|&&point| point != left_most && point != right_most)
        .partition(|&&point| left_turn_test(&(right_most - left_most), &(point - left_most)));

    above_list.sort_by(|a, b| (a.x() - b.x()).partial_cmp(&0.).unwrap());
    below_list.sort_by(|a, b| (b.x() - a.x()).partial_cmp(&0.).unwrap());

    Some(
        std::iter::once(left_most)
            .chain(above_list)
            .chain(std::iter::once(right_most))
            .chain(below_list)
            .chain(std::iter::once(left_most))
            .collect(),
    )
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct RandomGeoParameters {
    pub max_polygons_count: usize,
    pub max_polygon_vertices_count: usize,
    pub max_collisions_count: Option<u32>,
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

#[tokio::main]
async fn main() {
    const POLYGONS_PER_MULTIPOLYGON: usize = 10;
    const MULTI_POLYGONS: usize = 100;

    let mut rng = StdRng::seed_from_u64(1337);

    let points = random_points(&mut rng, 10_000_000);
    points.len();

    let polygons = random_multi_polygons(&mut rng, POLYGONS_PER_MULTIPOLYGON, MULTI_POLYGONS);
    let polygons: MultiPolygonCollection = polygons.into();
    polygons.len();

    println!("num_threads,time");
    for num_threads in [1, 2, 4] {
        let start = Instant::now();
        pip(points.clone(), polygons.clone(), num_threads).await;
        println!("{},{:?}", num_threads, start.elapsed());
    }
}
