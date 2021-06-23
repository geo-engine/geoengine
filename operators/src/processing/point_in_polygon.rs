use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use snafu::ensure;

use geoengine_datatypes::collections::{
    FeatureCollectionInfos, FeatureCollectionModifications, GeometryCollection,
    MultiPointCollection, MultiPolygonCollection, VectorDataType,
};
use geoengine_datatypes::primitives::{Coordinate2D, TimeInterval};

use crate::adapters::FeatureCollectionChunkMerger;
use crate::engine::QueryProcessor;
use crate::engine::{
    ExecutionContext, InitializedVectorOperator, Operator, QueryContext, TypedVectorQueryProcessor,
    VectorOperator, VectorQueryProcessor, VectorQueryRectangle, VectorResultDescriptor,
};
use crate::error;
use crate::util::Result;
use arrow::array::BooleanArray;
use async_trait::async_trait;

/// The point in polygon filter requires two inputs in the following order:
/// 1. a `MultiPointCollection` source
/// 2. a `MultiPolygonCollection` source
/// Then, it filters the `MultiPolygonCollection`s so that only those features are retained that are in any polygon.
pub type PointInPolygonFilter = Operator<PointInPolygonFilterParams, PointInPolygonFilterSource>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PointInPolygonFilterParams {}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PointInPolygonFilterSource {
    points: Box<dyn VectorOperator>,
    polygons: Box<dyn VectorOperator>,
}

#[typetag::serde]
#[async_trait]
impl VectorOperator for PointInPolygonFilter {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        let points = self.sources.points.initialize(context).await?;
        let polygons = self.sources.polygons.initialize(context).await?;

        ensure!(
            points.result_descriptor().data_type == VectorDataType::MultiPoint,
            error::InvalidType {
                expected: VectorDataType::MultiPoint.to_string(),
                found: points.result_descriptor().data_type.to_string(),
            }
        );
        ensure!(
            polygons.result_descriptor().data_type == VectorDataType::MultiPolygon,
            error::InvalidType {
                expected: VectorDataType::MultiPolygon.to_string(),
                found: polygons.result_descriptor().data_type.to_string(),
            }
        );

        let initialized_operator = InitializedPointInPolygonFilter {
            result_descriptor: points.result_descriptor().clone(),
            points,
            polygons,
        };

        Ok(initialized_operator.boxed())
    }
}

pub struct InitializedPointInPolygonFilter {
    points: Box<dyn InitializedVectorOperator>,
    polygons: Box<dyn InitializedVectorOperator>,
    result_descriptor: VectorResultDescriptor,
}

impl InitializedVectorOperator for InitializedPointInPolygonFilter {
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let point_processor = self
            .points
            .query_processor()?
            .multi_point()
            .expect("checked in `PointInPolygonFilter` constructor");

        let polygon_processor = self
            .polygons
            .query_processor()?
            .multi_polygon()
            .expect("checked in `PointInPolygonFilter` constructor");

        Ok(TypedVectorQueryProcessor::MultiPoint(
            PointInPolygonFilterProcessor::new(point_processor, polygon_processor).boxed(),
        ))
    }

    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }
}

pub struct PointInPolygonFilterProcessor {
    points: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
    polygons: Box<dyn VectorQueryProcessor<VectorType = MultiPolygonCollection>>,
}

impl PointInPolygonFilterProcessor {
    pub fn new(
        points: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
        polygons: Box<dyn VectorQueryProcessor<VectorType = MultiPolygonCollection>>,
    ) -> Self {
        Self { points, polygons }
    }

    fn filter_points(
        points: &MultiPointCollection,
        polygons: &MultiPolygonCollection,
        initial_filter: &BooleanArray,
    ) -> Result<BooleanArray> {
        let mut filter = Vec::with_capacity(points.len());

        let tester = PointInPolygonTester::new(polygons);

        let coordinates = points.coordinates();

        for ((coordinates_start_index, coordinates_end_index), time_interval) in
            two_tuple_windows(points.feature_offsets().iter().map(|&c| c as usize))
                .zip(points.time_intervals())
        {
            let is_multi_point_in_polygon_collection = coordinates
                [coordinates_start_index..coordinates_end_index]
                .iter()
                .any(|coordinate| tester.is_coordinate_in_any_polygon(coordinate, time_interval));

            filter.push(is_multi_point_in_polygon_collection);
        }

        arrow::compute::or(initial_filter, &filter.into()).map_err(Into::into)
    }
}

#[async_trait]
impl VectorQueryProcessor for PointInPolygonFilterProcessor {
    type VectorType = MultiPointCollection;

    async fn vector_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        // TODO: multi-threading

        let filtered_stream =
            self.points
                .query(query, ctx)
                .await?
                .and_then(move |points| async move {
                    let initial_filter = BooleanArray::from(vec![false; points.len()]);

                    let filter = self
                        .polygons
                        .query(query, ctx)
                        .await?
                        .fold(Ok(initial_filter), |filter, polygons| async {
                            let polygons = polygons?;

                            if polygons.is_empty() {
                                return filter;
                            }

                            Self::filter_points(&points, &polygons, &filter?)
                        })
                        .await?;

                    points.filter(filter).map_err(Into::into)
                });

        Ok(
            FeatureCollectionChunkMerger::new(filtered_stream.fuse(), ctx.chunk_byte_size())
                .boxed(),
        )
    }
}

/// Creates a context to check points against polygons
///
/// The algorithm is taken from <http://alienryderflex.com/polygon/>
///
struct PointInPolygonTester<'p> {
    polygons: &'p MultiPolygonCollection,
    constants: Vec<f64>,
    multiples: Vec<f64>,
}

impl<'p> PointInPolygonTester<'p> {
    pub fn new(polygons: &'p MultiPolygonCollection) -> Self {
        let number_of_coordinates = polygons.coordinates().len();

        let mut tester = Self {
            polygons,
            constants: vec![0.; number_of_coordinates],
            multiples: vec![0.; number_of_coordinates],
        };

        tester.precalculate_polygons();

        tester
    }

    fn precalculate_polygons(&mut self) {
        for (ring_start_index, ring_end_index) in
            two_tuple_windows(self.polygons.ring_offsets().iter().map(|&c| c as usize))
        {
            self.precalculate_ring(ring_start_index, ring_end_index);
        }
    }

    #[allow(clippy::suspicious_operation_groupings)]
    fn precalculate_ring(&mut self, ring_start_index: usize, ring_end_index: usize) {
        let number_of_corners = ring_end_index - ring_start_index - 1;
        let mut j = number_of_corners - 1;

        let polygon_coordinates = self.polygons.coordinates();

        for i in 0..number_of_corners {
            let c_i = polygon_coordinates[ring_start_index + i];
            let c_j = polygon_coordinates[ring_start_index + j];

            let helper_array_index = ring_start_index + i;

            if float_cmp::approx_eq!(f64, c_j.y, c_i.y) {
                self.constants[helper_array_index] = c_i.x;
                self.multiples[helper_array_index] = 0.0;
            } else {
                self.constants[helper_array_index] =
                    c_i.x - (c_i.y * c_j.x) / (c_j.y - c_i.y) + (c_i.y * c_i.x) / (c_j.y - c_i.y);
                self.multiples[helper_array_index] = (c_j.x - c_i.x) / (c_j.y - c_i.y);
            }

            j = i;
        }
    }

    fn is_coordinate_in_ring(
        &self,
        coordinate: &Coordinate2D,
        ring_index_start: usize,
        ring_index_stop: usize,
    ) -> bool {
        let number_of_corners = ring_index_stop - ring_index_start - 1;
        let mut j = number_of_corners - 1;
        let mut odd_nodes = false;

        let polygon_coordinates = self.polygons.coordinates();

        for i in 0..number_of_corners {
            let c_i = polygon_coordinates[ring_index_start + i];
            let c_j = polygon_coordinates[ring_index_start + j];

            if (c_i.y < coordinate.y && c_j.y >= coordinate.y)
                || (c_j.y < coordinate.y && c_i.y >= coordinate.y)
            {
                let coordinate_index = ring_index_start + i;

                odd_nodes ^= coordinate.y * self.multiples[coordinate_index]
                    + self.constants[coordinate_index]
                    < coordinate.x;
            }

            j = i;
        }

        odd_nodes
    }

    fn coordinate_in_multi_polygon_iter(
        &'p self,
        coordinate: &'p Coordinate2D,
        time_interval: &'p TimeInterval,
    ) -> impl Iterator<Item = bool> + 'p {
        let polygon_offsets = self.polygons.polygon_offsets();
        let ring_offsets = self.polygons.ring_offsets();

        let time_intervals = self.polygons.time_intervals();

        two_tuple_windows(self.polygons.feature_offsets().iter().map(|&c| c as usize))
            .zip(time_intervals)
            .map(
                move |(
                    (multi_polygon_start_index, multi_polygon_end_index),
                    multi_polygon_time_interval,
                )| {
                    if !multi_polygon_time_interval.intersects(time_interval) {
                        return false;
                    }

                    let mut is_coordinate_in_multi_polygon = false;

                    for (polygon_start_index, polygon_end_index) in two_tuple_windows(
                        polygon_offsets[multi_polygon_start_index..=multi_polygon_end_index]
                            .iter()
                            .map(|&c| c as usize),
                    ) {
                        let mut is_coordinate_in_polygon = true;

                        for (ring_number, (ring_start_index, ring_end_index)) in two_tuple_windows(
                            ring_offsets[polygon_start_index..=polygon_end_index]
                                .iter()
                                .map(|&c| c as usize),
                        )
                        .enumerate()
                        {
                            let is_coordinate_in_ring = self.is_coordinate_in_ring(
                                coordinate,
                                ring_start_index,
                                ring_end_index,
                            );

                            if (ring_number == 0 && !is_coordinate_in_ring)
                                || (ring_number > 0 && is_coordinate_in_ring)
                            {
                                // coordinate is either "not in outer ring" or "in inner ring"
                                is_coordinate_in_polygon = false;
                                break;
                            }
                        }

                        if is_coordinate_in_polygon {
                            is_coordinate_in_multi_polygon = true;
                            break;
                        }
                    }

                    is_coordinate_in_multi_polygon
                },
            )
    }

    /// Is the coordinate contained in any polygon of the collection?
    ///
    /// The function returns `true` if the `Coordinate2D` is inside the multi polygon, or
    /// `false` if it is not. If the point is exactly on the edge of the polygon,
    /// then the function may return `true` or `false`.
    ///
    /// TODO: check boundary conditions separately
    ///
    pub fn is_coordinate_in_any_polygon(
        &self,
        coordinate: &Coordinate2D,
        time_interval: &'p TimeInterval,
    ) -> bool {
        self.coordinate_in_multi_polygon_iter(coordinate, time_interval)
            .any(std::convert::identity)
    }

    #[allow(dead_code)]
    pub fn multi_polygons_containing_coordinate(
        &self,
        coordinate: &Coordinate2D,
        time_interval: &'p TimeInterval,
    ) -> Vec<bool> {
        self.coordinate_in_multi_polygon_iter(coordinate, time_interval)
            .collect()
    }
}

/// Loop through an iterator by yielding the current and previous tuple. Starts with the
/// (first, second) item, so the iterator must have more than one item to create an output.
fn two_tuple_windows<I, T>(mut iter: I) -> impl Iterator<Item = (T, T)>
where
    I: Iterator<Item = T>,
    T: Copy,
{
    let mut last = iter.next();

    iter.map(move |item| {
        let output = (last.unwrap(), item);
        last = Some(item);
        output
    })
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::primitives::{
        BoundingBox2D, MultiPoint, MultiPolygon, SpatialResolution, TimeInterval,
    };

    use crate::{engine::VectorQueryRectangle, mock::MockFeatureCollectionSource};

    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};

    #[test]
    fn point_in_polygon_tester() {
        let collection = MultiPolygonCollection::from_data(
            vec![MultiPolygon::new(vec![
                vec![vec![
                    Coordinate2D::new(20., 20.),
                    Coordinate2D::new(30., 20.),
                    Coordinate2D::new(30., 30.),
                    Coordinate2D::new(20., 30.),
                    Coordinate2D::new(20., 20.),
                ]],
                vec![
                    vec![
                        Coordinate2D::new(0., 0.),
                        Coordinate2D::new(10., 0.),
                        Coordinate2D::new(10., 10.),
                        Coordinate2D::new(0., 10.),
                        Coordinate2D::new(0., 0.),
                    ],
                    vec![
                        Coordinate2D::new(1., 5.),
                        Coordinate2D::new(3., 3.),
                        Coordinate2D::new(5., 3.),
                        Coordinate2D::new(6., 5.),
                        Coordinate2D::new(7., 1.5),
                        Coordinate2D::new(4., 0.),
                        Coordinate2D::new(2., 1.),
                        Coordinate2D::new(1., 3.),
                        Coordinate2D::new(1., 5.),
                    ],
                ],
            ])
            .unwrap()],
            vec![Default::default(); 1],
            Default::default(),
        )
        .unwrap();

        let tester = PointInPolygonTester::new(&collection);

        assert!(!tester.is_coordinate_in_ring(&Coordinate2D::new(4., 5.), 0, 5));
        assert!(tester.is_coordinate_in_ring(&Coordinate2D::new(4., 5.), 5, 10));
        assert!(!tester.is_coordinate_in_ring(&Coordinate2D::new(4., 5.), 10, 19));

        assert!(!tester.is_coordinate_in_ring(&Coordinate2D::new(4., 2.), 0, 5));
        assert!(tester.is_coordinate_in_ring(&Coordinate2D::new(4., 2.), 5, 10));
        assert!(tester.is_coordinate_in_ring(&Coordinate2D::new(4., 2.), 10, 19));

        assert!(
            tester.is_coordinate_in_any_polygon(&Coordinate2D::new(4., 5.), &Default::default())
        );
        assert!(
            !tester.is_coordinate_in_any_polygon(&Coordinate2D::new(4., 2.), &Default::default()),
        );

        assert_eq!(
            tester.multi_polygons_containing_coordinate(
                &Coordinate2D::new(4., 5.),
                &Default::default()
            ),
            vec![true]
        );
        assert_eq!(
            tester.multi_polygons_containing_coordinate(
                &Coordinate2D::new(4., 2.),
                &Default::default()
            ),
            vec![false]
        );
    }

    #[test]
    fn point_in_polygon_boundary_conditions() {
        let collection = MultiPolygonCollection::from_data(
            vec![MultiPolygon::new(vec![vec![vec![
                (0.0, 0.0).into(),
                (10.0, 0.0).into(),
                (10.0, 10.0).into(),
                (0.0, 10.0).into(),
                (0.0, 0.0).into(),
            ]]])
            .unwrap()],
            vec![Default::default(); 1],
            Default::default(),
        )
        .unwrap();

        let tester = PointInPolygonTester::new(&collection);

        // the algorithm is not stable for boundary cases directly on the edges

        assert!(tester.is_coordinate_in_any_polygon(
            &Coordinate2D::new(0.000_001, 0.000_001),
            &Default::default()
        ),);
        assert!(tester
            .is_coordinate_in_any_polygon(&Coordinate2D::new(0.000_001, 0.1), &Default::default()),);
        assert!(tester
            .is_coordinate_in_any_polygon(&Coordinate2D::new(0.1, 0.000_001), &Default::default()),);

        assert!(
            tester.is_coordinate_in_any_polygon(&Coordinate2D::new(9.9, 9.9), &Default::default()),
        );
        assert!(
            tester.is_coordinate_in_any_polygon(&Coordinate2D::new(10.0, 9.9), &Default::default()),
        );
        assert!(
            tester.is_coordinate_in_any_polygon(&Coordinate2D::new(9.9, 10.0), &Default::default()),
        );

        assert!(!tester
            .is_coordinate_in_any_polygon(&Coordinate2D::new(-0.1, -0.1), &Default::default()),);
        assert!(!tester
            .is_coordinate_in_any_polygon(&Coordinate2D::new(0.0, -0.1), &Default::default()),);
        assert!(!tester
            .is_coordinate_in_any_polygon(&Coordinate2D::new(-0.1, 0.0), &Default::default()),);

        assert!(!tester
            .is_coordinate_in_any_polygon(&Coordinate2D::new(10.1, 10.1), &Default::default()),);
        assert!(!tester
            .is_coordinate_in_any_polygon(&Coordinate2D::new(10.1, 9.9), &Default::default()),);
        assert!(!tester
            .is_coordinate_in_any_polygon(&Coordinate2D::new(9.9, 10.1), &Default::default()),);
    }

    #[tokio::test]
    async fn all() -> Result<()> {
        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.001, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            Default::default(),
        )?;

        let point_source = MockFeatureCollectionSource::single(points.clone()).boxed();

        let polygon_source =
            MockFeatureCollectionSource::single(MultiPolygonCollection::from_data(
                vec![MultiPolygon::new(vec![vec![vec![
                    (0.0, 0.0).into(),
                    (10.0, 0.0).into(),
                    (10.0, 10.0).into(),
                    (0.0, 10.0).into(),
                    (0.0, 0.0).into(),
                ]]])?],
                vec![TimeInterval::new_unchecked(0, 1); 1],
                Default::default(),
            )?)
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
        .await?;

        let query_processor = operator.query_processor()?.multi_point().unwrap();

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::new(usize::MAX);

        let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        assert_eq!(result[0], points);

        Ok(())
    }

    #[tokio::test]
    async fn none() -> Result<()> {
        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            Default::default(),
        )?;

        let point_source = MockFeatureCollectionSource::single(points.clone()).boxed();

        let polygon_source = MockFeatureCollectionSource::single(
            MultiPolygonCollection::from_data(vec![], vec![], Default::default())?,
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
        .await?;

        let query_processor = operator.query_processor()?.multi_point().unwrap();

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::new(usize::MAX);

        let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn time() -> Result<()> {
        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![(1.0, 1.1), (2.0, 2.1), (3.0, 3.1)]).unwrap(),
            vec![
                TimeInterval::new(0, 1)?,
                TimeInterval::new(5, 6)?,
                TimeInterval::new(0, 5)?,
            ],
            Default::default(),
        )?;

        let point_source = MockFeatureCollectionSource::single(points.clone()).boxed();

        let polygon = MultiPolygon::new(vec![vec![vec![
            (0.0, 0.0).into(),
            (10.0, 0.0).into(),
            (10.0, 10.0).into(),
            (0.0, 10.0).into(),
            (0.0, 0.0).into(),
        ]]])?;

        let polygon_source =
            MockFeatureCollectionSource::single(MultiPolygonCollection::from_data(
                vec![polygon.clone(), polygon],
                vec![TimeInterval::new(0, 1)?, TimeInterval::new(1, 2)?],
                Default::default(),
            )?)
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
        .await?;

        let query_processor = operator.query_processor()?.multi_point().unwrap();

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::new(usize::MAX);

        let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        assert_eq!(result[0], points.filter(vec![true, false, true])?);

        Ok(())
    }

    #[tokio::test]
    async fn multiple_inputs() -> Result<()> {
        let points1 = MultiPointCollection::from_data(
            MultiPoint::many(vec![(5.0, 5.1), (15.0, 15.1)]).unwrap(),
            vec![TimeInterval::new(0, 1)?; 2],
            Default::default(),
        )?;
        let points2 = MultiPointCollection::from_data(
            MultiPoint::many(vec![(6.0, 6.1), (16.0, 16.1)]).unwrap(),
            vec![TimeInterval::new(1, 2)?; 2],
            Default::default(),
        )?;

        let point_source =
            MockFeatureCollectionSource::multiple(vec![points1.clone(), points2.clone()]).boxed();

        let polygon1 = MultiPolygon::new(vec![vec![vec![
            (0.0, 0.0).into(),
            (10.0, 0.0).into(),
            (10.0, 10.0).into(),
            (0.0, 10.0).into(),
            (0.0, 0.0).into(),
        ]]])?;
        let polygon2 = MultiPolygon::new(vec![vec![vec![
            (10.0, 10.0).into(),
            (20.0, 10.0).into(),
            (20.0, 20.0).into(),
            (10.0, 20.0).into(),
            (10.0, 10.0).into(),
        ]]])?;

        let polygon_source = MockFeatureCollectionSource::multiple(vec![
            MultiPolygonCollection::from_data(
                vec![polygon1.clone()],
                vec![TimeInterval::new(0, 1)?],
                Default::default(),
            )?,
            MultiPolygonCollection::from_data(
                vec![polygon1, polygon2],
                vec![TimeInterval::new(1, 2)?, TimeInterval::new(1, 2)?],
                Default::default(),
            )?,
        ])
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
        .await?;

        let query_processor = operator.query_processor()?.multi_point().unwrap();

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };

        let ctx_one_chunk = MockQueryContext::new(usize::MAX);
        let ctx_minimal_chunks = MockQueryContext::new(0);

        let query = query_processor
            .query(query_rectangle, &ctx_minimal_chunks)
            .await
            .unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 2);

        assert_eq!(result[0], points1.filter(vec![true, false])?);
        assert_eq!(result[1], points2);

        let query = query_processor
            .query(query_rectangle, &ctx_one_chunk)
            .await
            .unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0],
            points1.filter(vec![true, false])?.append(&points2)?
        );

        Ok(())
    }
}
