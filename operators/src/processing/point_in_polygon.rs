mod tester;
mod wrapper;

use std::cmp::min;
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::dataset::NamedData;
use geoengine_datatypes::primitives::CacheHint;
use geoengine_datatypes::primitives::VectorQueryRectangle;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::adapters::FeatureCollectionChunkMerger;
use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedSources, InitializedVectorOperator, Operator,
    OperatorName, QueryContext, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor,
    VectorResultDescriptor, WorkflowOperatorPath,
};
use crate::engine::{OperatorData, QueryProcessor};
use crate::error::{self, Error};
use crate::util::Result;
use arrow::array::BooleanArray;
use async_trait::async_trait;
use geoengine_datatypes::collections::{
    FeatureCollectionInfos, FeatureCollectionModifications, GeometryCollection,
    MultiPointCollection, MultiPolygonCollection, VectorDataType,
};
pub use tester::PointInPolygonTester;
pub use wrapper::PointInPolygonTesterWithCollection;

/// The point in polygon filter requires two inputs in the following order:
/// 1. a `MultiPointCollection` source
/// 2. a `MultiPolygonCollection` source
///
/// Then, it filters the `MultiPolygonCollection`s so that only those features are retained that are in any polygon.
pub type PointInPolygonFilter = Operator<PointInPolygonFilterParams, PointInPolygonFilterSource>;

impl OperatorName for PointInPolygonFilter {
    const TYPE_NAME: &'static str = "PointInPolygonFilter";
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PointInPolygonFilterParams {}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PointInPolygonFilterSource {
    pub points: Box<dyn VectorOperator>,
    pub polygons: Box<dyn VectorOperator>,
}

impl OperatorData for PointInPolygonFilterSource {
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        self.points.data_names_collect(data_names);
        self.polygons.data_names_collect(data_names);
    }
}

struct InitializedPointInPolygonFilterSource {
    points: Box<dyn InitializedVectorOperator>,
    polygons: Box<dyn InitializedVectorOperator>,
}

#[async_trait]
impl InitializedSources<InitializedPointInPolygonFilterSource> for PointInPolygonFilterSource {
    async fn initialize_sources(
        self,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<InitializedPointInPolygonFilterSource> {
        let points_path = path.clone_and_append(0);
        let polygons_path = path.clone_and_append(1);

        Ok(InitializedPointInPolygonFilterSource {
            points: self.points.initialize(points_path, context).await?,
            polygons: self.polygons.initialize(polygons_path, context).await?,
        })
    }
}

#[typetag::serde]
#[async_trait]
impl VectorOperator for PointInPolygonFilter {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        let name = CanonicOperatorName::from(&self);

        let initialized_source = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?;

        let points_rd = initialized_source.points.result_descriptor();
        let polygons_rd = initialized_source.polygons.result_descriptor();

        ensure!(
            points_rd.data_type == VectorDataType::MultiPoint,
            error::InvalidType {
                expected: VectorDataType::MultiPoint.to_string(),
                found: points_rd.data_type.to_string(),
            }
        );
        ensure!(
            polygons_rd.data_type == VectorDataType::MultiPolygon,
            error::InvalidType {
                expected: VectorDataType::MultiPolygon.to_string(),
                found: polygons_rd.data_type.to_string(),
            }
        );

        ensure!(
            points_rd.spatial_reference == polygons_rd.spatial_reference,
            crate::error::InvalidSpatialReference {
                expected: points_rd.spatial_reference,
                found: polygons_rd.spatial_reference,
            }
        );

        // We use the result descriptor of the points because in the worst case no feature will be excluded.
        // We cannot use the polygon bbox because a `MultiPoint` could have one point within a polygon (and
        // thus be included in the result) and one point outside of the bbox of the polygons.
        let out_desc = initialized_source.points.result_descriptor().clone();

        let initialized_operator = InitializedPointInPolygonFilter {
            name,
            path,
            result_descriptor: out_desc,
            points: initialized_source.points,
            polygons: initialized_source.polygons,
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(PointInPolygonFilter);
}

pub struct InitializedPointInPolygonFilter {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
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
            PointInPolygonFilterProcessor::new(
                self.result_descriptor.clone(),
                point_processor,
                polygon_processor,
            )
            .boxed(),
        ))
    }

    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        PointInPolygonFilter::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }

    fn boxed(self) -> Box<dyn InitializedVectorOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}
pub struct PointInPolygonFilterProcessor {
    result_descriptor: VectorResultDescriptor,
    points: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
    polygons: Box<dyn VectorQueryProcessor<VectorType = MultiPolygonCollection>>,
}

impl PointInPolygonFilterProcessor {
    pub fn new(
        result_descriptor: VectorResultDescriptor,
        points: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
        polygons: Box<dyn VectorQueryProcessor<VectorType = MultiPolygonCollection>>,
    ) -> Self {
        Self {
            result_descriptor,
            points,
            polygons,
        }
    }

    fn filter_parallel(
        points: &Arc<MultiPointCollection>,
        polygons: &MultiPolygonCollection,
        thread_pool: &ThreadPool,
    ) -> Vec<bool> {
        debug_assert!(!points.is_empty());

        // TODO: parallelize over coordinate rather than features

        let tester = Arc::new(PointInPolygonTester::new(polygons)); // TODO: multithread

        let parallelism = thread_pool.current_num_threads();
        let chunk_size = (points.len() as f64 / parallelism as f64).ceil() as usize;

        let mut result = vec![false; points.len()];

        thread_pool.scope(|scope| {
            let num_features = points.len();
            let feature_offsets = points.feature_offsets();
            let time_intervals = points.time_intervals();
            let coordinates = points.coordinates();

            for (chunk_index, chunk_result) in result.chunks_mut(chunk_size).enumerate() {
                let feature_index_start = chunk_index * chunk_size;
                let features_index_end = min(feature_index_start + chunk_size, num_features);
                let tester = tester.clone();

                scope.spawn(move |_| {
                    for (
                        feature_index,
                        ((coordinates_start_index, coordinates_end_index), time_interval),
                    ) in two_tuple_windows(
                        feature_offsets[feature_index_start..=features_index_end]
                            .iter()
                            .map(|&c| c as usize),
                    )
                    .zip(time_intervals[feature_index_start..features_index_end].iter())
                    .enumerate()
                    {
                        let is_multi_point_in_polygon_collection = coordinates
                            [coordinates_start_index..coordinates_end_index]
                            .iter()
                            .any(|coordinate| {
                                tester.any_polygon_contains_coordinate(coordinate, time_interval)
                            });

                        chunk_result[feature_index] = is_multi_point_in_polygon_collection;
                    }
                });
            }
        });

        result
    }

    async fn filter_points(
        ctx: &dyn QueryContext,
        points: Arc<MultiPointCollection>,
        polygons: MultiPolygonCollection,
        initial_filter: &BooleanArray,
    ) -> Result<BooleanArray> {
        let thread_pool = ctx.thread_pool().clone();

        let thread_points = points.clone();
        let filter = crate::util::spawn_blocking(move || {
            Self::filter_parallel(&thread_points, &polygons, &thread_pool)
        })
        .await?;

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
        let filtered_stream =
            self.points
                .query(query.clone(), ctx)
                .await?
                .and_then(move |points| {
                    let query: geoengine_datatypes::primitives::QueryRectangle<
                        geoengine_datatypes::primitives::BoundingBox2D,
                        geoengine_datatypes::primitives::ColumnSelection,
                    > = query.clone();
                    async move {
                        if points.is_empty() {
                            return Ok(points);
                        }

                        let initial_filter = BooleanArray::from(vec![false; points.len()]);
                        let arc_points = Arc::new(points);

                        let (filter, cache_hint) = self
                            .polygons
                            .query(query.clone(), ctx)
                            .await?
                            .try_fold(
                                (initial_filter, CacheHint::max_duration()),
                                |acc, polygons| {
                                    let arc_points = arc_points.clone();
                                    async move {
                                        let (filter, mut cache_hint) = acc;
                                        let polygons = polygons;

                                        cache_hint.merge_with(&polygons.cache_hint);

                                        if polygons.is_empty() {
                                            return Ok((filter, cache_hint));
                                        }

                                        Ok((
                                            Self::filter_points(
                                                ctx,
                                                arc_points.clone(),
                                                polygons,
                                                &filter,
                                            )
                                            .await?,
                                            cache_hint,
                                        ))
                                    }
                                },
                            )
                            .await?;

                        let mut new_points =
                            arc_points.filter(filter).map_err(Into::<Error>::into)?;
                        new_points.cache_hint = cache_hint;

                        Ok(new_points)
                    }
                });

        Ok(
            FeatureCollectionChunkMerger::new(filtered_stream.fuse(), ctx.chunk_byte_size().into())
                .boxed(),
        )
    }

    fn vector_result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
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
        let output = (
            last.expect("it should have a first tuple in a two-tuple window"),
            item,
        );
        last = Some(item);
        output
    })
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::str::FromStr;

    use geoengine_datatypes::collections::ChunksEqualIgnoringCacheHint;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, Coordinate2D, MultiPoint, MultiPolygon, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::primitives::{CacheHint, ColumnSelection};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;

    use crate::engine::{ChunkByteSize, MockExecutionContext, MockQueryContext};
    use crate::error::Error;
    use crate::mock::MockFeatureCollectionSource;

    #[test]
    fn point_in_polygon_boundary_conditions() {
        let collection = MultiPolygonCollection::from_data(
            vec![
                MultiPolygon::new(vec![vec![vec![
                    (0.0, 0.0).into(),
                    (10.0, 0.0).into(),
                    (10.0, 10.0).into(),
                    (0.0, 10.0).into(),
                    (0.0, 0.0).into(),
                ]]])
                .unwrap(),
            ],
            vec![Default::default(); 1],
            Default::default(),
            CacheHint::default(),
        )
        .unwrap();

        let tester = PointInPolygonTester::new(&collection);

        // the algorithm is not stable for boundary cases directly on the edges

        assert!(tester.any_polygon_contains_coordinate(
            &Coordinate2D::new(0.000_001, 0.000_001),
            &Default::default()
        ),);
        assert!(tester.any_polygon_contains_coordinate(
            &Coordinate2D::new(0.000_001, 0.1),
            &Default::default()
        ),);
        assert!(tester.any_polygon_contains_coordinate(
            &Coordinate2D::new(0.1, 0.000_001),
            &Default::default()
        ),);

        assert!(
            tester
                .any_polygon_contains_coordinate(&Coordinate2D::new(9.9, 9.9), &Default::default()),
        );
        assert!(
            tester.any_polygon_contains_coordinate(
                &Coordinate2D::new(10.0, 9.9),
                &Default::default()
            ),
        );
        assert!(
            tester.any_polygon_contains_coordinate(
                &Coordinate2D::new(9.9, 10.0),
                &Default::default()
            ),
        );

        assert!(
            !tester.any_polygon_contains_coordinate(
                &Coordinate2D::new(-0.1, -0.1),
                &Default::default()
            ),
        );
        assert!(
            !tester.any_polygon_contains_coordinate(
                &Coordinate2D::new(0.0, -0.1),
                &Default::default()
            ),
        );
        assert!(
            !tester.any_polygon_contains_coordinate(
                &Coordinate2D::new(-0.1, 0.0),
                &Default::default()
            ),
        );

        assert!(
            !tester.any_polygon_contains_coordinate(
                &Coordinate2D::new(10.1, 10.1),
                &Default::default()
            ),
        );
        assert!(
            !tester.any_polygon_contains_coordinate(
                &Coordinate2D::new(10.1, 9.9),
                &Default::default()
            ),
        );
        assert!(
            !tester.any_polygon_contains_coordinate(
                &Coordinate2D::new(9.9, 10.1),
                &Default::default()
            ),
        );
    }

    #[tokio::test]
    async fn all() -> Result<()> {
        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.001, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            Default::default(),
            CacheHint::default(),
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
                CacheHint::default(),
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
        .initialize(
            WorkflowOperatorPath::initialize_root(),
            &MockExecutionContext::test_default(),
        )
        .await?;

        let query_processor = operator.query_processor()?.multi_point().unwrap();

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
            attributes: ColumnSelection::all(),
        };
        let ctx = MockQueryContext::new(ChunkByteSize::MAX);

        let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        assert!(result[0].chunks_equal_ignoring_cache_hint(&points));

        Ok(())
    }

    #[tokio::test]
    async fn empty() -> Result<()> {
        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            Default::default(),
            CacheHint::default(),
        )?;

        let point_source = MockFeatureCollectionSource::single(points.clone()).boxed();

        let polygon_source =
            MockFeatureCollectionSource::single(MultiPolygonCollection::from_data(
                vec![],
                vec![],
                Default::default(),
                CacheHint::default(),
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
        .initialize(
            WorkflowOperatorPath::initialize_root(),
            &MockExecutionContext::test_default(),
        )
        .await?;

        let query_processor = operator.query_processor()?.multi_point().unwrap();

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
            attributes: ColumnSelection::all(),
        };
        let ctx = MockQueryContext::new(ChunkByteSize::MAX);

        let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);
        assert!(result[0].chunks_equal_ignoring_cache_hint(&MultiPointCollection::empty()));

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
            CacheHint::default(),
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
                CacheHint::default(),
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
        .initialize(
            WorkflowOperatorPath::initialize_root(),
            &MockExecutionContext::test_default(),
        )
        .await?;

        let query_processor = operator.query_processor()?.multi_point().unwrap();

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
            attributes: ColumnSelection::all(),
        };
        let ctx = MockQueryContext::new(ChunkByteSize::MAX);

        let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        assert!(
            result[0].chunks_equal_ignoring_cache_hint(&points.filter(vec![true, false, true])?)
        );

        Ok(())
    }

    // #[async::main]
    #[tokio::test]
    async fn multiple_inputs() -> Result<()> {
        let points1 = MultiPointCollection::from_data(
            MultiPoint::many(vec![(5.0, 5.1), (15.0, 15.1)]).unwrap(),
            vec![TimeInterval::new(0, 1)?; 2],
            Default::default(),
            CacheHint::default(),
        )?;
        let points2 = MultiPointCollection::from_data(
            MultiPoint::many(vec![(6.0, 6.1), (16.0, 16.1)]).unwrap(),
            vec![TimeInterval::new(1, 2)?; 2],
            Default::default(),
            CacheHint::default(),
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
                CacheHint::default(),
            )?,
            MultiPolygonCollection::from_data(
                vec![polygon1, polygon2],
                vec![TimeInterval::new(1, 2)?, TimeInterval::new(1, 2)?],
                Default::default(),
                CacheHint::default(),
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
        .initialize(
            WorkflowOperatorPath::initialize_root(),
            &MockExecutionContext::test_default(),
        )
        .await?;

        let query_processor = operator.query_processor()?.multi_point().unwrap();

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
            attributes: ColumnSelection::all(),
        };

        let ctx_one_chunk = MockQueryContext::new(ChunkByteSize::MAX);
        let ctx_minimal_chunks = MockQueryContext::new(ChunkByteSize::MIN);

        let query = query_processor
            .query(query_rectangle.clone(), &ctx_minimal_chunks)
            .await
            .unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 2);

        assert!(result[0].chunks_equal_ignoring_cache_hint(&points1.filter(vec![true, false])?));
        assert!(result[1].chunks_equal_ignoring_cache_hint(&points2));

        let query = query_processor
            .query(query_rectangle, &ctx_one_chunk)
            .await
            .unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        assert!(result[0].chunks_equal_ignoring_cache_hint(
            &points1.filter(vec![true, false])?.append(&points2)?
        ));

        Ok(())
    }

    #[tokio::test]
    async fn empty_points() {
        let point_collection = MultiPointCollection::from_data(
            vec![],
            vec![],
            Default::default(),
            CacheHint::default(),
        )
        .unwrap();

        let polygon_collection = MultiPolygonCollection::from_data(
            vec![
                MultiPolygon::new(vec![vec![vec![
                    (0.0, 0.0).into(),
                    (10.0, 0.0).into(),
                    (10.0, 10.0).into(),
                    (0.0, 10.0).into(),
                    (0.0, 0.0).into(),
                ]]])
                .unwrap(),
            ],
            vec![TimeInterval::default()],
            Default::default(),
            CacheHint::default(),
        )
        .unwrap();

        let operator = PointInPolygonFilter {
            params: PointInPolygonFilterParams {},
            sources: PointInPolygonFilterSource {
                points: MockFeatureCollectionSource::single(point_collection).boxed(),
                polygons: MockFeatureCollectionSource::single(polygon_collection).boxed(),
            },
        }
        .boxed()
        .initialize(
            WorkflowOperatorPath::initialize_root(),
            &MockExecutionContext::test_default(),
        )
        .await
        .unwrap();

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((-10., -10.).into(), (10., 10.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
            attributes: ColumnSelection::all(),
        };

        let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

        let query_context = MockQueryContext::test_default();

        let query = query_processor
            .query(query_rectangle, &query_context)
            .await
            .unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);
        assert!(result[0].chunks_equal_ignoring_cache_hint(&MultiPointCollection::empty()));
    }

    #[tokio::test]
    async fn it_checks_sref() {
        let point_collection = MultiPointCollection::from_data(
            vec![],
            vec![],
            Default::default(),
            CacheHint::default(),
        )
        .unwrap();

        let polygon_collection = MultiPolygonCollection::from_data(
            vec![
                MultiPolygon::new(vec![vec![vec![
                    (0.0, 0.0).into(),
                    (10.0, 0.0).into(),
                    (10.0, 10.0).into(),
                    (0.0, 10.0).into(),
                    (0.0, 0.0).into(),
                ]]])
                .unwrap(),
            ],
            vec![TimeInterval::default()],
            Default::default(),
            CacheHint::default(),
        )
        .unwrap();

        let operator = PointInPolygonFilter {
            params: PointInPolygonFilterParams {},
            sources: PointInPolygonFilterSource {
                points: MockFeatureCollectionSource::with_collections_and_sref(
                    vec![point_collection],
                    SpatialReference::epsg_4326(),
                )
                .boxed(),
                polygons: MockFeatureCollectionSource::with_collections_and_sref(
                    vec![polygon_collection],
                    SpatialReference::from_str("EPSG:3857").unwrap(),
                )
                .boxed(),
            },
        }
        .boxed()
        .initialize(
            WorkflowOperatorPath::initialize_root(),
            &MockExecutionContext::test_default(),
        )
        .await;

        assert!(matches!(
            operator,
            Err(Error::InvalidSpatialReference {
                expected: _,
                found: _,
            })
        ));
    }
}
