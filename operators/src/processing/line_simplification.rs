use crate::{
    engine::{
        ExecutionContext, InitializedSources, InitializedVectorOperator, Operator, OperatorName,
        QueryContext, QueryProcessor, SingleVectorSource, TypedVectorQueryProcessor,
        VectorOperator, VectorQueryProcessor, VectorResultDescriptor, WorkflowOperatorPath,
    },
    util::Result,
};
use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use geoengine_datatypes::{
    collections::{
        FeatureCollection, GeoFeatureCollectionModifications, IntoGeometryIterator, VectorDataType,
    },
    error::{BoxedResultExt, ErrorSource},
    primitives::{
        Geometry, MultiLineString, MultiLineStringRef, MultiPolygon, MultiPolygonRef, SpatialQuery,
        SpatialResolution, VectorQueryRectangle, VectorSpatialQueryRectangle,
    },
    util::arrow::ArrowTyped,
};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

/// A `LineSimplification` operator simplifies the geometry of a `Vector` by removing vertices.
///
/// The simplification is performed on the geometry of the `Vector` and not on the data.
/// The `LineSimplification` operator is only available for (multi-)lines or (multi-)polygons.
///
pub type LineSimplification = Operator<LineSimplificationParams, SingleVectorSource>;

impl OperatorName for LineSimplification {
    const TYPE_NAME: &'static str = "LineSimplification";
}

#[typetag::serde]
#[async_trait]
impl VectorOperator for LineSimplification {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        if self
            .params
            .epsilon
            .map_or(false, |e| !e.is_finite() || e <= 0.0)
        {
            return Err(LineSimplificationError::InvalidEpsilon.into());
        }

        let sources = self.sources.initialize_sources(path, context).await?;
        let source = sources.vector;

        if source.result_descriptor().data_type != VectorDataType::MultiLineString
            && source.result_descriptor().data_type != VectorDataType::MultiPolygon
        {
            return Err(LineSimplificationError::InvalidGeometryType.into());
        }

        let initialized_operator = InitializedLineSimplification {
            result_descriptor: source.result_descriptor().clone(),
            source,
            algorithm: self.params.algorithm,
            epsilon: self.params.epsilon,
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(LineSimplification);
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub struct LineSimplificationParams {
    pub algorithm: LineSimplificationAlgorithm,
    /// The epsilon parameter is used to determine the maximum distance between the original and the simplified geometry.
    /// If `None` is provided, the epsilon is derived by the query's [`SpatialResolution`].
    pub epsilon: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub enum LineSimplificationAlgorithm {
    DouglasPeucker,
    Visvalingam,
}

pub struct InitializedLineSimplification {
    result_descriptor: VectorResultDescriptor,
    source: Box<dyn InitializedVectorOperator>,
    algorithm: LineSimplificationAlgorithm,
    epsilon: Option<f64>,
}

impl InitializedVectorOperator for InitializedLineSimplification {
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        match (self.source.query_processor()?, self.algorithm) {
            (
                TypedVectorQueryProcessor::Data(..) | TypedVectorQueryProcessor::MultiPoint(..),
                _,
            ) => Err(LineSimplificationError::InvalidGeometryType.into()),
            (
                TypedVectorQueryProcessor::MultiLineString(source),
                LineSimplificationAlgorithm::DouglasPeucker,
            ) => Ok(TypedVectorQueryProcessor::MultiLineString(
                LineSimplificationProcessor {
                    source,
                    _algorithm: DouglasPeucker,
                    epsilon: self.epsilon,
                }
                .boxed(),
            )),
            (
                TypedVectorQueryProcessor::MultiLineString(source),
                LineSimplificationAlgorithm::Visvalingam,
            ) => Ok(TypedVectorQueryProcessor::MultiLineString(
                LineSimplificationProcessor {
                    source,
                    _algorithm: Visvalingam,
                    epsilon: self.epsilon,
                }
                .boxed(),
            )),
            (
                TypedVectorQueryProcessor::MultiPolygon(source),
                LineSimplificationAlgorithm::DouglasPeucker,
            ) => Ok(TypedVectorQueryProcessor::MultiPolygon(
                LineSimplificationProcessor {
                    source,
                    _algorithm: DouglasPeucker,
                    epsilon: self.epsilon,
                }
                .boxed(),
            )),
            (
                TypedVectorQueryProcessor::MultiPolygon(source),
                LineSimplificationAlgorithm::Visvalingam,
            ) => Ok(TypedVectorQueryProcessor::MultiPolygon(
                LineSimplificationProcessor {
                    source,
                    _algorithm: Visvalingam,
                    epsilon: self.epsilon,
                }
                .boxed(),
            )),
        }
    }
}

struct LineSimplificationProcessor<P, G, A>
where
    P: VectorQueryProcessor<VectorType = FeatureCollection<G>>,
    G: Geometry,
    for<'c> FeatureCollection<G>: IntoGeometryIterator<'c>,
    for<'c> A: LineSimplificationAlgorithmImpl<
        <FeatureCollection<G> as IntoGeometryIterator<'c>>::GeometryType,
        G,
    >,
{
    source: P,
    _algorithm: A,
    epsilon: Option<f64>,
}

pub trait LineSimplificationAlgorithmImpl<In, Out: Geometry>: Send + Sync {
    fn simplify(geometry_ref: In, epsilon: f64) -> Out;

    fn derive_epsilon(spatial_resolution: SpatialResolution) -> f64 {
        f64::sqrt(spatial_resolution.x.powi(2) + spatial_resolution.y.powi(2)) / f64::sqrt(2.)
    }
}

struct DouglasPeucker;
struct Visvalingam;

impl<'c> LineSimplificationAlgorithmImpl<MultiLineStringRef<'c>, MultiLineString>
    for DouglasPeucker
{
    fn simplify(geometry: MultiLineStringRef<'c>, epsilon: f64) -> MultiLineString {
        use geo::Simplify;

        let geo_geometry = geo::MultiLineString::<f64>::from(&geometry);
        let geo_geometry = geo_geometry.simplify(&epsilon);
        geo_geometry.into()
    }
}

impl<'c> LineSimplificationAlgorithmImpl<MultiPolygonRef<'c>, MultiPolygon> for DouglasPeucker {
    fn simplify(geometry: MultiPolygonRef<'c>, epsilon: f64) -> MultiPolygon {
        use geo::Simplify;

        let geo_geometry = geo::MultiPolygon::<f64>::from(&geometry);
        let geo_geometry = geo_geometry.simplify(&epsilon);
        geo_geometry.into()
    }
}

impl<'c> LineSimplificationAlgorithmImpl<MultiLineStringRef<'c>, MultiLineString> for Visvalingam {
    fn simplify(geometry: MultiLineStringRef<'c>, epsilon: f64) -> MultiLineString {
        use geo::SimplifyVWPreserve;

        let geo_geometry = geo::MultiLineString::<f64>::from(&geometry);
        let geo_geometry = geo_geometry.simplifyvw_preserve(&epsilon);
        geo_geometry.into()
    }

    fn derive_epsilon(spatial_resolution: SpatialResolution) -> f64 {
        // for visvalingam, the epsilon is squared since it reflects some triangle area
        // this is a heuristic, though
        spatial_resolution.x * spatial_resolution.y
    }
}

impl<'c> LineSimplificationAlgorithmImpl<MultiPolygonRef<'c>, MultiPolygon> for Visvalingam {
    fn simplify(geometry: MultiPolygonRef<'c>, epsilon: f64) -> MultiPolygon {
        use geo::SimplifyVWPreserve;

        let geo_geometry = geo::MultiPolygon::<f64>::from(&geometry);
        let geo_geometry = geo_geometry.simplifyvw_preserve(&epsilon);
        geo_geometry.into()
    }

    fn derive_epsilon(spatial_resolution: SpatialResolution) -> f64 {
        // for visvalingam, the epsilon is squared since it reflects some triangle area
        // this is a heuristic, though
        spatial_resolution.x * spatial_resolution.y
    }
}

impl<P, G, A> LineSimplificationProcessor<P, G, A>
where
    P: VectorQueryProcessor<VectorType = FeatureCollection<G>>,
    G: Geometry,
    for<'c> FeatureCollection<G>: IntoGeometryIterator<'c>
        + GeoFeatureCollectionModifications<G, Output = FeatureCollection<G>>,
    for<'c> A: LineSimplificationAlgorithmImpl<
        <FeatureCollection<G> as IntoGeometryIterator<'c>>::GeometryType,
        G,
    >,
{
    fn simplify(collection: &FeatureCollection<G>, epsilon: f64) -> Result<FeatureCollection<G>> {
        // TODO: chunk within parallelization to reduce overhead if necessary

        let simplified_geometries = collection
            .geometries()
            .into_par_iter()
            .map(|geometry| A::simplify(geometry, epsilon))
            .collect();

        Ok(collection
            .replace_geometries(simplified_geometries)
            .boxed_context(error::ErrorDuringSimplification)?)
    }
}

#[async_trait]
impl<P, G, A> QueryProcessor for LineSimplificationProcessor<P, G, A>
where
    P: QueryProcessor<Output = FeatureCollection<G>, SpatialQuery = VectorSpatialQueryRectangle>,
    G: Geometry + ArrowTyped + 'static,
    for<'c> FeatureCollection<G>: IntoGeometryIterator<'c>
        + GeoFeatureCollectionModifications<G, Output = FeatureCollection<G>>,
    for<'c> A: LineSimplificationAlgorithmImpl<
        <FeatureCollection<G> as IntoGeometryIterator<'c>>::GeometryType,
        G,
    >,
{
    type Output = FeatureCollection<G>;
    type SpatialQuery = VectorSpatialQueryRectangle;

    async fn _query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let chunks = self.source.query(query, ctx).await?;

        let epsilon = self
            .epsilon
            .unwrap_or_else(|| A::derive_epsilon(query.spatial_query().spatial_resolution));

        let simplified_chunks = chunks.and_then(move |chunk| async move {
            crate::util::spawn_blocking_with_thread_pool(ctx.thread_pool().clone(), move || {
                Self::simplify(&chunk, epsilon)
            })
            .await
            .boxed_context(error::UnexpectedInternal)?
        });

        Ok(simplified_chunks.boxed())
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(error))]
pub enum LineSimplificationError {
    #[snafu(display("`epsilon` parameter must be greater than 0"))]
    InvalidEpsilon,
    #[snafu(display("Geometry must be of type `MultiLineString` or `MultiPolygon`"))]
    InvalidGeometryType,
    #[snafu(display("Error during simplification of geometry: {}", source))]
    ErrorDuringSimplification { source: Box<dyn ErrorSource> },
    #[snafu(display("Unexpected internal error: {}", source))]
    UnexpectedInternal { source: Box<dyn ErrorSource> },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::{MockExecutionContext, MockQueryContext, StaticMetaData},
        mock::MockFeatureCollectionSource,
        source::{
            OgrSource, OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType,
            OgrSourceErrorSpec, OgrSourceParameters,
        },
    };
    use geoengine_datatypes::{
        collections::{
            FeatureCollectionInfos, GeometryCollection, MultiLineStringCollection,
            MultiPointCollection, MultiPolygonCollection,
        },
        dataset::{DataId, DatasetId},
        primitives::{BoundingBox2D, FeatureData, MultiLineString, MultiPoint, TimeInterval},
        spatial_reference::SpatialReference,
        test_data,
        util::{test::TestDefault, Identifier},
    };

    #[tokio::test]
    async fn test_ser_de() {
        let operator = LineSimplification {
            params: LineSimplificationParams {
                epsilon: Some(1.0),
                algorithm: LineSimplificationAlgorithm::DouglasPeucker,
            },
            sources: MockFeatureCollectionSource::<MultiPolygon>::multiple(vec![])
                .boxed()
                .into(),
        }
        .boxed();

        let serialized = serde_json::to_value(&operator).unwrap();

        assert_eq!(
            serialized,
            serde_json::json!({
                "type": "LineSimplification",
                "params": {
                    "epsilon": 1.0,
                    "algorithm": "douglasPeucker",
                },
                "sources": {
                    "vector": {
                        "type": "MockFeatureCollectionSourceMultiPolygon",
                        "params": {
                            "collections": [],
                            "spatialReference": "EPSG:4326",
                            "measurements": null,
                        }
                    }
                },
            })
        );

        let _operator: Box<dyn VectorOperator> = serde_json::from_value(serialized).unwrap();
    }

    #[tokio::test]
    async fn test_errors() {
        // zero epsilon
        assert!(LineSimplification {
            params: LineSimplificationParams {
                epsilon: Some(0.0),
                algorithm: LineSimplificationAlgorithm::DouglasPeucker,
            },
            sources: MockFeatureCollectionSource::<MultiPolygon>::single(
                MultiPolygonCollection::empty()
            )
            .boxed()
            .into(),
        }
        .boxed()
        .initialize(
            WorkflowOperatorPath::initialize_root(),
            &MockExecutionContext::test_default()
        )
        .await
        .is_err());

        // invalid epsilon
        assert!(LineSimplification {
            params: LineSimplificationParams {
                epsilon: Some(f64::NAN),
                algorithm: LineSimplificationAlgorithm::Visvalingam,
            },
            sources: MockFeatureCollectionSource::<MultiPolygon>::single(
                MultiPolygonCollection::empty()
            )
            .boxed()
            .into(),
        }
        .boxed()
        .initialize(
            WorkflowOperatorPath::initialize_root(),
            &MockExecutionContext::test_default()
        )
        .await
        .is_err());

        // not lines or polygons
        assert!(LineSimplification {
            params: LineSimplificationParams {
                epsilon: None,
                algorithm: LineSimplificationAlgorithm::DouglasPeucker,
            },
            sources: MockFeatureCollectionSource::<MultiPoint>::single(
                MultiPointCollection::empty()
            )
            .boxed()
            .into(),
        }
        .boxed()
        .initialize(
            WorkflowOperatorPath::initialize_root(),
            &MockExecutionContext::test_default()
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn test_line_simplification() {
        let collection = MultiLineStringCollection::from_data(
            vec![
                MultiLineString::new(vec![vec![
                    (0.0, 0.0).into(),
                    (5.0, 4.0).into(),
                    (11.0, 5.5).into(),
                    (17.3, 3.2).into(),
                    (27.8, 0.1).into(),
                ]])
                .unwrap(),
                MultiLineString::new(vec![vec![(0.0, 0.0).into(), (5.0, 4.0).into()]]).unwrap(),
            ],
            vec![TimeInterval::new(0, 1).unwrap(); 2],
            [("foo".to_string(), FeatureData::Float(vec![0., 1.]))]
                .iter()
                .cloned()
                .collect(),
        )
        .unwrap();

        let source = MockFeatureCollectionSource::single(collection.clone()).boxed();

        let simplification = LineSimplification {
            params: LineSimplificationParams {
                epsilon: Some(1.0),
                algorithm: LineSimplificationAlgorithm::DouglasPeucker,
            },
            sources: source.into(),
        }
        .boxed();

        let initialized = simplification
            .initialize(
                WorkflowOperatorPath::initialize_root(),
                &MockExecutionContext::test_default(),
            )
            .await
            .unwrap();

        let processor = initialized
            .query_processor()
            .unwrap()
            .multi_line_string()
            .unwrap();

        let query_rectangle = VectorQueryRectangle::with_bounds_and_resolution(
            BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            TimeInterval::default(),
            SpatialResolution::one(),
        );

        let query_ctx = MockQueryContext::test_default();

        let stream = processor.query(query_rectangle, &query_ctx).await.unwrap();

        let collections: Vec<MultiLineStringCollection> =
            stream.map(Result::unwrap).collect().await;

        assert_eq!(collections.len(), 1);

        let expected = MultiLineStringCollection::from_data(
            vec![
                MultiLineString::new(vec![vec![
                    (0.0, 0.0).into(),
                    (5.0, 4.0).into(),
                    (11.0, 5.5).into(),
                    (27.8, 0.1).into(),
                ]])
                .unwrap(),
                MultiLineString::new(vec![vec![(0.0, 0.0).into(), (5.0, 4.0).into()]]).unwrap(),
            ],
            vec![TimeInterval::new(0, 1).unwrap(); 2],
            [("foo".to_string(), FeatureData::Float(vec![0., 1.]))]
                .iter()
                .cloned()
                .collect(),
        )
        .unwrap();

        assert_eq!(collections[0], expected);
    }

    #[tokio::test]
    async fn test_polygon_simplification() {
        let id: DataId = DatasetId::new().into();
        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            id.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/germany_polygon.gpkg").into(),
                    layer_name: "test_germany".to_owned(),
                    data_type: Some(VectorDataType::MultiPolygon),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        int: vec![],
                        float: vec![],
                        text: vec![],
                        bool: vec![],
                        datetime: vec![],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Abort,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPolygon,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: Default::default(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            }),
        );

        let simplification = LineSimplification {
            params: LineSimplificationParams {
                epsilon: None,
                algorithm: LineSimplificationAlgorithm::Visvalingam,
            },
            sources: OgrSource {
                params: OgrSourceParameters {
                    data: id,
                    attribute_projection: None,
                    attribute_filters: None,
                },
            }
            .boxed()
            .into(),
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap();

        assert_eq!(
            simplification.result_descriptor().data_type,
            VectorDataType::MultiPolygon
        );

        let query_processor = simplification
            .query_processor()
            .unwrap()
            .multi_polygon()
            .unwrap();

        let query_bbox = BoundingBox2D::new((-180.0, -90.0).into(), (180.00, 90.0).into()).unwrap();

        let query_context = MockQueryContext::test_default();
        let query = query_processor
            .query(
                VectorQueryRectangle::with_bounds_and_resolution(
                    query_bbox,
                    Default::default(),
                    SpatialResolution::new(1., 1.).unwrap(),
                ),
                &query_context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPolygonCollection> = query.try_collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let result = result.into_iter().next().unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result.feature_offsets().len(), 2);
        assert_eq!(result.polygon_offsets().len(), 23);
        assert_eq!(result.ring_offsets().len(), 23);
        assert_eq!(result.coordinates().len(), 98 /* was 3027 */);
    }
}
