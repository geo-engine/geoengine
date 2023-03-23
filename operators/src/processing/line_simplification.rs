use crate::{
    engine::{
        ExecutionContext, InitializedVectorOperator, Operator, OperatorName, QueryContext,
        QueryProcessor, SingleVectorSource, TypedVectorQueryProcessor, VectorOperator,
        VectorQueryProcessor, VectorResultDescriptor,
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
        BoundingBox2D, Geometry, MultiLineString, MultiLineStringRef, MultiPolygon,
        MultiPolygonRef, SpatialResolution, VectorQueryRectangle,
    },
    util::arrow::ArrowTyped,
};
use rayon::prelude::{ParallelBridge, ParallelIterator};
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
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        if self
            .params
            .epsilon
            .map_or(false, |e| !e.is_finite() || e <= 0.0)
        {
            return Err(LineSimplificationError::InvalidEpsilon.into());
        }

        let source = self.sources.vector.initialize(context).await?;

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
    pub algorithm: LineSimplificationAlgorithms,
    /// The epsilon parameter is used to determine the maximum distance between the original and the simplified geometry.
    /// If `None` is provided, the epsilon is derived by the query's [`SpatialResolution`].
    pub epsilon: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub enum LineSimplificationAlgorithms {
    DouglasPeucker,
    Visvalingam,
}

pub struct InitializedLineSimplification {
    result_descriptor: VectorResultDescriptor,
    source: Box<dyn InitializedVectorOperator>,
    algorithm: LineSimplificationAlgorithms,
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
                LineSimplificationAlgorithms::DouglasPeucker,
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
                LineSimplificationAlgorithms::Visvalingam,
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
                LineSimplificationAlgorithms::DouglasPeucker,
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
                LineSimplificationAlgorithms::Visvalingam,
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
    for<'c> A: LineSimplificationAlgorithm<
        <FeatureCollection<G> as IntoGeometryIterator<'c>>::GeometryType,
        G,
    >,
{
    source: P,
    _algorithm: A,
    epsilon: Option<f64>,
}

pub trait LineSimplificationAlgorithm<In, Out: Geometry>: Send + Sync {
    fn simplify(geometry_ref: In, epsilon: f64) -> Out;

    fn derive_epsilon(spatial_resolution: SpatialResolution) -> f64 {
        f64::max(spatial_resolution.x, spatial_resolution.y)
    }
}

struct DouglasPeucker;
struct Visvalingam;

impl<'c> LineSimplificationAlgorithm<MultiLineStringRef<'c>, MultiLineString> for DouglasPeucker {
    fn simplify(geometry: MultiLineStringRef<'c>, epsilon: f64) -> MultiLineString {
        use geo::Simplify;

        let geo_geometry = geo::MultiLineString::<f64>::from(&geometry);
        let geo_geometry = geo_geometry.simplify(&epsilon);
        geo_geometry.into()
    }
}

impl<'c> LineSimplificationAlgorithm<MultiPolygonRef<'c>, MultiPolygon> for DouglasPeucker {
    fn simplify(geometry: MultiPolygonRef<'c>, epsilon: f64) -> MultiPolygon {
        use geo::Simplify;

        let geo_geometry = geo::MultiPolygon::<f64>::from(&geometry);
        let geo_geometry = geo_geometry.simplify(&epsilon);
        geo_geometry.into()
    }
}

impl<'c> LineSimplificationAlgorithm<MultiLineStringRef<'c>, MultiLineString> for Visvalingam {
    fn simplify(geometry: MultiLineStringRef<'c>, epsilon: f64) -> MultiLineString {
        use geo::SimplifyVWPreserve;

        let geo_geometry = geo::MultiLineString::<f64>::from(&geometry);
        let geo_geometry = geo_geometry.simplifyvw_preserve(&epsilon);
        geo_geometry.into()
    }

    fn derive_epsilon(spatial_resolution: SpatialResolution) -> f64 {
        let epsilon = f64::max(spatial_resolution.x, spatial_resolution.y);

        // for visvalingam, the epsilon is squared since it reflects some triangle area
        // this is a heuristic, though
        epsilon * epsilon
    }
}

impl<'c> LineSimplificationAlgorithm<MultiPolygonRef<'c>, MultiPolygon> for Visvalingam {
    fn simplify(geometry: MultiPolygonRef<'c>, epsilon: f64) -> MultiPolygon {
        use geo::SimplifyVWPreserve;

        let geo_geometry = geo::MultiPolygon::<f64>::from(&geometry);
        let geo_geometry = geo_geometry.simplifyvw_preserve(&epsilon);
        geo_geometry.into()
    }

    fn derive_epsilon(spatial_resolution: SpatialResolution) -> f64 {
        let epsilon = f64::max(spatial_resolution.x, spatial_resolution.y);

        // for visvalingam, the epsilon is squared since it reflects some triangle area
        // this is a heuristic, though
        epsilon * epsilon
    }
}

impl<P, G, A> LineSimplificationProcessor<P, G, A>
where
    P: VectorQueryProcessor<VectorType = FeatureCollection<G>>,
    G: Geometry,
    for<'c> FeatureCollection<G>: IntoGeometryIterator<'c>
        + GeoFeatureCollectionModifications<G, Output = FeatureCollection<G>>,
    for<'c> A: LineSimplificationAlgorithm<
        <FeatureCollection<G> as IntoGeometryIterator<'c>>::GeometryType,
        G,
    >,
{
    fn simplify(collection: &FeatureCollection<G>, epsilon: f64) -> Result<FeatureCollection<G>> {
        // TODO: chunk within parallelization to reduce overhead if necessary

        let simplified_geometries = collection
            .geometries()
            .par_bridge()
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
    P: QueryProcessor<Output = FeatureCollection<G>, SpatialBounds = BoundingBox2D>,
    G: Geometry + ArrowTyped + 'static,
    for<'c> FeatureCollection<G>: IntoGeometryIterator<'c>
        + GeoFeatureCollectionModifications<G, Output = FeatureCollection<G>>,
    for<'c> A: LineSimplificationAlgorithm<
        <FeatureCollection<G> as IntoGeometryIterator<'c>>::GeometryType,
        G,
    >,
{
    type Output = FeatureCollection<G>;
    type SpatialBounds = BoundingBox2D;

    async fn _query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let chunks = self.source.query(query, ctx).await?;

        let epsilon = self
            .epsilon
            .unwrap_or_else(|| A::derive_epsilon(query.spatial_resolution));

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

    #[tokio::test]
    async fn test_ser_de() {
        todo!()
    }

    #[tokio::test]
    async fn test_errors() {
        todo!()
    }

    #[tokio::test]
    async fn test_line_simplification() {
        todo!()
    }

    #[tokio::test]
    async fn test_polygon_simplification() {
        todo!()
    }
}
