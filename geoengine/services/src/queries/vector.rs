//! Vector queries, encoded as `GeoJSON`.

use crate::{
    contexts::SessionContext,
    error::{self, Result},
    processing_graphs::{
        processing_graph::{ProcessingGraph, ProcessingGraphId},
        registry::ProcessingGraphRegistry,
    },
    quota::ComputationId,
};
use futures_util::TryStreamExt;
use geoengine_datatypes::{
    collections::{FeatureCollection, ToGeoJson},
    primitives::{
        BoundingBox2D, CacheHint, ColumnSelection, Geometry, TimeInterval, VectorQueryRectangle,
    },
    spatial_reference::SpatialReference,
    util::Identifier,
};
use geoengine_operators::{
    engine::{
        ProcessingGraphOperatorPath, QueryContext, QueryProcessorExt, ResultDescriptor,
        SingleRasterOrVectorSource, TypedVectorQueryProcessor, VectorOperator,
        VectorQueryProcessor,
    },
    processing::{DeriveOutRasterSpecsSource, Reprojection, ReprojectionParams},
    util::input::RasterOrVectorOperator,
};
use serde_json::json;

/// A vector processing graph, reprojected to `crs` if needed
pub struct VectorQuery {
    pub processing_graph: ProcessingGraphId,
    pub crs: SpatialReference,
    pub bounds: BoundingBox2D,
    pub time: TimeInterval,
}

/// Queries the features of a vector processing graph as a `GeoJSON` feature collection
pub async fn features_geojson<C: SessionContext>(
    ctx: C,
    query: VectorQuery,
) -> Result<(serde_json::Value, CacheHint, ComputationId)> {
    let VectorQuery {
        processing_graph: processing_graph_id,
        crs,
        bounds,
        time,
    } = query;

    let processing_graph: ProcessingGraph =
        ctx.db().load_processing_graph(&processing_graph_id).await?;

    let operator = processing_graph.operator.get_vector()?;

    let execution_context = ctx.execution_context()?;
    let processing_graph_operator_path_root = ProcessingGraphOperatorPath::initialize_root();

    let initialized = operator
        .clone()
        .initialize(processing_graph_operator_path_root, &execution_context)
        .await?;

    // handle request and processing graph crs matching
    let processing_graph_spatial_ref: Option<SpatialReference> =
        initialized.result_descriptor().spatial_reference().into();
    let processing_graph_spatial_ref =
        processing_graph_spatial_ref.ok_or(error::Error::InvalidSpatialReference)?;

    let request_spatial_ref = crs;

    // perform reprojection if necessary
    let initialized = if request_spatial_ref == processing_graph_spatial_ref {
        initialized
    } else {
        tracing::debug!(
            "vector query srs: {request_spatial_ref}, processing graph srs: {processing_graph_spatial_ref} --> injecting reprojection"
        );

        let reprojection_params = ReprojectionParams {
            target_spatial_reference: request_spatial_ref,
            derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
        };

        // create the reprojection operator in order to get the canonic operator name
        let reprojected_processing_graph = Reprojection {
            params: reprojection_params,
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Vector(operator),
            },
        }
        .typed();

        let processing_graph_operator_path_root = ProcessingGraphOperatorPath::initialize_root();

        // TODO: avoid re-initialization and re-use unprojected processing graph.
        // In order to check whether we need to inject a reprojection, we first need to initialize the
        // original processing graph. Then we can check the result projection. Previously, we then just wrapped
        // the initialized processing graph with an initialized reprojection. IMHO this is wrong because
        // initialization propagates the processing graph path down the children and appends a new segment for
        // each level. So we can't re-use an already initialized processing graph, because all the processing graph path/
        // operator names will be wrong. That's why I now build a new processing graph with a reprojection and
        // perform a full initialization. I only added the TODO because we did some optimization here
        // which broke at some point when the processing graph operator paths were introduced but no one noticed.
        let ivp = reprojected_processing_graph
            .initialize(processing_graph_operator_path_root, &execution_context)
            .await?;

        Box::new(ivp)
    };

    let processor = initialized.query_processor()?;

    let query_rect = VectorQueryRectangle::new(bounds, time, ColumnSelection::all());
    let computation_id = ComputationId::new();
    let query_ctx = ctx.query_context(processing_graph_id, computation_id)?;

    let (json, cache_hint) = match processor {
        TypedVectorQueryProcessor::Data(p) => {
            vector_stream_to_geojson(p, query_rect, query_ctx).await
        }
        TypedVectorQueryProcessor::MultiPoint(p) => {
            vector_stream_to_geojson(p, query_rect, query_ctx).await
        }
        TypedVectorQueryProcessor::MultiLineString(p) => {
            vector_stream_to_geojson(p, query_rect, query_ctx).await
        }
        TypedVectorQueryProcessor::MultiPolygon(p) => {
            vector_stream_to_geojson(p, query_rect, query_ctx).await
        }
    }?;

    Ok((json, cache_hint, computation_id))
}

async fn vector_stream_to_geojson<G, C: QueryContext + 'static>(
    processor: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
    query_rect: VectorQueryRectangle,
    query_ctx: C,
) -> Result<(serde_json::Value, CacheHint)>
where
    G: Geometry + 'static,
    for<'c> FeatureCollection<G>: ToGeoJson<'c>,
{
    let default_cache_ttl = query_ctx.default_cache_ttl();

    let features: Vec<serde_json::Value> = Vec::new();
    // TODO: more efficient merging of the partial feature collections
    let stream = processor
        .query_into_owned_boxed_stream(query_rect, Box::new(query_ctx))
        .await?;

    let (features, cache_hint) = stream
        .try_fold(
            (features, None::<CacheHint>),
            |(mut output, mut cache_hint), collection| async move {
                // TODO: avoid parsing the generated json
                let mut json: serde_json::Value =
                    serde_json::from_str(&collection.to_geo_json()).expect("to_geojson is correct");
                let more_features = json
                    .get_mut("features")
                    .expect("to_geojson is correct")
                    .as_array_mut()
                    .expect("to geojson is correct");

                output.append(more_features);

                cache_hint
                    .get_or_insert_with(CacheHint::max_duration)
                    .merge_with(&collection.cache_hint);

                Ok((output, cache_hint))
            },
        )
        .await?;
    let cache_hint = cache_hint.unwrap_or_else(|| default_cache_ttl.into());

    let mut output = json!({
        "type": "FeatureCollection"
    });

    output
        .as_object_mut()
        .expect("as defined")
        .insert("features".into(), serde_json::Value::Array(features));

    Ok((output, cache_hint))
}
