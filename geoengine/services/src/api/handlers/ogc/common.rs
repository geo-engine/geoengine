use crate::{
    api::{
        handlers::{
            ogc::{
                OgcApiResult,
                error::{self, OgcApiError},
                util::{
                    LinkCreator, crs_from_spatial_reference_option,
                    get_initialized_raster_operator, link_creator, load_layer, parse_bbox_option,
                    parse_datetime_option, raster_workflow_metadata, to_ogc_bbox,
                },
            },
            workflows::{ProvenanceEntry, workflow_provenance},
        },
        model::datatypes::{DataProviderId, LayerId},
    },
    contexts::{ApplicationContext, SessionContext},
    layers::layer::Layer,
    workflows::registry::WorkflowRegistry,
};
use actix_web::{HttpResponse, web};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt, stream::BoxStream};
use geoengine_datatypes::{
    error::BoxedResultExt,
    primitives::{TimeInstance, TimeInterval},
};
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{RasterQueryProcessor, RasterResultDescriptor, TypedRasterQueryProcessor},
};
use itertools::Itertools;
use ogcapi_types::common::{
    Bbox as OgcBbox, Collection, Collections, Conformance, Crs, Datetime as OgcDatetime, Extent,
    LandingPage, SpatialExtent, TemporalExtent,
    link_rel::{CONFORMANCE, DATA, SELF, TILING_SCHEMES},
    media_type::JSON,
};
use std::collections::HashSet;
use tracing::warn;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

// TODO: add to [`ogcapi_types::common::link_rel`] and use from there
const MAP_TILESETS_REL: &str = "http://www.opengis.net/def/rel/ogc/1.0/tilesets-map";

const MAX_NUMBER_OF_TIME_INTERVALS: usize = 256;

// TODO: add to [`ogcapi_types::common`] and use from there
#[derive(Debug, Clone, Copy)]
enum CollectionItemType {
    Tile,
}

impl std::fmt::Display for CollectionItemType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            CollectionItemType::Tile => "tile",
        };
        write!(f, "{value}")
    }
}

/// OGC API Landing Page
///
/// Cf. [OGC API - Common - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).
#[utoipa::path(
    tag = "OGC API",
    get,
    path = "/{dataConnectorId}/{layerId}/",
    responses(
        (status = 200, description = "OK", body = LandingPage,
            example = json!({
                "title": "Geo Engine OGC API",
                "description": "Processing System für Geospatial Data",
                "links": [
                    {
                        "href": "http://geoengine.example.com/",
                        "rel": SELF,
                        "type": JSON
                    },
                    {
                        "href": "http://geoengine.example.com/conformance",
                        "rel": CONFORMANCE,
                        "type": JSON
                    },
                    {
                        "href": "http://geoengine.example.com/collections",
                        "rel": DATA,
                        "type": JSON
                    },
                    {
                        "href": "http://geoengine.example.com/tileMatrixSets",
                        "rel": TILING_SCHEMES,
                        "type": JSON
                    }
                ]
            })
        )
    ),
    params(
        ("dataConnectorId" = DataProviderId, description = "ID of the data connector"),
        ("layerId" = LayerId, description = "ID of the layer, which is used as collection ID"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn landing_page<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId)>,
) -> OgcApiResult<web::Json<LandingPage>> {
    let (data_connector_id, layer_id) = path.into_inner();

    ensure_layer_exists::<C>(session, app_ctx, data_connector_id, layer_id.clone()).await?;

    let create_link = link_creator(data_connector_id, layer_id.clone());

    Ok(web::Json(LandingPage {
        title: Some(format!(
            "Geo Engine OGC API [{data_connector_id}/{layer_id}]"
        )),
        description: Some(format!(
            "OGC API Landing Page for Geo Engine and layer {layer_id} of data connector {data_connector_id}"
        )),
        links: vec![
            create_link("", SELF, JSON)?,
            create_link("conformance", CONFORMANCE, JSON)?,
            create_link("collections", DATA, JSON)?,
            create_link("tileMatrixSets", TILING_SCHEMES, JSON)?,
        ],
        ..LandingPage::default()
    }))
}

/// Response to a `HEAD` request to the OGC API Landing Page endpoint.
pub async fn landing_page_head<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId)>,
) -> OgcApiResult<HttpResponse> {
    let (data_connector_id, layer_id) = path.into_inner();
    ensure_layer_exists::<C>(session, app_ctx, data_connector_id, layer_id).await?;

    Ok(HttpResponse::Ok().content_type(JSON).finish()) // return 200 OK with no body
}

/// OGC API Conformance Classes
///
/// Cf. [OGC API - Common - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).
#[utoipa::path(
    tag = "OGC API",
    get,
    path = "/{dataConnectorId}/{layerId}/conformance",
    responses(
        (status = 200, description = "OK", body = Conformance,
            example = json!({
                "conformsTo": [
                    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
                    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/json",
                    "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/collections",
                    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/core",
                    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tileset",
                    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tilesets-list",
                    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/core",
                    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tileset",
                    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tilesets-list",
                    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/png",
                    "http://www.opengis.net/spec/tms/2.0/conf/tilematrixset",
                    "http://www.opengis.net/spec/tms/2.0/conf/json-tilematrixset",
                ]
            })
        )
    ),
    params(
        ("dataConnectorId" = DataProviderId, description = "ID of the data connector"),
        ("layerId" = LayerId, description = "ID of the layer, which is used as collection ID"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn conformance<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId)>,
) -> OgcApiResult<web::Json<Conformance>> {
    let (data_connector_id, layer_id) = path.into_inner();
    ensure_layer_exists::<C>(session, app_ctx, data_connector_id, layer_id).await?;

    Ok(web::Json(Conformance::new(&[
        "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
        "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/landing-page",
        "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/oas30",
        "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/json",
        "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/collections",
        "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/core",
        "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tileset",
        "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tilesets-list",
        "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/png",
        "http://www.opengis.net/spec/tms/2.0/conf/tilematrixset",
        "http://www.opengis.net/spec/tms/2.0/conf/json-tilematrixset",
    ])))
}

#[derive(Debug, serde::Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct CollectionsQueryParams {
    /// Either a date-time or an interval, half-bounded or bounded. Date and time expressions adhere to RFC 3339. Half-bounded intervals use double dots (`..`).
    #[param(
        value_type = String,
        example = "2018-02-12T23:20:50Z"
    )]
    #[serde(default)]
    #[serde(deserialize_with = "parse_datetime_option")]
    #[allow(unused, reason = "TODO: incorporate if it makes sense")]
    pub datetime: Option<OgcDatetime>,

    /// Only features with geometries intersecting the bounding box are selected. Provide four or six comma-separated numbers in CRS84 order: minLon,minLat,maxLon,maxLat (optionally with vertical min/max).
    #[param(
        value_type = String,
        example = "-180,-90,180,90"
    )]
    #[serde(default)]
    #[serde(deserialize_with = "parse_bbox_option")]
    #[allow(unused, reason = "TODO: incorporate if it makes sense")]
    pub bbox: Option<OgcBbox>,

    /// Optional limit for the number of first-level collections returned (minimum: 1, maximum: 10000, default: 10).
    #[param(example = 10)]
    #[allow(unused, reason = "TODO: incorporate if it makes sense")]
    pub limit: Option<u32>,

    /// Response format. If omitted, the `Accept` header is used.
    #[param(example = "json")]
    #[allow(unused, reason = "TODO: incorporate if it makes sense")]
    pub f: Option<CollectionsResponseFormat>,
}

#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum CollectionsResponseFormat {
    Json,
    // Html, // TODO: support HTML response format
}

/// OGC API Collections List
///
/// Cf. [OGC API - Common - Part 2: Collections](https://docs.ogc.org/DRAFTS/20-024.html).
///
/// Inside Geo Engine, every [`Layer`] gets its own OGC API endpoint.
/// Inside this endpoint, this [`Layer`] is represented as a single [`Collection`](ogcapi_types::common::Collection).
/// Therefore, the list of collections for a given layer will always contain exactly one collection,
/// and the `collectionId` will always be the same as the [`LayerId`].
///
#[utoipa::path(
    tag = "OGC API",
    get,
    path = "/{dataConnectorId}/{layerId}/collections",
    responses(
        (status = 200, description = "OK", body = Collections)
    ),
    params(
        ("dataConnectorId" = DataProviderId, description = "ID of the data connector"),
        ("layerId" = LayerId, description = "ID of the layer, which is used as collection ID"),
        CollectionsQueryParams,
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn collections<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId)>,
    _query: web::Query<CollectionsQueryParams>,
) -> OgcApiResult<web::Json<Collections>> {
    let (data_connector_id, layer_id) = path.into_inner();

    let ctx = app_ctx.session_context(session);
    let layer = load_layer::<C>(&ctx, data_connector_id, layer_id.clone()).await?;

    let create_link = link_creator(data_connector_id, layer_id.clone());

    let collections = vec![
        build_collection(
            layer_id,
            &create_link,
            raster_workflow_metadata::<C::SessionContext>(
                layer.workflow.clone(),
                ctx.execution_context()?,
            ),
            workflow_provenance(&layer.workflow, &ctx).map_err(Into::into),
            no_timestamps(),
        )
        .await?,
    ];

    let collections = Collections {
        links: vec![create_link("collections", SELF, JSON)?],
        time_stamp: None,
        number_matched: None,
        number_returned: Some(collections.len() as u64),
        crs: {
            let mut seen_crs = HashSet::<&Crs>::new();
            collections
                .iter()
                .flat_map(|c| c.crs.as_slice())
                .filter(|crs| seen_crs.insert(crs))
                .cloned()
                .collect()
        },
        collections,
    };

    Ok(web::Json(collections))
}

#[derive(Debug, serde::Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct CollectionQueryParams {
    /// Response format. If omitted, the `Accept` header is used.
    #[param(example = "json")]
    #[allow(unused, reason = "TODO: incorporate if it makes sense")]
    pub f: Option<CollectionsResponseFormat>,
}

/// OGC API Collection Metadata
///
/// Cf. [OGC API - Common - Part 2: Collections](https://docs.ogc.org/DRAFTS/20-024.html).
#[utoipa::path(
    tag = "OGC API",
    get,
    path = "/{dataConnectorId}/{layerId}/collections/{layerId}",
    responses(
        (status = 200, description = "OK", body = Collection)
    ),
    params(
        ("dataConnectorId" = DataProviderId, description = "ID of the data connector"),
        ("layerId" = LayerId, description = "ID of the layer, which is used as collection ID"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn collection<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId, LayerId)>,
    _query: web::Query<CollectionQueryParams>,
) -> OgcApiResult<web::Json<Collection>> {
    let (data_connector_id, layer_id, collection_id) = path.into_inner();

    if layer_id != collection_id {
        return Err(OgcApiError::CollectionNotFound { collection_id })?;
    }

    let ctx = app_ctx.session_context(session);
    let layer = load_layer::<C>(&ctx, data_connector_id, layer_id.clone()).await?;

    let processing_graph_id = ctx.db().register_workflow(layer.workflow.clone()).await?;

    let query_context = ctx.query_context(processing_graph_id.0, Uuid::new_v4())?;

    let descriptor = raster_workflow_metadata::<C::SessionContext>(
        layer.workflow.clone(),
        ctx.execution_context()?,
    )
    .await?;

    let raster_query_processor =
        raster_query_processor::<C::SessionContext>(&layer, ctx.execution_context()?).await?;

    let create_link = link_creator(data_connector_id, layer_id.clone());

    let collection = build_collection(
        layer_id,
        &create_link,
        futures::future::ready(Ok(descriptor.clone())),
        workflow_provenance(&layer.workflow, &ctx).map_err(Into::into),
        Some(
            time_stream::<C::SessionContext>(
                &query_context,
                &raster_query_processor,
                descriptor.time.bounds.unwrap_or_default(),
            )
            .await?,
        ),
    )
    .await?;

    Ok(web::Json(collection))
}

async fn raster_query_processor<C: SessionContext>(
    layer: &Layer,
    execution_context: C::ExecutionContext,
) -> OgcApiResult<TypedRasterQueryProcessor> {
    let initialized_operator =
        get_initialized_raster_operator::<C>(layer, &execution_context).await?;

    initialized_operator
        .query_processor()
        .boxed_context(error::InitializingProcessingGraph)
}

async fn time_stream<'a, C: SessionContext>(
    query_context: &'a C::QueryContext,
    processor: &'a TypedRasterQueryProcessor,
    query_interval: TimeInterval,
) -> OgcApiResult<BoxStream<'a, OgcApiResult<TimeInterval>>> {
    let time_stream = call_on_generic_raster_processor!(
        processor, p => p.time_query(query_interval, query_context).await
    )
    .map_err(|source| OgcApiError::Internal {
        source: source.into(),
    })?
    .map_err(|source| OgcApiError::Internal {
        source: source.into(),
    });

    Ok(time_stream.boxed())
}

async fn time_intervals_from_stream(
    mut stream: impl Stream<Item = OgcApiResult<TimeInterval>> + Unpin,
) -> OgcApiResult<Option<Vec<[Option<DateTime<Utc>>; 2]>>> {
    let mut intervals = Vec::new();

    while let Some(interval) = stream.next().await {
        intervals.push(ogc_interval(interval?));

        if intervals.len() >= MAX_NUMBER_OF_TIME_INTERVALS {
            warn!(
                "Stopped at returning {MAX_NUMBER_OF_TIME_INTERVALS} time intervals for OGC API Collection"
            );
            break;
        }
    }

    Ok(Some(intervals))
}

fn no_timestamps() -> Option<impl Stream<Item = OgcApiResult<TimeInterval>>> {
    None::<futures::stream::Empty<OgcApiResult<TimeInterval>>>
}

async fn build_collection(
    collection_id: LayerId,
    create_link: &LinkCreator,
    descriptor: impl Future<Output = OgcApiResult<RasterResultDescriptor>>,
    provenance: impl Future<Output = OgcApiResult<Vec<ProvenanceEntry>>>,
    timestamps: Option<impl Stream<Item = OgcApiResult<TimeInterval>> + Unpin>,
) -> OgcApiResult<Collection> {
    let (descriptor, provenance, timestamps) = if let Some(stream) = timestamps {
        futures::try_join!(descriptor, provenance, time_intervals_from_stream(stream))?
    } else {
        futures::try_join!(descriptor, provenance, futures::future::ok(None))?
    };

    let crs = if descriptor.spatial_reference.is_unreferenced() {
        None
    } else {
        Some(crs_from_spatial_reference_option(
            descriptor.spatial_reference,
        )?)
    };

    Ok(Collection {
        id: collection_id.to_string(),
        title: Some(format!("Raster Layer [{collection_id}]")),
        description: Some(format!(
            "Raster collection generated from layer `{collection_id}`"
        )),
        item_type: CollectionItemType::Tile.to_string(),
        links: vec![
            create_link(&format!("collections/{collection_id}"), SELF, JSON)?,
            create_link(
                &format!("collections/{collection_id}/map/tiles"),
                MAP_TILESETS_REL,
                JSON,
            )?,
        ],
        extent: Some(Extent {
            spatial: Some(SpatialExtent {
                bbox: vec![to_ogc_bbox(descriptor.spatial_bounds())],
                crs: crs.clone(),
            }),
            temporal: timestamps.map(|timestamps| TemporalExtent {
                interval: timestamps,
                ..TemporalExtent::default()
            }),
        }),
        crs: crs.into_iter().collect(),
        attribution: attribution_from_provenance(&provenance),
        ..Collection::default()
    })
}

fn attribution_from_provenance(provenance: &[ProvenanceEntry]) -> Option<String> {
    if provenance.is_empty() {
        return None;
    }

    provenance
        .iter()
        .filter_map(ProvenanceEntry::attribution)
        .join(", ")
        .into()
}

fn ogc_interval(interval: TimeInterval) -> [Option<DateTime<Utc>>; 2] {
    [
        time_instance_to_ogc_datetime(interval.start()),
        time_instance_to_ogc_datetime(interval.end()),
    ]
}

fn time_instance_to_ogc_datetime(time_instance: TimeInstance) -> Option<DateTime<Utc>> {
    if time_instance.is_min() || time_instance.is_max() {
        return None;
    }

    time_instance
        .as_date_time()
        .map(chrono::DateTime::<chrono::Utc>::from)
}

async fn ensure_layer_exists<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    data_connector_id: DataProviderId,
    layer_id: LayerId,
) -> OgcApiResult<()> {
    let ctx = app_ctx.session_context(session);
    let _layer = load_layer::<C>(&ctx, data_connector_id, layer_id).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::model::datatypes::DataProviderId,
        contexts::{ApplicationContext, PostgresContext, Session, SessionContext, SessionId},
        ge_context,
        util::tests::{add_ndvi_to_layers, admin_login, read_body_json, send_test_request},
    };
    use actix_web::{http::header, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use pretty_assertions::assert_eq;
    use tokio_postgres::NoTls;

    async fn session_and_layer_id(
        app_ctx: &PostgresContext<NoTls>,
    ) -> (SessionId, DataProviderId, LayerId) {
        let session = admin_login(app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = ctx.session().id();
        let (data_connector_id, layer_id) = add_ndvi_to_layers(app_ctx).await;

        (session_id, data_connector_id.into(), layer_id.into())
    }

    #[ge_context::test]
    async fn it_returns_ogc_landing_page(app_ctx: PostgresContext<NoTls>) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, data_connector_id, layer_id) = session_and_layer_id(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!("/ogc/{data_connector_id}/{layer_id}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        assert_eq!(
            body,
            serde_json::json!({
              "title": format!("Geo Engine OGC API [{data_connector_id}/{layer_id}]"),
              "description": format!("OGC API Landing Page for Geo Engine and layer {layer_id} of data connector {data_connector_id}"),
              "links": [
                {
                  "href": format!("{server_url}/api/ogc/{data_connector_id}/{layer_id}/"),
                  "rel": "self",
                  "type": "application/json"
                },
                {
                  "href": format!("{server_url}/api/ogc/{data_connector_id}/{layer_id}/conformance"),
                  "rel": "conformance",
                  "type": "application/json"
                },
                {
                  "href": format!("{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections"),
                  "rel": "data",
                  "type": "application/json"
                },
                {
                  "href": format!("{server_url}/api/ogc/{data_connector_id}/{layer_id}/tileMatrixSets"),
                  "rel": "http://www.opengis.net/def/rel/ogc/1.0/tiling-schemes",
                  "type": "application/json"
                }
              ]
            })
        );
    }

    #[ge_context::test]
    async fn it_returns_ogc_conformance_classes(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) = session_and_layer_id(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!("/ogc/{data_connector_id}/{layer_id}/conformance"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        assert_eq!(
            body,
            serde_json::json!({
              "conformsTo": [
                "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
                "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/landing-page",
                "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/oas30",
                "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/json",
                "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/collections",
                "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/core",
                "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tileset",
                "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tilesets-list",
                "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/png",
                "http://www.opengis.net/spec/tms/2.0/conf/tilematrixset",
                "http://www.opengis.net/spec/tms/2.0/conf/json-tilematrixset",
              ]
            })
        );
    }

    #[ge_context::test]
    async fn it_returns_ogc_collections(app_ctx: PostgresContext<NoTls>) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, data_connector_id, layer_id) = session_and_layer_id(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!("/ogc/{data_connector_id}/{layer_id}/collections"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        assert_eq!(
            body,
            serde_json::json!({
                "links": [{
                    "href": format!("{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections"),
                    "rel": "self",
                    "type": "application/json"
                }],
                "numberReturned": 1,
                "collections": [{
                    "id": layer_id,
                    "title": format!("Raster Layer [{layer_id}]"),
                    "description": format!("Raster collection generated from layer `{layer_id}`"),
                    "attribution": "Sample Citation",
                    "extent": {
                        "spatial": {
                            "bbox": [[-180.0, -90.0, 180.0, 90.0]],
                            "crs": "http://www.opengis.net/def/crs/EPSG/0/4326"
                        }
                    },
                    "itemType": "tile",
                    "crs": ["http://www.opengis.net/def/crs/EPSG/0/4326"],
                    "links": [
                        {
                            "href": format!("{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}"),
                            "rel": "self",
                            "type": "application/json"
                        },
                        {
                            "href": format!("{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles"),
                            "rel": MAP_TILESETS_REL,
                            "type": "application/json"
                        }
                    ]
                }],
                "crs": ["http://www.opengis.net/def/crs/EPSG/0/4326"]
            })
        );
    }

    #[ge_context::test]
    async fn it_returns_ogc_collection_metadata(app_ctx: PostgresContext<NoTls>) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, data_connector_id, layer_id) = session_and_layer_id(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        assert_eq!(
            body,
            serde_json::json!({
                "id": layer_id,
                "title": format!("Raster Layer [{layer_id}]"),
                "description": format!("Raster collection generated from layer `{layer_id}`"),
                "attribution": "Sample Citation",
                "extent": {
                    "spatial": {
                        "bbox": [[-180.0, -90.0, 180.0, 90.0]],
                        "crs": "http://www.opengis.net/def/crs/EPSG/0/4326"
                    },
                    "temporal": {
                        "interval": [
                            ["2014-01-01T00:00:00Z", "2014-02-01T00:00:00Z"],
                            ["2014-02-01T00:00:00Z", "2014-03-01T00:00:00Z"],
                            ["2014-03-01T00:00:00Z", "2014-04-01T00:00:00Z"],
                            ["2014-04-01T00:00:00Z", "2014-05-01T00:00:00Z"],
                            ["2014-05-01T00:00:00Z", "2014-06-01T00:00:00Z"],
                            ["2014-06-01T00:00:00Z", "2014-07-01T00:00:00Z"]
                        ],
                        "trs": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"
                    }
                },
                "crs": ["http://www.opengis.net/def/crs/EPSG/0/4326"],
                "itemType": "tile",
                "links": [
                    {
                        "href": format!("{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}"),
                        "rel": "self",
                        "type": "application/json"
                    },
                    {
                        "href": format!("{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles"),
                        "rel": MAP_TILESETS_REL,
                        "type": "application/json"
                    }
                ]
            })
        );
    }
}
