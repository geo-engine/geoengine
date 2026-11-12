use crate::{
    api::{
        handlers::ogc::{
            OgcApiResult,
            error::OgcApiError,
            tms_spec::{
                CustomNativeTMS, CustomWebMercatorTMS, TileMatrixSetProvider,
                TypedTileMatrixSetProvider, WebMercatorQuadTMS,
            },
            util::{
                crs_from_spatial_reference_option, link_creator, load_layer, parse_datetime_option,
                raster_processing_graph_metadata,
            },
        },
        model::datatypes::{DataProviderId, LayerId},
    },
    config::{self},
    contexts::{ApplicationContext, SessionContext},
    queries::{
        ogc_tile::{TileQuery, load_tile_layer, render_tile, tile_etag},
        run_query,
    },
    util::server::{CacheControlHeader, connection_closed},
};
use actix_web::{HttpRequest, HttpResponse, http::header, web};
use geoengine_datatypes::primitives::{AxisAlignedRectangle, TimeInstance, TimeInterval};
use geoengine_operators::engine::ExecutionContext;
use ogcapi_types::{
    common::{
        Crs, Datetime as OgcDatetime, IntervalDatetime,
        link_rel::{ITEM, SELF, TILING_SCHEME},
        media_type::{JSON, PNG},
    },
    tiles::{
        AccessConstraints, BoundingBox2D, DataType, TileMatrixSetId, TileSet, TileSetItem,
        TileSets, TilesCrs,
    },
};
use std::time::Duration;
use utoipa::IntoParams;

/// OGC API Collection Tilesets List
///
/// Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).
#[utoipa::path(
	tag = "OGC API",
	get,
	path = "/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles",
	responses(
		(status = 200, description = "OK", body = TileSets),
		(status = 404, description = "Collection not found")
	),
	params(
		("dataConnectorId" = DataProviderId, description = "ID of the data connector"),
        ("layerId" = LayerId, description = "ID of the layer, which is used as collection ID"),
	),
	security(
		("session_token" = [])
	)
)]
pub async fn collection_tilesets<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId, LayerId)>,
) -> OgcApiResult<web::Json<TileSets>> {
    let (data_connector_id, layer_id, collection_id) = path.into_inner();

    ensure_matching_collection(&layer_id, collection_id)?;

    let ctx = app_ctx.session_context(session);

    let layer = load_layer::<C>(&ctx, data_connector_id, layer_id.clone()).await?;

    let descriptor = raster_processing_graph_metadata::<C::SessionContext>(
        layer.processing_graph.clone(),
        ctx.execution_context()?,
    )
    .await?;

    let create_link = link_creator(data_connector_id, layer_id.clone());

    Ok(web::Json(TileSets {
        tilesets: vec![
            TileSetItem {
                title: Some(CustomNativeTMS::TILE_MATRIX_SET_TITLE.to_string()),
                data_type: DataType::Map,
                crs: TilesCrs::Simple(crs_from_spatial_reference_option(
                    descriptor.spatial_reference,
                )?),
                tile_matrix_set_uri: None,
                links: vec![create_link(
                    &format!(
                        "collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}",
                        TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
                    ),
                    SELF,
                    JSON,
                )?],
            },
            TileSetItem {
                title: Some(CustomWebMercatorTMS::TILE_MATRIX_SET_TITLE.to_string()),
                data_type: DataType::Map,
                crs: TilesCrs::Simple(Crs::from_epsg(3857)),
                tile_matrix_set_uri: None,
                links: vec![create_link(
                    &format!(
                        "collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}",
                        TILE_MATRIX_SET_ID = CustomWebMercatorTMS::TILE_MATRIX_SET_ID
                    ),
                    SELF,
                    JSON,
                )?],
            },
            TileSetItem {
                title: Some(WebMercatorQuadTMS::TILE_MATRIX_SET_TITLE.to_string()),
                data_type: DataType::Map,
                // TODO: fix `ogcapi-types` to support `TilesCrs::Uri` and use it here instead of `TilesCrs::Simple`
                // crs: TilesCrs::Uri {
                //     uri: "http://www.opengis.net/def/crs/EPSG/0/3857".to_string(),
                // },
                crs: TilesCrs::Simple(Crs::from_epsg(3857)),
                tile_matrix_set_uri: Some(
                    "http://www.opengis.net/def/tilematrixset/OGC/1.0/WebMercatorQuad".to_string(),
                ),
                links: vec![create_link(
                    &format!(
                        "collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}",
                        TILE_MATRIX_SET_ID = WebMercatorQuadTMS::TILE_MATRIX_SET_ID
                    ),
                    SELF,
                    JSON,
                )?],
            },
        ],
        links: vec![create_link(
            &format!("collections/{layer_id}/map/tiles"),
            SELF,
            JSON,
        )?],
    }))
}

/// OGC API Collection Tileset Metadata
///
/// Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).
#[utoipa::path(
	tag = "OGC API",
	get,
	path = "/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles/{tileMatrixSetId}",
	responses(
		(status = 200, description = "OK", body = TileSet),
		(status = 404, description = "Collection or tile matrix set not found")
	),
	params(
		("dataConnectorId" = DataProviderId, description = "ID of the data connector"),
        ("layerId" = LayerId, description = "ID of the layer, which is used as collection ID"),
		("tileMatrixSetId" = TileMatrixSetId, description = "Tile matrix set identifier")
	),
	security(
		("session_token" = [])
	)
)]
pub async fn collection_tileset<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId, LayerId, TileMatrixSetId)>,
) -> OgcApiResult<web::Json<TileSet>> {
    let (data_connector_id, layer_id, collection_id, tile_matrix_set_id) = path.into_inner();

    ensure_matching_collection(&layer_id, collection_id)?;
    TypedTileMatrixSetProvider::ensure_exists(&tile_matrix_set_id)?;

    let ctx = app_ctx.session_context(session);
    let execution_context = ctx.execution_context()?;
    let tiling_specification = execution_context.tiling_specification();
    let layer = load_layer::<C>(&ctx, data_connector_id, layer_id.clone()).await?;
    let descriptor = raster_processing_graph_metadata::<C::SessionContext>(
        layer.processing_graph,
        ctx.execution_context()?,
    )
    .await?;

    // Resolve the appropriate TMS provider to get correct CRS
    let provider = TypedTileMatrixSetProvider::resolve(
        &tile_matrix_set_id,
        &descriptor,
        tiling_specification,
    )?;

    let create_link = link_creator(data_connector_id, layer_id.clone());
    let crs = provider.tiles_crs()?;
    let spatial_bounds = descriptor.spatial_bounds();

    Ok(web::Json(TileSet {
        title: Some("Tileset Metadata".to_string()),
        data_type: DataType::Map,
        description: None,
        keywords: vec![],
        tile_matrix_set_uri: None,
        tile_matrix_set_limits: vec![], // TODO: add limits if data is expensive to process
        crs: crs.clone(),
        epoch: None,
        layers: vec![],
        bounding_box: Some(BoundingBox2D {
            lower_left: spatial_bounds.lower_left().into(),
            upper_right: spatial_bounds.upper_right().into(),
            crs: Some(crs),
            ordered_axes: None, // TODO: should we add this?
        }),
        center_point: None,
        style: None,
        attribution: None, // TODO: Is this the license of the tileset or data?
        license: None,     // TODO: Is this the license of the tileset or data?
        access_constraints: Some(AccessConstraints::Restricted), // TODO: re-iterate about this
        version: None,
        created: None,
        updated: None,
        point_of_contact: None,
        media_types: vec![PNG.to_string()],
        links: vec![
            create_link(
                &format!("collections/{layer_id}/map/tiles/{tile_matrix_set_id}"),
                SELF,
                JSON,
            )?,
            create_link(
                &format!("tileMatrixSets/{tile_matrix_set_id}"),
                TILING_SCHEME,
                JSON,
            )?,
            {
                let mut link = create_link(
                    &format!("collections/{layer_id}/map/tiles/{tile_matrix_set_id}"),
                    ITEM,
                    PNG,
                )?
                .templated(true);
                link.href += "/{tileMatrix}/{tileRow}/{tileCol}?datetime={datetime}"; // prevents it from being urlencoded
                link
            },
        ],
    }))
}

#[derive(Debug, serde::Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct TileQueryParams {
    /// Either a date-time or an interval, half-bounded or bounded. Date and time expressions adhere to RFC 3339. Half-bounded intervals use double dots (`..`).
    #[param(value_type = String, example = "2018-02-12T23:20:50Z")]
    #[serde(default)]
    #[serde(deserialize_with = "parse_datetime_option")]
    pub datetime: Option<OgcDatetime>,
}

impl TileQuery {
    fn from_params(
        path: web::Path<(
            DataProviderId,
            LayerId,
            LayerId,
            TileMatrixSetId,
            u8,
            u32,
            u32,
        )>,
        query: TileQueryParams,
    ) -> OgcApiResult<Self> {
        let (
            data_connector_id,
            layer_id,
            collection_id,
            tile_matrix_set_id,
            tile_matrix,
            tile_row,
            tile_col,
        ) = path.into_inner();

        ensure_matching_collection(&layer_id, collection_id)?;
        TypedTileMatrixSetProvider::ensure_exists(&tile_matrix_set_id)?;

        Ok(Self {
            data_connector_id,
            layer_id,
            tile_matrix_set_id,
            time_interval: query_time_from_datetime(query.datetime)?,
            tile_matrix,
            tile_row,
            tile_col,
        })
    }
}

/// OGC API Tile
///
/// Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).
///
/// ## Sketch
///
/// ```text
/// pointOfOrigin (cornerOfOrigin=topLeft)
///
/// tileMatrixMinX, tileMatrixMaxY                                               tileMatrixMaxX
///        |                                                                            |
///        v                                                                            v
///        +---------------------------+---------------------------+-----+---------------------------+ ---> tileCol axis
///        | 0,0                       | 1,0                       | ... | matrixWidth-1,0           |
///        |                           |                           |     |                           |
///        +---------------------------+---------------------------+-----+---------------------------+
///        | 0,1                       | 1,1                       | ... | matrixWidth-1,1           |
///        |                           |                           |     |                           |
///        +---------------------------+---------------------------+-----+---------------------------+
///        | ...                       | ...                       | ... | ...                       |
///        |                           |                           |     |                           |
///        +---------------------------+---------------------------+-----+---------------------------+
///        | 0,                        | 1,                        | ... | matrixWidth-1,            |
///  v     |   matrixHeight-1          |   matrixHeight-1          |     |   matrixHeight-1          | --+ tileHeight
/// tileMatrixMinY                     |                           |     |                           |   | (in pixels)
///        +---------------------------+---------------------------+-----+---------------------------+ --+
///  |                                                                   |<-       tileWidth       ->|
///  v                                                                   |        (in pixels)        |
/// tileRow axis
/// ```
///
#[utoipa::path(
	tag = "OGC API",
	get,
	path = "/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles/{tileMatrixSetId}/{tileMatrix}/{tileRow}/{tileCol}",
	responses(
		(status = 200, response = crate::api::model::responses::PngResponse),
		(status = 400, description = "Invalid tile coordinates or datetime"),
		(status = 404, description = "Collection or tile matrix set not found")
	),
	params(
		("dataConnectorId" = DataProviderId, description = "ID of the data connector"),
        ("layerId" = LayerId, description = "ID of the layer, which is used as collection ID"),
		("tileMatrixSetId" = TileMatrixSetId, description = "Tile matrix set identifier"),
		("tileMatrix" = u8, description = "Tile matrix level"),
		("tileRow" = u64, description = "Tile row"),
		("tileCol" = u64, description = "Tile column"),
		TileQueryParams
	),
	security(
		("session_token" = [])
	)
)]
pub async fn tile<C: ApplicationContext>(
    req: HttpRequest,
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(
        DataProviderId,
        LayerId,
        LayerId,
        TileMatrixSetId,
        u8,
        u32,
        u32,
    )>,
    query: web::Query<TileQueryParams>,
) -> OgcApiResult<HttpResponse> {
    let query = TileQuery::from_params(path, query.into_inner())?;

    let ctx = app_ctx.session_context(session);

    let connection_closed_handler = connection_closed(
        &req,
        config::get_config_element::<config::Ogc>()
            .ok()
            .and_then(|cfg| cfg.tiles)
            .and_then(|tiles| tiles.request_timeout_seconds)
            .map(Duration::from_secs),
    );

    let layer = load_tile_layer::<C>(&ctx, &query).await?;

    // If the client has a matching `ETag`, the processing graph, symbology, tile and time are the
    // same, so the tile is the same. Changes of the data are covered by the cache duration.
    // The client keeps the `Cache-Control` of its stored response, so the 304 does not repeat it.
    let etag = tile_etag(&layer, &query);
    if if_none_match(&req, &etag) {
        return Ok(HttpResponse::NotModified()
            .append_header((header::ETAG, etag))
            .finish());
    }

    let (image_bytes, cache_hint, computation_id) = run_query(
        &ctx,
        connection_closed_handler,
        render_tile::<C>(ctx.clone(), query, layer),
    )
    .await?;

    // Within the cache duration, the client uses its cached tile. After that, it revalidates the
    // tile with its `ETag`.
    Ok(HttpResponse::Ok()
        .content_type(mime::IMAGE_PNG)
        .append_header((header::ETAG, etag))
        .append_header(cache_hint.cache_control_header())
        .append_header(computation_id)
        .body(image_bytes))
}

/// Whether the `If-None-Match` header of `req` contains `etag`.
///
/// `If-None-Match` uses the weak comparison, so the `W/` prefix is ignored on both sides.
fn if_none_match(req: &HttpRequest, etag: &str) -> bool {
    fn opaque_tag(tag: &str) -> &str {
        let tag = tag.trim();
        tag.strip_prefix("W/").unwrap_or(tag)
    }

    let etag = opaque_tag(etag);

    req.headers()
        .get_all(header::IF_NONE_MATCH)
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|tag| tag.trim() == "*" || opaque_tag(tag) == etag)
}

fn ensure_matching_collection(layer_id: &LayerId, collection_id: LayerId) -> OgcApiResult<()> {
    if *layer_id != collection_id {
        return Err(OgcApiError::CollectionNotFound { collection_id });
    }

    Ok(())
}

fn query_time_from_datetime(datetime: Option<OgcDatetime>) -> OgcApiResult<TimeInterval> {
    match datetime {
        Some(OgcDatetime::Datetime(datetime)) => {
            TimeInterval::new_instant(geoengine_datatypes::primitives::DateTime::from(datetime))
                .map_err(|source| OgcApiError::Internal {
                    source: source.into(),
                })
        }
        Some(OgcDatetime::Interval { from, to }) => {
            let start = interval_start_to_time_instance(&from);
            let end = interval_end_to_time_instance(&to);

            TimeInterval::new(start, end).map_err(|source| OgcApiError::Internal {
                source: source.into(),
            })
        }
        None => Err(OgcApiError::MissingTime),
    }
}

fn interval_start_to_time_instance(endpoint: &IntervalDatetime) -> TimeInstance {
    match endpoint {
        IntervalDatetime::Datetime(datetime) => {
            geoengine_datatypes::primitives::DateTime::from(*datetime).into()
        }
        IntervalDatetime::Open => TimeInstance::MIN,
    }
}

fn interval_end_to_time_instance(endpoint: &IntervalDatetime) -> TimeInstance {
    match endpoint {
        IntervalDatetime::Datetime(datetime) => {
            geoengine_datatypes::primitives::DateTime::from(*datetime).into()
        }
        IntervalDatetime::Open => TimeInstance::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::handlers::ogc::test_util::{
            session_and_3857_layer_id, session_and_4326_layer_id,
            session_and_4326_project_layer_id, session_and_4326_rgb_layer_id,
            session_and_native_3857_layer_id, session_and_ndvi_multi_band_layer_id,
        },
        contexts::PostgresContext,
        ge_context,
        util::tests::{read_body_json, send_test_request},
    };
    use actix_web::http::header;
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::{
        test_data,
        util::{assert_image_equals, test::save_test_bytes_if_not_exists},
    };
    use pretty_assertions::assert_eq;
    use tokio_postgres::NoTls;

    #[ge_context::test]
    async fn it_lists_tilesets_for_collection(app_ctx: PostgresContext<NoTls>) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        assert_eq!(
            body,
            serde_json::json!({
                "tilesets": [
                    {
                        "title": CustomNativeTMS::TILE_MATRIX_SET_TITLE,
                        "dataType": "map",
                        "crs": "http://www.opengis.net/def/crs/EPSG/0/4326",
                        "links": [
                            {
                                "href": format!(
                                    "{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}", TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
                                ),
                                "rel": "self",
                                "type": "application/json"
                            }
                        ]
                    },
                                        {
                        "title": CustomWebMercatorTMS::TILE_MATRIX_SET_TITLE,
                        "dataType": "map",
                        "crs": "http://www.opengis.net/def/crs/EPSG/0/3857",
                        "links": [
                            {
                                "href": format!(
                                    "{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}", TILE_MATRIX_SET_ID = CustomWebMercatorTMS::TILE_MATRIX_SET_ID
                                ),
                                "rel": "self",
                                "type": "application/json"
                            }
                        ]
                    },
                    {
                        "title": WebMercatorQuadTMS::TILE_MATRIX_SET_TITLE,
                        "dataType": "map",
                        "crs": "http://www.opengis.net/def/crs/EPSG/0/3857",
                        "tileMatrixSetURI": "http://www.opengis.net/def/tilematrixset/OGC/1.0/WebMercatorQuad",
                        "links": [
                            {
                                "href": format!(
                                    "{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}",
                                    TILE_MATRIX_SET_ID = WebMercatorQuadTMS::TILE_MATRIX_SET_ID
                                ),
                                "rel": "self",
                                "type": "application/json"
                            }
                        ]
                    }
                ],
                "links": [
                    {
                        "href": format!(
                            "{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles"
                        ),
                        "rel": "self",
                        "type": "application/json"
                    }
                ]
            })
        );
    }

    #[ge_context::test]
    async fn it_returns_tileset_metadata_with_template_link(app_ctx: PostgresContext<NoTls>) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}",
                TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        assert_eq!(
            body,
            serde_json::json!({
                "title": "Tileset Metadata",
                "dataType": "map",
                "crs": "http://www.opengis.net/def/crs/EPSG/0/4326",
                "boundingBox": {
                    "lowerLeft": [-180.0, -90.0],
                    "upperRight": [180.0, 90.0],
                    "crs": "http://www.opengis.net/def/crs/EPSG/0/4326"
                },
                "accessConstraints": "restricted",
                "mediaTypes": ["image/png"],
                "links": [
                    {
                        "href": format!(
                            "{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}",
                            TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
                        ),
                        "rel": "self",
                        "type": "application/json"
                    },
                    {
                        "href": format!(
                            "{server_url}/api/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/{TILE_MATRIX_SET_ID}",
                            TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
                        ),
                        "rel": "http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme",
                        "type": "application/json"
                    },
                    {
                        "href": format!(
                            "{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}/{{tileMatrix}}/{{tileRow}}/{{tileCol}}?datetime={{datetime}}",
                            TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
                        ),
                        "rel": "item",
                        "type": "image/png",
                        "templated": true
                    }
                ]
            })
        );
    }

    #[ge_context::test]
    async fn it_renders_tile_png_with_datetime(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;

        let req = actix_web::test::TestRequest::get()
			.uri(&format!(
				"/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}/2/1/4?datetime=2014-04-01T00:00:00Z",
                TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
			))
			.append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let content_type = res
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let image_bytes = actix_web::test::read_body(res).await;

        assert_eq!(status, 200);
        assert_eq!(content_type.as_deref(), Some("image/png"));

        let file_path = test_data!("ogc/tiles/ndvi_2_1_4.png").to_path_buf();
        save_test_bytes_if_not_exists(&image_bytes, &file_path);

        assert_image_equals(&file_path, &image_bytes);
    }

    #[ge_context::test]
    async fn it_renders_tile_png_of_a_project_layer(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) =
            session_and_4326_project_layer_id(&app_ctx).await;

        assert_eq!(
            data_connector_id,
            crate::layers::storage::PROJECTS_PROVIDER_ID.into()
        );

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}/2/1/4?datetime=2014-04-01T00:00:00Z",
                TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let image_bytes = actix_web::test::read_body(res).await;

        assert_eq!(status, 200);

        // the project layer is a copy of the catalog layer, so it renders the same tile
        assert_image_equals(test_data!("ogc/tiles/ndvi_2_1_4.png"), &image_bytes);
    }

    /// Creates a project with an NDVI layer whose data has the cache TTL `cache_ttl`.
    /// Returns the session, the project, and the uri of a tile of the layer.
    async fn ndvi_project_layer_tile(
        app_ctx: &PostgresContext<NoTls>,
        cache_ttl: geoengine_datatypes::primitives::CacheTtlSeconds,
    ) -> (crate::users::UserSession, crate::projects::Project, String) {
        use crate::projects::{LayerUpdate, ProjectDb, ProjectLayerUpdate, UpdateProject};
        use crate::util::tests::{
            admin_login, create_project_helper, ndvi_255_symbology,
            register_ndvi_processing_graph_helper_with_cache_ttl,
        };

        let (_, processing_graph) =
            register_ndvi_processing_graph_helper_with_cache_ttl(app_ctx, cache_ttl).await;

        let session = admin_login(app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());
        let project = create_project_helper(&ctx).await;

        let project = ctx
            .db()
            .update_project(UpdateProject {
                id: project,
                name: None,
                description: None,
                layers: Some(vec![LayerUpdate::UpdateOrInsert(ProjectLayerUpdate {
                    id: None,
                    name: "NDVI".to_string(),
                    description: String::new(),
                    processing_graph,
                    symbology: Some(ndvi_255_symbology()),
                    properties: vec![],
                    metadata: Default::default(),
                    visibility: Default::default(),
                    source: None,
                })]),
                plots: None,
                bounds: None,
                time_step: None,
            })
            .await
            .unwrap();

        let layer_id = &project.layers[0].layer.id.layer_id;
        let uri = format!(
            "/ogc/{PROJECTS_PROVIDER_ID}/{layer_id}/collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}/2/1/4?datetime=2014-04-01T00:00:00Z",
            PROJECTS_PROVIDER_ID = crate::layers::storage::PROJECTS_PROVIDER_ID,
            TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
        );

        (session, project, uri)
    }

    async fn get_tile(
        app_ctx: &PostgresContext<NoTls>,
        session: &crate::users::UserSession,
        uri: &str,
        if_none_match: Option<&str>,
    ) -> actix_web::dev::ServiceResponse {
        let mut req = actix_web::test::TestRequest::get()
            .uri(uri)
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())));
        if let Some(etag) = if_none_match {
            req = req.append_header((header::IF_NONE_MATCH, etag));
        }
        send_test_request(req, app_ctx.clone()).await
    }

    fn header_value(
        res: &actix_web::dev::ServiceResponse,
        name: header::HeaderName,
    ) -> Option<String> {
        res.headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
    }

    #[ge_context::test]
    async fn it_revalidates_tiles_with_an_etag(app_ctx: PostgresContext<NoTls>) {
        use crate::projects::{LayerUpdate, ProjectDb, ProjectLayerUpdate, UpdateProject};

        let (session, project, uri) = ndvi_project_layer_tile(
            &app_ctx,
            geoengine_datatypes::primitives::CacheTtlSeconds::new(0),
        )
        .await;

        // the first request renders the tile and returns an `ETag`
        let res = get_tile(&app_ctx, &session, &uri, None).await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            header_value(&res, header::CACHE_CONTROL).as_deref(),
            Some("no-cache")
        );
        let etag = header_value(&res, header::ETAG).expect("should have an ETag");
        assert!(etag.starts_with("W/\""), "{etag}");

        // the tile has not changed, so it is not rendered again
        let res = get_tile(&app_ctx, &session, &uri, Some(&etag)).await;
        assert_eq!(header_value(&res, header::ETAG), Some(etag.clone()));
        assert_eq!(res.status(), 304);
        assert_eq!(header_value(&res, header::CACHE_CONTROL), None);
        assert!(actix_web::test::read_body(res).await.is_empty());

        // the strong form of the tag and a list of tags match too
        let res = get_tile(
            &app_ctx,
            &session,
            &uri,
            Some(&format!(
                "\"other\", {}",
                etag.strip_prefix("W/").expect("should be a weak ETag")
            )),
        )
        .await;
        assert_eq!(res.status(), 304);

        // changing the symbology changes the tile
        let layer = &project.layers[0];
        let mut symbology = crate::util::tests::ndvi_255_symbology();
        if let crate::projects::Symbology::Raster(raster) = &mut symbology {
            raster.opacity = 0.5;
        }

        app_ctx
            .session_context(session.clone())
            .db()
            .update_project(UpdateProject {
                id: project.id,
                name: None,
                description: None,
                layers: Some(vec![LayerUpdate::UpdateOrInsert(ProjectLayerUpdate {
                    id: Some(layer.layer.id.layer_id.clone()),
                    name: layer.layer.name.clone(),
                    description: layer.layer.description.clone(),
                    processing_graph:
                        crate::processing_graphs::processing_graph::ProcessingGraphId::from_hash(
                            &layer.layer.processing_graph,
                        ),
                    symbology: Some(symbology),
                    properties: vec![],
                    metadata: Default::default(),
                    visibility: Default::default(),
                    source: None,
                })]),
                plots: None,
                bounds: None,
                time_step: None,
            })
            .await
            .unwrap();

        let res = get_tile(&app_ctx, &session, &uri, Some(&etag)).await;
        assert_eq!(res.status(), 200);
        let new_etag = header_value(&res, header::ETAG).expect("should have an ETag");
        assert_ne!(new_etag, etag);
    }

    #[ge_context::test]
    async fn it_sends_an_etag_with_the_cache_duration_of_the_data(app_ctx: PostgresContext<NoTls>) {
        let (session, _project, uri) = ndvi_project_layer_tile(
            &app_ctx,
            geoengine_datatypes::primitives::CacheTtlSeconds::new(60),
        )
        .await;

        let res = get_tile(&app_ctx, &session, &uri, None).await;
        assert_eq!(res.status(), 200);
        let etag = header_value(&res, header::ETAG).expect("should have an ETag");
        let cache_control = header_value(&res, header::CACHE_CONTROL).unwrap();
        assert!(
            cache_control.starts_with("private, max-age="),
            "{cache_control}"
        );

        // after the cache duration, the browser revalidates the tile
        let res = get_tile(&app_ctx, &session, &uri, Some(&etag)).await;
        assert_eq!(res.status(), 304);
    }

    #[ge_context::test]
    async fn it_renders_overview_tile_png_with_datetime(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;

        let req = actix_web::test::TestRequest::get()
			.uri(&format!(
				"/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}/0/0/0?datetime=2014-04-01T00:00:00Z",
                TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
			))
			.append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let content_type = res
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let image_bytes = actix_web::test::read_body(res).await;

        assert_eq!(status, 200);
        assert_eq!(content_type.as_deref(), Some("image/png"));

        let file_path = test_data!("ogc/tiles/ndvi_0_0_0.png").to_path_buf();
        save_test_bytes_if_not_exists(&image_bytes, &file_path);

        assert_image_equals(&file_path, &image_bytes);
    }

    #[ge_context::test]
    async fn it_renders_3857_overview_tile_png_with_datetime(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) = session_and_3857_layer_id(&app_ctx).await;

        let req = actix_web::test::TestRequest::get()
			.uri(&format!(
				"/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}/0/0/0?datetime=2014-04-01T00:00:00Z",
                TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
			))
			.append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let content_type = res
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let image_bytes = actix_web::test::read_body(res).await;

        assert_eq!(status, 200);
        assert_eq!(content_type.as_deref(), Some("image/png"));

        let file_path = test_data!("ogc/tiles/ndvi_3857_0_0_0.png").to_path_buf();
        save_test_bytes_if_not_exists(&image_bytes, &file_path);

        assert_image_equals(&file_path, &image_bytes);
    }

    #[ge_context::test]
    async fn it_renders_native_3857_overview_tile_png_with_datetime(
        app_ctx: PostgresContext<NoTls>,
    ) {
        let (session_id, data_connector_id, layer_id) =
            session_and_native_3857_layer_id(&app_ctx).await;

        let req = actix_web::test::TestRequest::get()
			.uri(&format!(
				"/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}/0/0/0?datetime=2014-04-01T00:00:00Z",
                TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
			))
			.append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let content_type = res
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let image_bytes = actix_web::test::read_body(res).await;

        assert_eq!(status, 200);
        assert_eq!(content_type.as_deref(), Some("image/png"));

        let file_path = test_data!("ogc/tiles/ndvi_native_3857_0_0_0.png").to_path_buf();
        save_test_bytes_if_not_exists(&image_bytes, &file_path);

        assert_image_equals(&file_path, &image_bytes);
    }

    #[ge_context::test]
    async fn it_renders_webmercator_tile_png_with_datetime(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) =
            session_and_native_3857_layer_id(&app_ctx).await;

        let req = actix_web::test::TestRequest::get()
			.uri(&format!(
				"/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}/0/0/0?datetime=2014-04-01T00:00:00Z",
                TILE_MATRIX_SET_ID = WebMercatorQuadTMS::TILE_MATRIX_SET_ID
			))
			.append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let content_type = res
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let image_bytes = actix_web::test::read_body(res).await;

        assert_eq!(
            status,
            200,
            "Response body: {:?}",
            std::str::from_utf8(&image_bytes)
        );
        assert_eq!(content_type.as_deref(), Some("image/png"));

        let file_path = test_data!("ogc/tiles/ndvi_webmercator_0_0_0.png").to_path_buf();
        save_test_bytes_if_not_exists(&image_bytes, &file_path);

        assert_image_equals(&file_path, &image_bytes);
    }

    #[ge_context::test]
    async fn it_renders_custom_webmercator_rgb_tile_png_with_datetime(
        app_ctx: PostgresContext<NoTls>,
    ) {
        let (session_id, data_connector_id, layer_id) =
            session_and_4326_rgb_layer_id(&app_ctx).await;

        let req = actix_web::test::TestRequest::get()
			.uri(&format!(
				"/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}/1/0/1?datetime=2014-04-01T00:00:00Z",
                TILE_MATRIX_SET_ID = CustomWebMercatorTMS::TILE_MATRIX_SET_ID
			))
			.append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let content_type = res
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let image_bytes = actix_web::test::read_body(res).await;

        assert_eq!(
            status,
            200,
            "Response body: {:?}",
            std::str::from_utf8(&image_bytes)
        );
        assert_eq!(content_type.as_deref(), Some("image/png"));

        let file_path = test_data!("ogc/tiles/natural_earth_rgb_1_0_1.png").to_path_buf();
        save_test_bytes_if_not_exists(&image_bytes, &file_path);

        assert_image_equals(&file_path, &image_bytes);
    }

    #[test]
    fn it_retrieves_query_time_from_datetime() {
        use chrono::{TimeZone, Utc};
        use geoengine_datatypes::primitives::{DateTime, TimeInstance, TimeInterval};

        assert_eq!(
            query_time_from_datetime(Some(OgcDatetime::Datetime(
                Utc.with_ymd_and_hms(2026, 4, 1, 12, 30, 45).unwrap()
            )))
            .unwrap(),
            TimeInterval::new_instant(DateTime::new_utc_checked(2026, 4, 1, 12, 30, 45).unwrap())
                .unwrap()
        );

        assert_eq!(
            query_time_from_datetime(Some(OgcDatetime::Interval {
                from: IntervalDatetime::Open,
                to: IntervalDatetime::Open
            }))
            .unwrap(),
            TimeInterval::new(TimeInstance::MIN, TimeInstance::MAX).unwrap()
        );

        assert!(query_time_from_datetime(None).is_err());
    }

    #[ge_context::test]
    async fn it_renders_ndvi_via_multi_band_tile_png_with_datetime(
        app_ctx: PostgresContext<NoTls>,
    ) {
        let (session_id, data_connector_id, layer_id) =
            session_and_ndvi_multi_band_layer_id(&app_ctx).await;

        let req = actix_web::test::TestRequest::get()
			.uri(&format!(
				"/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{TILE_MATRIX_SET_ID}/0/0/0?datetime=2014-04-01T00:00:00Z",
                TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
			))
			.append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let content_type = res
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let image_bytes = actix_web::test::read_body(res).await;

        assert_eq!(status, 200);
        assert_eq!(content_type.as_deref(), Some("image/png"));

        let file_path = test_data!("ogc/tiles/ndvi_0_0_0.png").to_path_buf();
        save_test_bytes_if_not_exists(&image_bytes, &file_path);

        assert_image_equals(&file_path, &image_bytes);
    }

    /// Serving a layer in a foreign CRS (here EPSG 4326 with bounds reaching latitude 90
    /// and beyond latitude -90) through the `WebMercatorQuad` tile matrix set requires
    /// reprojection, so the rendering pipeline depends on the reprojected pixel size
    /// suggestion. Whatever the suggested resolution is, tiles at every zoom level must
    /// render as PNG (200), the data must never be interpolated finer than its own
    /// resolution, and failures must not escalate to an internal server error.
    ///
    /// This exact layer and tile matrix combination is also part of the OGC API tiles
    /// validation, which previously reported HTTP 500s for these requests.
    #[ge_context::test]
    async fn it_renders_ndvi_web_mercator_quad_tiles_at_multiple_zoom_levels(
        app_ctx: PostgresContext<NoTls>,
    ) {
        let (session_id, data_connector_id, layer_id) =
            session_and_ndvi_multi_band_layer_id(&app_ctx).await;

        // zoom 0: very coarse request; zoom 14: ~9.5 m/px, around the native ~11 km
        // resolution of the reprojected layer; zoom 20: much finer than the native one.
        for (zoom, row, col) in [
            (0u8, 0u32, 0u32),
            (1, 1, 1),
            (14, 6000, 6000),
            (20, 300_000, 300_000),
        ] {
            let req = actix_web::test::TestRequest::get()
                .uri(&format!(
                    "/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/WebMercatorQuad/{zoom}/{row}/{col}?datetime=2014-04-01T00:00:00Z"
                ))
                .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

            let res = send_test_request(req, app_ctx.clone()).await;
            let status = res.status();
            let content_type = res
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(ToOwned::to_owned);

            assert_eq!(
                status, 200,
                "tile WebMercatorQuad/{zoom}/{row}/{col} must render"
            );
            assert_eq!(content_type.as_deref(), Some("image/png"));
        }
    }
}
