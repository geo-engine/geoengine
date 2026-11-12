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
        ogc_tile::{FieldSelection, TileQuery, render_coverage_tile, render_tile},
        run_query,
    },
    util::server::{CacheControlHeader, connection_closed},
};
use actix_web::{HttpRequest, HttpResponse, web};
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

/// Media type of coverage tiles, cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/20-057/20-057.html), Clause 16.4
// TODO: add to [`ogcapi_types::common::media_type`] and use from there
pub(crate) const GEOTIFF: &str = "image/tiff; application=geotiff";

/// The kind of tiles of a tileset
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TileDataType {
    /// Rendered map tiles as PNG
    Map,
    /// Coverage tiles with the raw values as `GeoTIFF`
    Coverage,
}

impl TileDataType {
    /// The path segment of the tilesets below the collection, e.g., `map` in `collections/{collectionId}/map/tiles`
    fn path_segment(self) -> &'static str {
        match self {
            Self::Map => "map",
            Self::Coverage => "coverage",
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Map => DataType::Map,
            Self::Coverage => DataType::Coverage,
        }
    }

    fn media_type(self) -> &'static str {
        match self {
            Self::Map => PNG,
            Self::Coverage => GEOTIFF,
        }
    }

    fn tilesets_path(self, layer_id: &LayerId) -> String {
        format!("collections/{layer_id}/{}/tiles", self.path_segment())
    }
}

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
    tilesets::<C>(session, app_ctx, path, TileDataType::Map).await
}

/// OGC API Collection Coverage Tilesets List
///
/// Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/20-057/20-057.html)
/// and [OGC API - Coverages - Part 1: Core](https://docs.ogc.org/DRAFTS/19-087.html).
#[utoipa::path(
	tag = "OGC API",
	get,
	path = "/{dataConnectorId}/{layerId}/collections/{layerId}/coverage/tiles",
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
pub async fn collection_coverage_tilesets<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId, LayerId)>,
) -> OgcApiResult<web::Json<TileSets>> {
    tilesets::<C>(session, app_ctx, path, TileDataType::Coverage).await
}

async fn tilesets<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId, LayerId)>,
    tile_data_type: TileDataType,
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
                data_type: tile_data_type.data_type(),
                crs: TilesCrs::Simple(crs_from_spatial_reference_option(
                    descriptor.spatial_reference,
                )?),
                tile_matrix_set_uri: None,
                links: vec![create_link(
                    &format!(
                        "{tilesets_path}/{TILE_MATRIX_SET_ID}",
                        tilesets_path = tile_data_type.tilesets_path(&layer_id),
                        TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
                    ),
                    SELF,
                    JSON,
                )?],
            },
            TileSetItem {
                title: Some(CustomWebMercatorTMS::TILE_MATRIX_SET_TITLE.to_string()),
                data_type: tile_data_type.data_type(),
                crs: TilesCrs::Simple(Crs::from_epsg(3857)),
                tile_matrix_set_uri: None,
                links: vec![create_link(
                    &format!(
                        "{tilesets_path}/{TILE_MATRIX_SET_ID}",
                        tilesets_path = tile_data_type.tilesets_path(&layer_id),
                        TILE_MATRIX_SET_ID = CustomWebMercatorTMS::TILE_MATRIX_SET_ID
                    ),
                    SELF,
                    JSON,
                )?],
            },
            TileSetItem {
                title: Some(WebMercatorQuadTMS::TILE_MATRIX_SET_TITLE.to_string()),
                data_type: tile_data_type.data_type(),
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
                        "{tilesets_path}/{TILE_MATRIX_SET_ID}",
                        tilesets_path = tile_data_type.tilesets_path(&layer_id),
                        TILE_MATRIX_SET_ID = WebMercatorQuadTMS::TILE_MATRIX_SET_ID
                    ),
                    SELF,
                    JSON,
                )?],
            },
        ],
        links: vec![create_link(
            &tile_data_type.tilesets_path(&layer_id),
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
    tileset::<C>(session, app_ctx, path, TileDataType::Map).await
}

/// OGC API Collection Coverage Tileset Metadata
///
/// Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/20-057/20-057.html)
/// and [OGC API - Coverages - Part 1: Core](https://docs.ogc.org/DRAFTS/19-087.html).
#[utoipa::path(
	tag = "OGC API",
	get,
	path = "/{dataConnectorId}/{layerId}/collections/{layerId}/coverage/tiles/{tileMatrixSetId}",
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
pub async fn collection_coverage_tileset<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId, LayerId, TileMatrixSetId)>,
) -> OgcApiResult<web::Json<TileSet>> {
    tileset::<C>(session, app_ctx, path, TileDataType::Coverage).await
}

async fn tileset<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId, LayerId, TileMatrixSetId)>,
    tile_data_type: TileDataType,
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
    let tileset_path = format!(
        "{tilesets_path}/{tile_matrix_set_id}",
        tilesets_path = tile_data_type.tilesets_path(&layer_id)
    );
    let media_type = tile_data_type.media_type();

    Ok(web::Json(TileSet {
        title: Some("Tileset Metadata".to_string()),
        data_type: tile_data_type.data_type(),
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
        media_types: vec![media_type.to_string()],
        links: vec![
            create_link(&tileset_path, SELF, JSON)?,
            create_link(
                &format!("tileMatrixSets/{tile_matrix_set_id}"),
                TILING_SCHEME,
                JSON,
            )?,
            {
                let mut link = create_link(&tileset_path, ITEM, media_type)?.templated(true);
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

#[derive(Debug, serde::Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct CoverageTileQueryParams {
    /// Either a date-time or an interval, half-bounded or bounded. Date and time expressions adhere to RFC 3339. Half-bounded intervals use double dots (`..`).
    #[param(value_type = String, example = "2018-02-12T23:20:50Z")]
    #[serde(default)]
    #[serde(deserialize_with = "parse_datetime_option")]
    pub datetime: Option<OgcDatetime>,

    /// Comma-separated list of the fields (bands) to return, in this order. If omitted, all fields are returned. Must not be combined with `exclude-properties`.
    #[param(example = "red,green,blue")]
    pub properties: Option<String>,

    /// Comma-separated list of the fields (bands) not to return. An empty value returns all fields. Must not be combined with `properties`.
    #[serde(rename = "exclude-properties")]
    #[param(rename = "exclude-properties", example = "alpha")]
    pub exclude_properties: Option<String>,
}

/// Path of a tile: data connector, layer, collection, tile matrix set, tile matrix, tile row and tile column
type TilePath = web::Path<(
    DataProviderId,
    LayerId,
    LayerId,
    TileMatrixSetId,
    u8,
    u32,
    u32,
)>;

impl TileQuery {
    fn from_params(path: TilePath, datetime: Option<OgcDatetime>) -> OgcApiResult<Self> {
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
            time_interval: query_time_from_datetime(datetime)?,
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
    path: TilePath,
    query: web::Query<TileQueryParams>,
) -> OgcApiResult<HttpResponse> {
    let query = TileQuery::from_params(path, query.into_inner().datetime)?;

    let ctx = app_ctx.session_context(session);

    let connection_closed_handler = tile_connection_closed_handler(&req);

    let (image_bytes, cache_hint, computation_id) = run_query(
        &ctx,
        connection_closed_handler,
        render_tile::<C>(ctx.clone(), query),
    )
    .await?;

    Ok(HttpResponse::Ok()
        .content_type(mime::IMAGE_PNG)
        .append_header(cache_hint.cache_control_header())
        .append_header(computation_id)
        .body(image_bytes))
}

/// OGC API Coverage Tile
///
/// Returns the raw values of the tile as `GeoTIFF`, with one band per selected field.
/// The tile covers the exact spatial extent of the tile and has the tile size in pixels as its number of cells.
///
/// Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/20-057/20-057.html)
/// and [OGC API - Coverages - Part 1: Core](https://docs.ogc.org/DRAFTS/19-087.html) (field selection and tiled coverage data).
#[utoipa::path(
	tag = "OGC API",
	get,
	path = "/{dataConnectorId}/{layerId}/collections/{layerId}/coverage/tiles/{tileMatrixSetId}/{tileMatrix}/{tileRow}/{tileCol}",
	responses(
		(status = 200, response = crate::api::model::responses::GeoTiffResponse),
		(status = 400, description = "Invalid tile coordinates, datetime or field selection"),
		(status = 404, description = "Collection or tile matrix set not found")
	),
	params(
		("dataConnectorId" = DataProviderId, description = "ID of the data connector"),
        ("layerId" = LayerId, description = "ID of the layer, which is used as collection ID"),
		("tileMatrixSetId" = TileMatrixSetId, description = "Tile matrix set identifier"),
		("tileMatrix" = u8, description = "Tile matrix level"),
		("tileRow" = u64, description = "Tile row"),
		("tileCol" = u64, description = "Tile column"),
		CoverageTileQueryParams
	),
	security(
		("session_token" = [])
	)
)]
pub async fn coverage_tile<C: ApplicationContext>(
    req: HttpRequest,
    session: C::Session,
    app_ctx: web::Data<C>,
    path: TilePath,
    query: web::Query<CoverageTileQueryParams>,
) -> OgcApiResult<HttpResponse> {
    let CoverageTileQueryParams {
        datetime,
        properties,
        exclude_properties,
    } = query.into_inner();
    let field_selection =
        FieldSelection::from_query_params(properties.as_deref(), exclude_properties.as_deref())?;
    let query = TileQuery::from_params(path, datetime)?;

    let ctx = app_ctx.session_context(session);

    let connection_closed_handler = tile_connection_closed_handler(&req);

    let (tiff_bytes, cache_hint, computation_id) = run_query(
        &ctx,
        connection_closed_handler,
        render_coverage_tile::<C>(ctx.clone(), query, field_selection),
    )
    .await?;

    Ok(HttpResponse::Ok()
        .content_type(GEOTIFF)
        .append_header(cache_hint.cache_control_header())
        .append_header(computation_id)
        .body(tiff_bytes))
}

fn tile_connection_closed_handler(req: &HttpRequest) -> futures::future::BoxFuture<'static, ()> {
    connection_closed(
        req,
        config::get_config_element::<config::Ogc>()
            .ok()
            .and_then(|cfg| cfg.tiles)
            .and_then(|tiles| tiles.request_timeout_seconds)
            .map(Duration::from_secs),
    )
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
            session_and_3857_layer_id, session_and_4326_layer_id, session_and_4326_rgb_layer_id,
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

    /// A `GeoTIFF` read back with GDAL
    struct GeoTiffTile {
        width: usize,
        height: usize,
        epsg_code: i32,
        geo_transform: [f64; 6],
        /// band descriptions, i.e., the field names
        band_names: Vec<String>,
        /// pixel values per band, row by row
        bands: Vec<Vec<f64>>,
        /// number of valid (unmasked) pixels per band
        valid_pixels: Vec<usize>,
    }

    fn read_geotiff(bytes: &[u8]) -> GeoTiffTile {
        let path = format!("/vsimem/{}.tiff", uuid::Uuid::new_v4());
        gdal::vsi::create_mem_file(&path, bytes.to_vec())
            .expect("in-memory file should be created");

        let tile = {
            let dataset = gdal::Dataset::open(&path).expect("response should be a GeoTIFF");
            let (width, height) = dataset.raster_size();

            let mut band_names = Vec::new();
            let mut bands = Vec::new();
            let mut valid_pixels = Vec::new();
            for band_index in 1..=dataset.raster_count() {
                let band = dataset.rasterband(band_index).expect("band should exist");
                band_names.push(
                    gdal::Metadata::description(&band).expect("band should have a description"),
                );
                bands.push(
                    band.read_as::<f64>((0, 0), (width, height), (width, height), None)
                        .expect("band should be readable")
                        .data()
                        .to_vec(),
                );
                valid_pixels.push(
                    band.open_mask_band()
                        .expect("mask band should exist")
                        .read_as::<u8>((0, 0), (width, height), (width, height), None)
                        .expect("mask band should be readable")
                        .data()
                        .iter()
                        .filter(|&&mask| mask > 0)
                        .count(),
                );
            }

            GeoTiffTile {
                width,
                height,
                epsg_code: dataset
                    .spatial_ref()
                    .expect("GeoTIFF should have a spatial reference")
                    .auth_code()
                    .expect("spatial reference should have an EPSG code"),
                geo_transform: dataset
                    .geo_transform()
                    .expect("GeoTIFF should have a geo transform"),
                band_names,
                bands,
                valid_pixels,
            }
        };

        gdal::vsi::unlink_mem_file(&path).expect("in-memory file should be removed");

        tile
    }

    async fn request_coverage_tile(
        app_ctx: PostgresContext<NoTls>,
        session_id: crate::contexts::SessionId,
        uri: &str,
    ) -> (
        actix_http::StatusCode,
        Option<String>,
        actix_web::web::Bytes,
    ) {
        let req = actix_web::test::TestRequest::get()
            .uri(uri)
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let content_type = res
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let body = actix_web::test::read_body(res).await;

        (status, content_type, body)
    }

    #[ge_context::test]
    async fn it_lists_coverage_tilesets_for_collection(app_ctx: PostgresContext<NoTls>) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/coverage/tiles"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        let tilesets = body["tilesets"]
            .as_array()
            .expect("tilesets should be a list");
        assert_eq!(tilesets.len(), 3);

        for (tileset, tile_matrix_set_id) in tilesets.iter().zip([
            CustomNativeTMS::TILE_MATRIX_SET_ID,
            CustomWebMercatorTMS::TILE_MATRIX_SET_ID,
            WebMercatorQuadTMS::TILE_MATRIX_SET_ID,
        ]) {
            assert_eq!(tileset["dataType"], "coverage");
            assert_eq!(
                tileset["links"],
                serde_json::json!([{
                    "href": format!(
                        "{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/coverage/tiles/{tile_matrix_set_id}"
                    ),
                    "rel": "self",
                    "type": "application/json"
                }])
            );
        }

        assert_eq!(
            body["links"],
            serde_json::json!([{
                "href": format!(
                    "{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/coverage/tiles"
                ),
                "rel": "self",
                "type": "application/json"
            }])
        );
    }

    #[ge_context::test]
    async fn it_returns_coverage_tileset_metadata_with_template_link(
        app_ctx: PostgresContext<NoTls>,
    ) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;
        let tile_matrix_set_id = WebMercatorQuadTMS::TILE_MATRIX_SET_ID;

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/coverage/tiles/{tile_matrix_set_id}"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        assert_eq!(body["dataType"], "coverage");
        assert_eq!(body["mediaTypes"], serde_json::json!([GEOTIFF]));
        assert_eq!(
            body["links"],
            serde_json::json!([
                {
                    "href": format!(
                        "{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/coverage/tiles/{tile_matrix_set_id}"
                    ),
                    "rel": "self",
                    "type": "application/json"
                },
                {
                    "href": format!(
                        "{server_url}/api/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/{tile_matrix_set_id}"
                    ),
                    "rel": "http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme",
                    "type": "application/json"
                },
                {
                    "href": format!(
                        "{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/coverage/tiles/{tile_matrix_set_id}/{{tileMatrix}}/{{tileRow}}/{{tileCol}}?datetime={{datetime}}"
                    ),
                    "rel": "item",
                    "type": GEOTIFF,
                    "templated": true
                }
            ])
        );
    }

    #[ge_context::test]
    async fn it_renders_web_mercator_quad_coverage_tile_geotiff(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) =
            session_and_ndvi_multi_band_layer_id(&app_ctx).await;

        let (status, content_type, body) = request_coverage_tile(
            app_ctx,
            session_id,
            &format!(
                "/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/coverage/tiles/WebMercatorQuad/1/0/1?datetime=2014-04-01T00:00:00Z"
            ),
        )
        .await;

        assert_eq!(status, 200, "{:?}", std::str::from_utf8(&body));
        assert_eq!(content_type.as_deref(), Some(GEOTIFF));

        let tile = read_geotiff(&body);

        // tile 1/0/1 of `WebMercatorQuad` is the north-eastern quarter of the world, with 256×256 cells
        assert_eq!((tile.width, tile.height), (256, 256));
        assert_eq!(tile.epsg_code, 3857);
        let cell_size = 78_271.516_964_020_4;
        let expected_geo_transform = [0.0, cell_size, 0.0, 20_037_508.342_789_2, 0.0, -cell_size];
        for (actual, expected) in tile.geo_transform.iter().zip(expected_geo_transform) {
            assert!(
                float_cmp::approx_eq!(f64, *actual, expected, epsilon = 1e-6),
                "geo transform {:?} should be {expected_geo_transform:?}",
                tile.geo_transform
            );
        }

        assert_eq!(tile.bands.len(), 1);
        assert!(tile.valid_pixels[0] > 0, "tile should contain data");
        assert!(
            tile.bands[0]
                .iter()
                .all(|value| (0.0..=255.0).contains(value)),
            "NDVI values should be bytes"
        );
    }

    /// Coverage tiles must have exactly the tile size in cells and cover the exact extent of the tile,
    /// also at zoom levels much finer than the data, where floating point errors in the tile bounds
    /// could add a row or column of cells.
    #[ge_context::test]
    async fn it_renders_web_mercator_quad_coverage_tiles_at_multiple_zoom_levels(
        app_ctx: PostgresContext<NoTls>,
    ) {
        const ORIGIN: [f64; 2] = [-20_037_508.342_789_2, 20_037_508.342_789_2];
        const CELL_SIZE_AT_ZOOM_0: f64 = 156_543.033_928_041;

        let (session_id, data_connector_id, layer_id) =
            session_and_ndvi_multi_band_layer_id(&app_ctx).await;

        for (zoom, row, col) in [
            (0u8, 0u32, 0u32),
            (3, 2, 5),
            (14, 6000, 6000),
            (20, 300_000, 300_000),
        ] {
            let (status, _, body) = request_coverage_tile(
                app_ctx.clone(),
                session_id,
                &format!(
                    "/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/coverage/tiles/WebMercatorQuad/{zoom}/{row}/{col}?datetime=2014-04-01T00:00:00Z"
                ),
            )
            .await;

            assert_eq!(
                status,
                200,
                "tile {zoom}/{row}/{col}: {:?}",
                std::str::from_utf8(&body)
            );

            let tile = read_geotiff(&body);

            assert_eq!(
                (tile.width, tile.height),
                (256, 256),
                "tile {zoom}/{row}/{col} should have 256×256 cells"
            );

            let cell_size = CELL_SIZE_AT_ZOOM_0 / f64::from(1u32 << zoom);
            let expected_geo_transform = [
                ORIGIN[0] + f64::from(col) * 256. * cell_size,
                cell_size,
                0.0,
                ORIGIN[1] - f64::from(row) * 256. * cell_size,
                0.0,
                -cell_size,
            ];
            for (actual, expected) in tile.geo_transform.iter().zip(expected_geo_transform) {
                assert!(
                    float_cmp::approx_eq!(f64, *actual, expected, epsilon = 1e-6),
                    "tile {zoom}/{row}/{col}: geo transform {:?} should be {expected_geo_transform:?}",
                    tile.geo_transform
                );
            }
        }
    }

    #[ge_context::test]
    async fn it_renders_native_coverage_tile_on_the_tile_matrix_grid(
        app_ctx: PostgresContext<NoTls>,
    ) {
        let (session_id, data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;
        let tile_matrix_set_id = CustomNativeTMS::TILE_MATRIX_SET_ID;
        let (tile_matrix, tile_row, tile_col) = (2, 1, 4);

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/{tile_matrix_set_id}"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let tile_matrix_set = read_body_json(send_test_request(req, app_ctx.clone()).await).await;
        let tile_matrix_definition = tile_matrix_set["tileMatrices"]
            .as_array()
            .expect("tile matrices should be a list")
            .iter()
            .find(|matrix| {
                matrix["id"].as_str().and_then(|id| id.parse::<u8>().ok()) == Some(tile_matrix)
            })
            .expect("tile matrix should exist")
            .clone();

        let (status, content_type, body) = request_coverage_tile(
            app_ctx,
            session_id,
            &format!(
                "/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/coverage/tiles/{tile_matrix_set_id}/{tile_matrix}/{tile_row}/{tile_col}?datetime=2014-04-01T00:00:00Z"
            ),
        )
        .await;

        assert_eq!(status, 200, "{:?}", std::str::from_utf8(&body));
        assert_eq!(content_type.as_deref(), Some(GEOTIFF));

        let tile = read_geotiff(&body);

        let tile_width = tile_matrix_definition["tileWidth"].as_u64().unwrap() as usize;
        let tile_height = tile_matrix_definition["tileHeight"].as_u64().unwrap() as usize;
        let cell_size = tile_matrix_definition["cellSize"].as_f64().unwrap();
        let origin_x = tile_matrix_definition["pointOfOrigin"][0].as_f64().unwrap();
        let origin_y = tile_matrix_definition["pointOfOrigin"][1].as_f64().unwrap();

        assert_eq!((tile.width, tile.height), (tile_width, tile_height));
        assert_eq!(tile.epsg_code, 4326);
        let expected_geo_transform = [
            origin_x + f64::from(tile_col) * tile_width as f64 * cell_size,
            cell_size,
            0.0,
            origin_y - f64::from(tile_row) * tile_height as f64 * cell_size,
            0.0,
            -cell_size,
        ];
        for (actual, expected) in tile.geo_transform.iter().zip(expected_geo_transform) {
            assert!(
                float_cmp::approx_eq!(f64, *actual, expected, epsilon = 1e-9),
                "geo transform {:?} should be {expected_geo_transform:?}",
                tile.geo_transform
            );
        }

        assert_eq!(tile.bands.len(), 1);
        assert!(tile.valid_pixels[0] > 0, "tile should contain data");
    }

    #[ge_context::test]
    async fn it_selects_fields_of_coverage_tiles(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) =
            session_and_4326_rgb_layer_id(&app_ctx).await;

        let tile_uri = |query: &str| {
            format!(
                "/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/coverage/tiles/{TILE_MATRIX_SET_ID}/0/0/0?datetime=2014-04-01T00:00:00Z{query}",
                TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
            )
        };

        // bands of the layer: blue, green, red
        let (status, _, body) =
            request_coverage_tile(app_ctx.clone(), session_id, &tile_uri("")).await;
        assert_eq!(status, 200, "{:?}", std::str::from_utf8(&body));
        let all_bands = read_geotiff(&body);
        assert_eq!(all_bands.band_names, ["blue", "green", "red"]);
        assert!(all_bands.valid_pixels.iter().all(|&valid| valid > 0));
        assert!(
            all_bands.bands[0] != all_bands.bands[1]
                && all_bands.bands[0] != all_bands.bands[2]
                && all_bands.bands[1] != all_bands.bands[2],
            "the bands should differ for the test to be meaningful"
        );

        let (status, _, body) = request_coverage_tile(
            app_ctx.clone(),
            session_id,
            &tile_uri("&properties=red,blue"),
        )
        .await;
        assert_eq!(status, 200, "{:?}", std::str::from_utf8(&body));
        let selected = read_geotiff(&body);
        assert_eq!(selected.band_names, ["red", "blue"]);
        assert!(
            selected.bands == [all_bands.bands[2].clone(), all_bands.bands[0].clone()],
            "`properties=red,blue` should return the red and blue bands in this order"
        );

        let (status, _, body) = request_coverage_tile(
            app_ctx.clone(),
            session_id,
            &tile_uri("&exclude-properties=green"),
        )
        .await;
        assert_eq!(status, 200, "{:?}", std::str::from_utf8(&body));
        let excluded = read_geotiff(&body);
        assert_eq!(excluded.band_names, ["blue", "red"]);
        assert!(
            excluded.bands == [all_bands.bands[0].clone(), all_bands.bands[2].clone()],
            "`exclude-properties=green` should return the blue and red bands"
        );

        let (status, _, body) = request_coverage_tile(
            app_ctx.clone(),
            session_id,
            &tile_uri("&exclude-properties="),
        )
        .await;
        assert_eq!(status, 200, "{:?}", std::str::from_utf8(&body));
        assert!(
            read_geotiff(&body).bands == all_bands.bands,
            "`exclude-properties=` should return all bands"
        );

        for invalid_query in [
            "&properties=alpha",
            "&exclude-properties=alpha",
            "&properties=red&exclude-properties=green",
            "&properties=",
            "&properties=red,red",
            "&exclude-properties=blue,green,red",
        ] {
            let (status, _, body) =
                request_coverage_tile(app_ctx.clone(), session_id, &tile_uri(invalid_query)).await;
            assert_eq!(
                status,
                400,
                "{invalid_query}: {:?}",
                std::str::from_utf8(&body)
            );
        }
    }

    #[ge_context::test]
    async fn it_rejects_out_of_bounds_coverage_tiles(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;

        let (status, _, body) = request_coverage_tile(
            app_ctx,
            session_id,
            &format!(
                "/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/coverage/tiles/WebMercatorQuad/1/2/0?datetime=2014-04-01T00:00:00Z"
            ),
        )
        .await;

        assert_eq!(status, 400, "{:?}", std::str::from_utf8(&body));
    }

    #[test]
    fn it_parses_field_selection_query_params() {
        assert_eq!(
            FieldSelection::from_query_params(None, None).unwrap(),
            FieldSelection::All
        );
        assert_eq!(
            FieldSelection::from_query_params(Some("red, blue"), None).unwrap(),
            FieldSelection::Include(vec!["red".to_string(), "blue".to_string()])
        );
        assert_eq!(
            FieldSelection::from_query_params(None, Some("green")).unwrap(),
            FieldSelection::Exclude(vec!["green".to_string()])
        );
        assert_eq!(
            FieldSelection::from_query_params(None, Some("")).unwrap(),
            FieldSelection::Exclude(vec![])
        );
        assert!(matches!(
            FieldSelection::from_query_params(Some("red"), Some("green")),
            Err(OgcApiError::ConflictingFieldSelection)
        ));
    }

    #[test]
    fn it_resolves_field_selection_to_band_order() {
        use geoengine_datatypes::primitives::Measurement;
        use geoengine_operators::engine::{RasterBandDescriptor, RasterBandDescriptors};

        let bands = RasterBandDescriptors::new(
            ["blue", "green", "red"]
                .into_iter()
                .map(|name| RasterBandDescriptor::new(name.to_string(), Measurement::Unitless))
                .collect(),
        )
        .unwrap();

        let band_order = |selection: FieldSelection| selection.band_order(&bands);

        assert_eq!(band_order(FieldSelection::All).unwrap(), vec![0, 1, 2]);
        assert_eq!(
            band_order(FieldSelection::Include(vec![
                "red".to_string(),
                "blue".to_string()
            ]))
            .unwrap(),
            vec![2, 0]
        );
        assert_eq!(
            band_order(FieldSelection::Exclude(vec!["green".to_string()])).unwrap(),
            vec![0, 2]
        );
        assert_eq!(
            band_order(FieldSelection::Exclude(vec![])).unwrap(),
            vec![0, 1, 2]
        );
        assert!(matches!(
            band_order(FieldSelection::Include(vec!["alpha".to_string()])),
            Err(OgcApiError::UnknownField { .. })
        ));
        assert!(matches!(
            band_order(FieldSelection::Include(vec![
                "red".to_string(),
                "red".to_string()
            ])),
            Err(OgcApiError::DuplicateField { .. })
        ));
        assert!(matches!(
            band_order(FieldSelection::Include(vec![])),
            Err(OgcApiError::EmptyFieldSelection)
        ));
        assert!(matches!(
            band_order(FieldSelection::Exclude(vec![
                "blue".to_string(),
                "green".to_string(),
                "red".to_string()
            ])),
            Err(OgcApiError::EmptyFieldSelection)
        ));
    }
}
