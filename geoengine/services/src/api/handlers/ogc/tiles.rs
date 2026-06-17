use std::time::Duration;

use crate::{
    api::{
        handlers::ogc::{
            OgcApiResult,
            error::{self, OgcApiError},
            tms::CUSTOM_TILE_MATRIX_SET_ID,
            util::{
                crs_from_spatial_reference_option, link_creator, load_layer, parse_datetime_option,
                raster_workflow_metadata,
            },
        },
        model::datatypes::{DataProviderId, LayerId},
    },
    config,
    contexts::{ApplicationContext, SessionContext},
    layers::layer::Layer,
    projects::Symbology,
    util::server::{CacheControlHeader, connection_closed},
    workflows::registry::WorkflowRegistry,
};
use actix_web::{HttpRequest, HttpResponse, web};
use geoengine_datatypes::{
    error::BoxedResultExt,
    primitives::{
        AxisAlignedRectangle, BandSelection, RasterQueryRectangle, TimeInstance, TimeInterval,
    },
    raster::{
        GridBoundingBox2D, GridBounds, GridIdx2D, GridShapeAccess, TilingSpatialGridDefinition,
        TilingSpecification,
    },
    util::Identifier,
};
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{
        ExecutionContext, InitializedRasterOperator, RasterResultDescriptor, TypedOperator,
        TypedRasterQueryProcessor, WorkflowOperatorPath,
    },
    util::raster_stream_to_png::raster_stream_to_png_bytes,
};
use ogcapi_types::{
    common::{
        Datetime as OgcDatetime, IntervalDatetime,
        link_rel::{ITEM, SELF, TILING_SCHEME},
        media_type::{JSON, PNG},
    },
    tiles::{
        AccessConstraints, BoundingBox2D, DataType, TileMatrixSetId, TileSet, TileSetItem,
        TileSets, TilesCrs,
    },
};
use utoipa::IntoParams;
use uuid::Uuid;

const TILESET_TITLE: &str = "Tileset Metadata";
const TILESET_LIST_TITLE: &str = "Tiles in GeoEngine custom TMS";

/// OGC API Collection Tilesets List
///
/// Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).
#[utoipa::path(
	tag = "OGC API",
	get,
	path = "/ogc/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles",
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

    let descriptor = raster_workflow_metadata::<C::SessionContext>(
        layer.workflow.clone(),
        ctx.execution_context()?,
    )
    .await?;

    let create_link = link_creator(data_connector_id, layer_id.clone());

    Ok(web::Json(TileSets {
        tilesets: vec![TileSetItem {
            title: Some(TILESET_LIST_TITLE.to_string()),
            data_type: DataType::Map,
            // tile_matrix_set_id: CUSTOM_TILE_MATRIX_SET_ID.to_string(),
            crs: TilesCrs::Simple(crs_from_spatial_reference_option(
                descriptor.spatial_reference,
            )?),
            tile_matrix_set_uri: None,
            links: vec![create_link(
                &format!("collections/{layer_id}/map/tiles/{CUSTOM_TILE_MATRIX_SET_ID}"),
                SELF,
                JSON,
            )?],
        }],
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
	path = "/ogc/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles/{tileMatrixSetId}",
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
    ensure_matching_tile_matrix_set(&tile_matrix_set_id)?;

    let ctx = app_ctx.session_context(session);
    let layer = load_layer::<C>(&ctx, data_connector_id, layer_id.clone()).await?;
    let descriptor =
        raster_workflow_metadata::<C::SessionContext>(layer.workflow, ctx.execution_context()?)
            .await?;

    let create_link = link_creator(data_connector_id, layer_id.clone());
    let crs = TilesCrs::Simple(crs_from_spatial_reference_option(
        descriptor.spatial_reference,
    )?);
    let spatial_bounds = descriptor.spatial_bounds();

    Ok(web::Json(TileSet {
        title: Some(TILESET_TITLE.to_string()),
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

struct TileQuery {
    data_connector_id: DataProviderId,
    layer_id: LayerId,

    time_interval: TimeInterval,

    // tile_matrix_set_id: TileMatrixSetId,
    tile_matrix: u8,
    tile_row: u32,
    tile_col: u32,
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
        ensure_matching_tile_matrix_set(&tile_matrix_set_id)?;

        Ok(Self {
            data_connector_id,
            layer_id,
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
	path = "/ogc/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles/{tileMatrixSetId}/{tileMatrix}/{tileRow}/{tileCol}",
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
    let execution_context = ctx.execution_context()?;

    let layer = load_layer::<C>(&ctx, query.data_connector_id, query.layer_id.clone()).await?;
    let mut initialized_operator =
        get_initialized_raster_operator::<C::SessionContext>(&layer, &execution_context).await?;
    let tiling_specification = execution_context.tiling_specification();

    let max_zoom_level = calculate_max_zoom_level(
        initialized_operator.result_descriptor(),
        &tiling_specification,
    );
    let multiple_of_resolution =
        2u32.pow(max_zoom_level.saturating_sub(u32::from(query.tile_matrix)));
    dbg!(query.tile_matrix, max_zoom_level, multiple_of_resolution);

    let multiple_of_resolution = 1; // TODO: implement in TMS first

    #[cfg(debug_assertions)]
    let original_result_descriptor = initialized_operator.result_descriptor().clone();

    initialized_operator = raster_operator_in_fitting_resolution::<C::SessionContext>(
        initialized_operator,
        &execution_context,
        multiple_of_resolution,
    )
    .await?;

    #[cfg(debug_assertions)]
    assert_multiple_of_original_resolution(
        &original_result_descriptor,
        initialized_operator.result_descriptor(),
    );

    let (tile_width, tile_height) = (
        u32::try_from(tiling_specification.grid_shape().x()).unwrap_or(u32::MAX),
        u32::try_from(tiling_specification.grid_shape().y()).unwrap_or(u32::MAX),
    );

    let query_rect = calculate_query_rectangle(
        &query,
        initialized_operator.result_descriptor(),
        tiling_specification,
    )?;

    let (processor, query_ctx) =
        create_query_processor_and_query_context(&layer, &initialized_operator, &ctx).await?;

    let connection_closed_handler = connection_closed(
        &req,
        config::get_config_element::<config::Ogc>()
            .ok()
            .and_then(|cfg| cfg.tiles)
            .and_then(|tiles| tiles.request_timeout_seconds)
            .map(Duration::from_secs),
    );

    let (image_bytes, cache_hint) = call_on_generic_raster_processor!(
        processor,
        p => raster_stream_to_png_bytes(
            p,
            query_rect,
            query_ctx,
            tile_width,
            tile_height,
            Some(query.time_interval),
            layer.symbology.and_then(Symbology::into_raster_symbology).map(|symbology| symbology.raster_colorizer),
            connection_closed_handler,
        ).await
    )
    .map_err(crate::error::Error::from)?;

    Ok(HttpResponse::Ok()
        .content_type(mime::IMAGE_PNG)
        .append_header(cache_hint.cache_control_header())
        .body(image_bytes))
}

fn calculate_query_rectangle(
    query: &TileQuery,
    result_descriptor: &RasterResultDescriptor,
    tiling_specification: TilingSpecification,
) -> OgcApiResult<RasterQueryRectangle> {
    let tiling_spatial_grid_definition = result_descriptor
        .spatial_grid
        .tiling_grid_definition(tiling_specification);

    Ok(RasterQueryRectangle::new(
        tile_grid_bbox(
            query.tile_matrix,
            &tiling_spatial_grid_definition,
            query.tile_row,
            query.tile_col,
        )?,
        query.time_interval,
        BandSelection::first(),
    ))
}

async fn raster_operator_in_fitting_resolution<C: SessionContext>(
    initialized_operator: Box<dyn InitializedRasterOperator>,
    execution_context: &C::ExecutionContext,
    multiple_of_resolution: u32,
) -> OgcApiResult<Box<dyn InitializedRasterOperator>> {
    if multiple_of_resolution <= 1 {
        return Ok(initialized_operator);
    }

    let new_resolution = initialized_operator
        .result_descriptor()
        .spatial_grid
        .spatial_resolution()
        * f64::from(multiple_of_resolution);

    initialized_operator
        .optimize_and_reinitialize(new_resolution, execution_context)
        .await
        .boxed_context(error::InitializingProcessingGraph)
}

#[cfg(debug_assertions)]
fn assert_multiple_of_original_resolution(
    original_result_descriptor: &RasterResultDescriptor,
    new_result_descriptor: &RasterResultDescriptor,
) {
    let original_pixel_resolution = original_result_descriptor
        .spatial_grid_descriptor()
        .grid_shape();
    let new_pixel_resolution = new_result_descriptor.spatial_grid_descriptor().grid_shape();

    debug_assert!(
        original_pixel_resolution
            .x()
            .is_multiple_of(new_pixel_resolution.x()),
        "New resolution is not a multiple of the original resolution: original: {original_pixel_resolution:?}, new: {new_pixel_resolution:?}",
    );
    debug_assert!(
        original_pixel_resolution
            .y()
            .is_multiple_of(new_pixel_resolution.y()),
        "New resolution is not a multiple of the original resolution: original: {original_pixel_resolution:?}, new: {new_pixel_resolution:?}",
    );
}

fn calculate_max_zoom_level(
    result_descriptor: &RasterResultDescriptor,
    tile_size: &TilingSpecification,
) -> u32 {
    let [x_size, y_size] = result_descriptor
        .spatial_grid_descriptor()
        .grid_shape()
        .into_inner()
        .map(|x| x as f64);
    let [tile_size_x, tile_size_y] = tile_size.tile_size_in_pixels.into_inner().map(|x| x as f64);

    let max_level_x = f64::log2(x_size / tile_size_x).ceil();
    let max_level_y = f64::log2(y_size / tile_size_y).ceil();

    max_level_x.max(max_level_y) as u32
}

/// Computes the pixel bounds of a tile in the global pixel grid of the tiling scheme.
/// The resolution is determined by `tile_matrix`, which corresponds to the zoom level in a TMS.
/// The origin and orientation of the tile grid is determined by the `tiling_spatial_grid_definition`.
fn tile_grid_bbox(
    tile_matrix: u8,
    tiling_spatial_grid_definition: &TilingSpatialGridDefinition,
    tile_row: u32,
    tile_col: u32,
) -> OgcApiResult<GridBoundingBox2D> {
    let grid_bounds = tiling_spatial_grid_definition.tiling_grid_bounds();
    let tiling_strategy = tiling_spatial_grid_definition.generate_data_tiling_strategy();

    let tile_grid_bounds = tiling_strategy.raster_spatial_query_to_tiling_grid_box(grid_bounds);
    let tile_index =
        tile_grid_bounds.min_index() + GridIdx2D::new([tile_row as isize, tile_col as isize]);

    let min_pixel_index = tiling_strategy.tile_idx_to_global_pixel_idx(tile_index);
    let max_pixel_index = min_pixel_index
        + GridIdx2D::new([
            tiling_strategy.tile_size_in_pixels.x() as isize,
            tiling_strategy.tile_size_in_pixels.y() as isize,
        ])
        - GridIdx2D::new([1, 1]); // inclusive bounds

    GridBoundingBox2D::new(min_pixel_index, max_pixel_index).map_err(|_source| {
        OgcApiError::InvalidTileCoordinates {
            matrix: tile_matrix.to_string(),
            row: tile_row,
            col: tile_col,
        }
    })
}

fn ensure_matching_collection(layer_id: &LayerId, collection_id: LayerId) -> OgcApiResult<()> {
    if *layer_id != collection_id {
        return Err(OgcApiError::CollectionNotFound { collection_id });
    }

    Ok(())
}

fn ensure_matching_tile_matrix_set(tile_matrix_set_id: &TileMatrixSetId) -> OgcApiResult<()> {
    if tile_matrix_set_id != &TileMatrixSetId::Custom(CUSTOM_TILE_MATRIX_SET_ID.to_string()) {
        return Err(OgcApiError::TileMatrixSetNotFound {
            tile_matrix_set_id: tile_matrix_set_id.to_string(),
        });
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
        None => Ok(default_time_from_config()),
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

fn default_time_from_config() -> TimeInterval {
    config::get_config_element::<crate::config::Ogc>()
        .ok()
        .and_then(|ogc| ogc.default_time)
        .map_or_else(
            || {
                geoengine_datatypes::primitives::TimeInterval::new_instant(
                    geoengine_datatypes::primitives::TimeInstance::now(),
                )
                .expect("current system time should be valid")
            },
            |time| time.time_interval(),
        )
}

async fn get_initialized_raster_operator<C: SessionContext>(
    layer: &Layer,
    execution_context: &C::ExecutionContext,
) -> OgcApiResult<Box<dyn InitializedRasterOperator>> {
    let operator = match layer.workflow.operator()? {
        TypedOperator::Raster(operator) => operator,
        TypedOperator::Vector(_) => {
            return Err(OgcApiError::ExpectedRaster {
                found: "vector".to_string(),
            });
        }
        TypedOperator::Plot(_) => {
            return Err(OgcApiError::ExpectedRaster {
                found: "plot".to_string(),
            });
        }
    };

    operator
        .initialize(WorkflowOperatorPath::initialize_root(), execution_context)
        .await
        .boxed_context(error::InitializingProcessingGraph)
}

async fn create_query_processor_and_query_context<C: SessionContext>(
    layer: &Layer,
    initialized_operator: &dyn InitializedRasterOperator,
    ctx: &C,
) -> OgcApiResult<(TypedRasterQueryProcessor, C::QueryContext)> {
    let processing_graph_id = ctx.db().register_workflow(layer.workflow.clone()).await?; // TODO: can we get this without re-registering it?
    let query_ctx = ctx.query_context(*processing_graph_id.uuid(), Uuid::new_v4())?;

    let query_processor = initialized_operator
        .query_processor()
        .boxed_context(error::InitializingProcessingGraph)?;

    Ok((query_processor, query_ctx))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        contexts::{
            ApplicationContext, PostgresContext, PostgresSessionContext, Session, SessionContext,
            SessionId,
        },
        ge_context,
        util::tests::{
            add_ndvi_3857_to_layers, add_ndvi_to_layers, admin_login, read_body_json,
            send_test_request,
        },
    };
    use actix_web::{http::header, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::{
        test_data,
        util::{assert_image_equals, test::save_test_bytes_if_not_exists},
    };
    use ogcapi_types::tiles::{CornerOfOrigin, TileMatrix};
    use pretty_assertions::assert_eq;
    use tokio_postgres::NoTls;

    async fn session_and_4326_layer_id(
        app_ctx: &PostgresContext<NoTls>,
    ) -> (SessionId, DataProviderId, LayerId) {
        let session = admin_login(app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = ctx.session().id();
        let (data_connector_id, layer_id) = add_ndvi_to_layers(app_ctx).await;

        (session_id, data_connector_id.into(), layer_id.into())
    }

    async fn session_and_3857_layer_id(
        app_ctx: &PostgresContext<NoTls>,
    ) -> (SessionId, DataProviderId, LayerId) {
        let session = admin_login(app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = ctx.session().id();
        let (data_connector_id, layer_id) = add_ndvi_3857_to_layers(app_ctx).await;

        (session_id, data_connector_id.into(), layer_id.into())
    }

    #[ge_context::test]
    async fn it_lists_tilesets_for_collection(app_ctx: PostgresContext<NoTls>) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;

        let req = test::TestRequest::get()
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
                        "title": TILESET_LIST_TITLE,
                        "dataType": "map",
                        "crs": "http://www.opengis.net/def/crs/EPSG/0/4326",
                        "links": [
                            {
                                "href": format!(
                                    "{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{CUSTOM_TILE_MATRIX_SET_ID}"
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

        let req = test::TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{CUSTOM_TILE_MATRIX_SET_ID}"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        assert_eq!(
            body,
            serde_json::json!({
                "title": TILESET_TITLE,
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
                            "{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{CUSTOM_TILE_MATRIX_SET_ID}"
                        ),
                        "rel": "self",
                        "type": "application/json"
                    },
                    {
                        "href": format!(
                            "{server_url}/api/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/{CUSTOM_TILE_MATRIX_SET_ID}"
                        ),
                        "rel": "http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme",
                        "type": "application/json"
                    },
                    {
                        "href": format!(
                            "{server_url}/api/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{CUSTOM_TILE_MATRIX_SET_ID}/{{tileMatrix}}/{{tileRow}}/{{tileCol}}?datetime={{datetime}}"
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

        let req = test::TestRequest::get()
			.uri(&format!(
				"/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{CUSTOM_TILE_MATRIX_SET_ID}/0/1/4?datetime=2014-04-01T00:00:00Z"
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

        let file_path = test_data!("ogc/tiles/ndvi_0_1_4.png").to_path_buf();
        save_test_bytes_if_not_exists(&image_bytes, &file_path);

        assert_image_equals(&file_path, &image_bytes);
    }

    #[ge_context::test]
    async fn it_calculates_correct_pixel_bounds_for_4326_tiles(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;

        let ctx = app_ctx.session_context(app_ctx.session_by_id(session_id).await.unwrap());

        let layer = load_layer::<PostgresContext<NoTls>>(&ctx, data_connector_id, layer_id.clone())
            .await
            .unwrap();
        let tiling_specification = ctx.execution_context().unwrap().tiling_specification();
        let result_descriptor = get_initialized_raster_operator::<PostgresSessionContext<NoTls>>(
            &layer,
            &ctx.execution_context().unwrap(),
        )
        .await
        .unwrap()
        .result_descriptor()
        .clone();

        let tile_matrix = TileMatrix {
            id: 0.to_string(),
            title: None,
            description: None,
            keywords: vec![],
            scale_denominator: f64::NAN, // unused
            cell_size: f64::NAN,         // unused
            corner_of_origin: CornerOfOrigin::TopLeft,
            point_of_origin: [-21_890_660.358_935_43, 21_897_016.897_822_894],
            tile_width: 512.try_into().unwrap(),
            tile_height: 512.try_into().unwrap(),
            matrix_width: 8.try_into().unwrap(),
            matrix_height: 4.try_into().unwrap(),
            variable_matrix_widths: vec![],
        };

        let tiling_spatial_grid_definition =
            result_descriptor.tiling_grid_definition(tiling_specification);
        let tiling_strategy = tiling_spatial_grid_definition.generate_data_tiling_strategy();

        let tiling_iterator = tiling_strategy.tile_information_iterator_from_pixel_bounds(
            tiling_spatial_grid_definition.tiling_grid_bounds(),
        );

        let mut tile_row = 0;
        let mut tile_col = 0;
        for tile_info in tiling_iterator {
            assert_eq!(
                tile_grid_bbox(0, &tiling_spatial_grid_definition, tile_row, tile_col,).unwrap(),
                tile_info.global_pixel_bounds(),
                "Tile row {tile_row}, tile col {tile_col}"
            );

            tile_col += 1;
            if tile_col >= tile_matrix.matrix_width.get() as u32 {
                tile_col = 0;
                tile_row += 1;
            }
        }
    }

    #[ge_context::test]
    async fn it_calculates_correct_pixel_bounds_for_3857_tiles(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) = session_and_3857_layer_id(&app_ctx).await;

        let ctx = app_ctx.session_context(app_ctx.session_by_id(session_id).await.unwrap());

        let layer = load_layer::<PostgresContext<NoTls>>(&ctx, data_connector_id, layer_id.clone())
            .await
            .unwrap();
        let tiling_specification = ctx.execution_context().unwrap().tiling_specification();

        let result_descriptor = get_initialized_raster_operator::<PostgresSessionContext<NoTls>>(
            &layer,
            &ctx.execution_context().unwrap(),
        )
        .await
        .unwrap()
        .result_descriptor()
        .clone();

        let tile_matrix = TileMatrix {
            id: 0.to_string(),
            title: None,
            description: None,
            keywords: vec![],
            scale_denominator: f64::NAN, // unused
            cell_size: f64::NAN,         // unused
            corner_of_origin: CornerOfOrigin::TopLeft,
            point_of_origin: [-21_890_660.358_935_43, 21_897_016.897_822_894],
            tile_width: 512.try_into().unwrap(),
            tile_height: 512.try_into().unwrap(),
            matrix_width: 6.try_into().unwrap(),
            matrix_height: 6.try_into().unwrap(),
            variable_matrix_widths: vec![],
        };

        let tiling_spatial_grid_definition =
            result_descriptor.tiling_grid_definition(tiling_specification);
        let tiling_strategy = tiling_spatial_grid_definition.generate_data_tiling_strategy();

        let tiling_iterator = tiling_strategy.tile_information_iterator_from_pixel_bounds(
            tiling_spatial_grid_definition.tiling_grid_bounds(),
        );

        let mut tile_row = 0;
        let mut tile_col = 0;
        for tile_info in tiling_iterator {
            assert_eq!(
                tile_grid_bbox(0, &tiling_spatial_grid_definition, tile_row, tile_col,).unwrap(),
                tile_info.global_pixel_bounds(),
                "Tile row {tile_row}, tile col {tile_col}"
            );

            tile_col += 1;
            if tile_col >= tile_matrix.matrix_width.get() as u32 {
                tile_col = 0;
                tile_row += 1;
            }
        }
    }
}
