use crate::{
    api::handlers::ogc::{
        OgcApiResult,
        error::{self, OgcApiError},
        tms::{CUSTOM_TILE_MATRIX_SET_ID, MAX_TILE_MATRIX_LEVEL},
        util::{
            crs_from_spatial_reference_option, link_creator, parse_datetime_option,
            raster_workflow_metadata,
        },
    },
    config::get_config_element,
    contexts::{ApplicationContext, SessionContext},
    util::server::{CacheControlHeader, connection_closed},
    workflows::{
        registry::WorkflowRegistry,
        workflow::{Workflow, WorkflowId},
    },
};
use actix_web::{HttpRequest, HttpResponse, web};
use geoengine_datatypes::{
    error::BoxedResultExt,
    operations::image::RasterColorizer,
    primitives::{
        AxisAlignedRectangle, BandSelection, Coordinate2D, RasterQueryRectangle,
        SpatialPartition2D, TimeInstance, TimeInterval,
    },
};
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{
        ExecutionContext, RasterResultDescriptor, TypedOperator, TypedRasterQueryProcessor,
        WorkflowOperatorPath,
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
        BoundingBox2D, DataType, GeospatialData, TileMatrixLimits, TileSet, TileSetItem, TileSets,
        TilesCrs,
    },
};
use utoipa::IntoParams;
use uuid::Uuid;

const TILESET_TITLE: &str = "Tileset Metadata";
const TILESET_LIST_TITLE: &str = "Tiles in GeoEngine custom TMS";

#[derive(Debug, serde::Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct TileQueryParams {
    /// Either a date-time or an interval, half-bounded or bounded. Date and time expressions adhere to RFC 3339. Half-bounded intervals use double dots (`..`).
    #[param(value_type = String, example = "2018-02-12T23:20:50Z")]
    #[serde(default)]
    #[serde(deserialize_with = "parse_datetime_option")]
    pub datetime: Option<OgcDatetime>,
}

/// OGC API Collection Tilesets List
///
/// Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).
#[utoipa::path(
	tag = "OGC API",
	get,
	path = "/ogc/{processingGraphId}/collections/{collectionId}/map/tiles",
	responses(
		(status = 200, description = "OK", body = TileSets),
		(status = 404, description = "Collection not found")
	),
	params(
		("processingGraphId" = WorkflowId, description = "ID of the processing graph, which is used as collection ID"),
		("collectionId" = WorkflowId, description = "Collection identifier")
	),
	security(
		("session_token" = [])
	)
)]
pub async fn collection_tilesets<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(WorkflowId, WorkflowId)>,
) -> OgcApiResult<web::Json<TileSets>> {
    let (processing_graph_id, collection_id) = path.into_inner();

    ensure_matching_collection(processing_graph_id, collection_id)?;

    let ctx = app_ctx.session_context(session);
    let processing_graph = ctx.db().load_workflow(&processing_graph_id).await?;
    let descriptor =
        raster_workflow_metadata::<C::SessionContext>(processing_graph, ctx.execution_context()?)
            .await?;

    let create_link = link_creator(processing_graph_id);

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
                &format!("collections/{collection_id}/map/tiles/{CUSTOM_TILE_MATRIX_SET_ID}"),
                SELF,
                JSON,
            )?],
        }],
        links: vec![create_link(
            &format!("collections/{collection_id}/map/tiles"),
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
	path = "/ogc/{processingGraphId}/collections/{collectionId}/map/tiles/{tileMatrixSetId}",
	responses(
		(status = 200, description = "OK", body = TileSet),
		(status = 404, description = "Collection or tile matrix set not found")
	),
	params(
		("processingGraphId" = WorkflowId, description = "ID of the processing graph, which is used as collection ID"),
		("collectionId" = WorkflowId, description = "Collection identifier"),
		("tileMatrixSetId" = String, description = "Tile matrix set identifier")
	),
	security(
		("session_token" = [])
	)
)]
pub async fn collection_tileset<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(WorkflowId, WorkflowId, String)>,
) -> OgcApiResult<web::Json<TileSet>> {
    let (processing_graph_id, collection_id, tile_matrix_set_id) = path.into_inner();

    ensure_matching_collection(processing_graph_id, collection_id)?;
    ensure_matching_tile_matrix_set(tile_matrix_set_id.as_str())?;

    let ctx = app_ctx.session_context(session);
    let processing_graph = ctx.db().load_workflow(&processing_graph_id).await?;
    let descriptor =
        raster_workflow_metadata::<C::SessionContext>(processing_graph, ctx.execution_context()?)
            .await?;

    let create_link = link_creator(processing_graph_id);
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
        // tile_matrix_set_limits: vec![
        //     // TileMatrixLimits {
        //     //     tile_matrix: tile_matrix_set_id.clone(),
        //     //     min_tile_row: 0,
        //     //     max_tile_row: MAX_TILE_MATRIX_LEVEL.into(),
        //     //     min_tile_col: 0,
        //     //     max_tile_col: MAX_TILE_MATRIX_LEVEL.into(),
        //     // }
        // ],
        tile_matrix_set_limits: (0..=MAX_TILE_MATRIX_LEVEL)
            .map(|tile_matrix| {
                let max = 2_u64.pow(u32::from(tile_matrix));
                TileMatrixLimits {
                    tile_matrix: tile_matrix.to_string(),
                    min_tile_row: 0,
                    max_tile_row: max,
                    min_tile_col: 0,
                    max_tile_col: max,
                }
            })
            .collect(),
        crs: crs.clone(),
        epoch: None,
        layers: vec![GeospatialData {
            id: processing_graph_id.to_string(),
            title: None,
            description: None,
            keywords: vec![],
            data_type: DataType::Map,
            geometry_dimension: None,
            feature_type: None,
            attribution: None,
            license: None,
            point_of_contact: None,
            publisher: None,
            theme: None,
            crs: None,
            epoch: None,
            min_scale_denominator: None,
            max_scale_denominator: None,
            min_cell_size: None,
            max_cell_size: None,
            max_tile_matrix: Some(MAX_TILE_MATRIX_LEVEL.to_string()),
            min_tile_matrix: Some(0.to_string()), // TODO: add lower limits if data is expensive to process
            bounding_box: None,
            created: None,
            updated: None,
            style: None,
            geo_data_classes: vec![],
            properties_schema: None,
            links: vec![],
        }],
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
        access_constraints: None,
        // access_constraints: Some(AccessConstraints::Restricted), // TODO: re-iterate about this
        version: None,
        created: None,
        updated: None,
        point_of_contact: None,
        media_types: vec![PNG.to_string()],
        links: vec![
            create_link(
                &format!("collections/{collection_id}/map/tiles/{tile_matrix_set_id}"),
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
                    &format!("collections/{collection_id}/map/tiles/{tile_matrix_set_id}"),
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

/// OGC API Tile
///
/// Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).
#[utoipa::path(
	tag = "OGC API",
	get,
	path = "/ogc/{processingGraphId}/collections/{collectionId}/map/tiles/{tileMatrixSetId}/{tileMatrix}/{tileRow}/{tileCol}",
	responses(
		(status = 200, response = crate::api::model::responses::PngResponse),
		(status = 400, description = "Invalid tile coordinates or datetime"),
		(status = 404, description = "Collection or tile matrix set not found")
	),
	params(
		("processingGraphId" = WorkflowId, description = "ID of the processing graph, which is used as collection ID"),
		("collectionId" = WorkflowId, description = "Collection identifier"),
		("tileMatrixSetId" = String, description = "Tile matrix set identifier"),
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
    path: web::Path<(WorkflowId, WorkflowId, String, u8, u64, u64)>,
    query: web::Query<TileQueryParams>,
) -> OgcApiResult<HttpResponse> {
    let (processing_graph_id, collection_id, tile_matrix_set_id, tile_matrix, tile_row, tile_col) =
        path.into_inner();

    ensure_matching_collection(processing_graph_id, collection_id)?;
    ensure_matching_tile_matrix_set(tile_matrix_set_id.as_str())?;

    let query_time = query_time_from_datetime(query.into_inner().datetime)?;

    let ctx = app_ctx.session_context(session);
    let processing_graph = ctx.db().load_workflow(&processing_graph_id).await?;
    let tiling_specification = ctx.execution_context()?.tiling_specification();
    let tile_size = tiling_specification.tile_size_in_pixels;
    let tile_width = u32::try_from(tile_size.x()).unwrap_or(u32::MAX);
    let tile_height = u32::try_from(tile_size.y()).unwrap_or(u32::MAX);
    let execution_context = ctx.execution_context()?;

    let (processor, descriptor) = raster_query_processor_and_descriptor::<C::SessionContext>(
        processing_graph,
        execution_context,
    )
    .await?;

    let tile_bounds = tile_spatial_bounds(&descriptor, tile_matrix, tile_row, tile_col)?;

    let query_grid = descriptor
        .spatial_grid
        .tiling_grid_definition(tiling_specification)
        .tiling_spatial_grid_definition()
        .spatial_bounds_to_compatible_spatial_grid(tile_bounds);

    let query_rect =
        RasterQueryRectangle::new(query_grid.grid_bounds(), query_time, BandSelection::first());

    let query_ctx = ctx.query_context(processing_graph_id.0, Uuid::new_v4())?;

    let conn_closed = connection_closed(&req, None);

    let (image_bytes, cache_hint) = call_on_generic_raster_processor!(
        processor,
        p => raster_stream_to_png_bytes(
            p,
            query_rect,
            query_ctx,
            tile_width,
            tile_height,
            Some(query_time),
            None::<RasterColorizer>,
            conn_closed
        ).await
    )
    .map_err(crate::error::Error::from)?;

    Ok(HttpResponse::Ok()
        .content_type(mime::IMAGE_PNG)
        .append_header(cache_hint.cache_control_header())
        .body(image_bytes))
}

fn ensure_matching_collection(
    processing_graph_id: WorkflowId,
    collection_id: WorkflowId,
) -> OgcApiResult<()> {
    if processing_graph_id != collection_id {
        return Err(OgcApiError::CollectionNotFound { collection_id });
    }

    Ok(())
}

fn ensure_matching_tile_matrix_set(tile_matrix_set_id: &str) -> OgcApiResult<()> {
    if tile_matrix_set_id != CUSTOM_TILE_MATRIX_SET_ID {
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
    get_config_element::<crate::config::Ogc>()
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

fn tile_spatial_bounds(
    descriptor: &RasterResultDescriptor,
    tile_matrix: u8,
    tile_row: u64,
    tile_col: u64,
) -> OgcApiResult<SpatialPartition2D> {
    if tile_matrix > MAX_TILE_MATRIX_LEVEL {
        return Err(OgcApiError::InvalidTileCoordinates {
            matrix: tile_matrix,
            row: tile_row,
            col: tile_col,
        });
    }

    let matrix_size = 1_u64 << tile_matrix;

    if tile_row >= matrix_size || tile_col >= matrix_size {
        return Err(OgcApiError::InvalidTileCoordinates {
            matrix: tile_matrix,
            row: tile_row,
            col: tile_col,
        });
    }

    let spatial_bounds = descriptor.spatial_bounds();
    let upper_left = spatial_bounds.upper_left();

    let tile_span_x = spatial_bounds.size_x() / matrix_size as f64;
    let tile_span_y = spatial_bounds.size_y() / matrix_size as f64;

    let tile_upper_left = Coordinate2D::new(
        upper_left.x + tile_col as f64 * tile_span_x,
        upper_left.y - tile_row as f64 * tile_span_y,
    );
    let tile_lower_right = Coordinate2D::new(
        tile_upper_left.x + tile_span_x,
        tile_upper_left.y - tile_span_y,
    );

    SpatialPartition2D::new(tile_upper_left, tile_lower_right)
        .boxed_context(error::InvalidBoundingBox)
}

async fn raster_query_processor_and_descriptor<C: SessionContext>(
    processing_graph: Workflow,
    execution_context: C::ExecutionContext,
) -> OgcApiResult<(TypedRasterQueryProcessor, RasterResultDescriptor)> {
    let operator = match processing_graph.operator()? {
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

    let initialized_operator = operator
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await
        .boxed_context(error::InitializingProcessingGraph)?;

    let result_descriptor = initialized_operator.result_descriptor().clone();
    let query_processor = initialized_operator
        .query_processor()
        .boxed_context(error::InitializingProcessingGraph)?;

    Ok((query_processor, result_descriptor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{
        ApplicationContext, PostgresContext, Session, SessionContext, SessionId,
    };
    use crate::ge_context;
    use crate::util::tests::{
        admin_login, read_body_json, register_ndvi_workflow_helper, send_test_request,
    };
    use actix_web::{http::header, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::assert_image_equals;
    use pretty_assertions::assert_eq;
    use tokio_postgres::NoTls;

    async fn session_and_processing_graph_id(
        app_ctx: &PostgresContext<NoTls>,
    ) -> (SessionId, WorkflowId) {
        let session = admin_login(app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = ctx.session().id();
        let (_, processing_graph_id) = register_ndvi_workflow_helper(app_ctx).await;

        (session_id, processing_graph_id)
    }

    #[ge_context::test]
    async fn it_lists_tilesets_for_collection(app_ctx: PostgresContext<NoTls>) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, processing_graph_id) = session_and_processing_graph_id(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!(
                "/ogc/{processing_graph_id}/collections/{processing_graph_id}/map/tiles"
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
                        "tileMatrixSetId": CUSTOM_TILE_MATRIX_SET_ID,
                        "links": [
                            {
                                "href": format!(
                                    "{server_url}/api/ogc/{processing_graph_id}/collections/{processing_graph_id}/map/tiles/{CUSTOM_TILE_MATRIX_SET_ID}"
                                ),
                                "rel": "self",
                                "type": "application/json"
                            }
                        ]
                    }
                ]
            })
        );
    }

    #[ge_context::test]
    async fn it_returns_tileset_metadata_with_template_link(app_ctx: PostgresContext<NoTls>) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, processing_graph_id) = session_and_processing_graph_id(&app_ctx).await;

        let req = test::TestRequest::get()
			.uri(&format!(
				"/ogc/{processing_graph_id}/collections/{processing_graph_id}/map/tiles/{CUSTOM_TILE_MATRIX_SET_ID}"
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
                "tileMatrixSetId": CUSTOM_TILE_MATRIX_SET_ID,
                "links": [
                    {
                        "href": format!(
                            "{server_url}/api/ogc/{processing_graph_id}/collections/{processing_graph_id}/map/tiles/{CUSTOM_TILE_MATRIX_SET_ID}/{{tileMatrix}}/{{tileRow}}/{{tileCol}}?datetime={{datetime}}"
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
        let (session_id, processing_graph_id) = session_and_processing_graph_id(&app_ctx).await;

        let req = test::TestRequest::get()
			.uri(&format!(
				"/ogc/{processing_graph_id}/collections/{processing_graph_id}/map/tiles/{CUSTOM_TILE_MATRIX_SET_ID}/0/0/0?datetime=2014-01-01T00:00:00Z"
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

        let file_path = test_data!("ogc/tiles/ndvi.png").to_path_buf();
        if !file_path.exists() {
            geoengine_datatypes::util::test::save_test_bytes(&image_bytes, &file_path);
        }

        assert_image_equals(&file_path, &image_bytes);
    }
}
