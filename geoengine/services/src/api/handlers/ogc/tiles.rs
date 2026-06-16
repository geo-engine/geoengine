use crate::{
    api::{
        handlers::ogc::{
            OgcApiResult,
            error::{self, OgcApiError},
            tms::{CUSTOM_TILE_MATRIX_SET_ID, build_tile_matrices},
            util::{
                crs_from_spatial_reference_option, link_creator, load_layer, parse_datetime_option,
                raster_workflow_metadata,
            },
        },
        model::datatypes::{DataProviderId, LayerId},
    },
    config::get_config_element,
    contexts::{ApplicationContext, SessionContext},
    util::server::{CacheControlHeader, connection_closed},
    workflows::{registry::WorkflowRegistry, workflow::Workflow},
};
use actix_web::{HttpRequest, HttpResponse, web};
use geoengine_datatypes::{
    error::BoxedResultExt,
    operations::image::RasterColorizer,
    primitives::{
        AxisAlignedRectangle, BandSelection, RasterQueryRectangle, TimeInstance, TimeInterval,
    },
    raster::{
        GridBoundingBox2D, GridBounds, GridIdx2D, TilingSpatialGridDefinition, TilingSpecification,
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
        BoundingBox2D, DataType, TileMatrix, TileMatrixSetId, TileSet, TileSetItem, TileSets,
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
        // tile_matrix_set_limits: vec![
        //     // TileMatrixLimits {
        //     //     tile_matrix: tile_matrix_set_id.clone(),
        //     //     min_tile_row: 0,
        //     //     max_tile_row: MAX_TILE_MATRIX_LEVEL.into(),
        //     //     min_tile_col: 0,
        //     //     max_tile_col: MAX_TILE_MATRIX_LEVEL.into(),
        //     // }
        // ],
        // tile_matrix_set_limits: (0..=MAX_TILE_MATRIX_LEVEL)
        //     .map(|tile_matrix| {
        //         let max = 2_u64.pow(u32::from(tile_matrix));
        //         TileMatrixLimits {
        //             tile_matrix: tile_matrix.to_string(),
        //             min_tile_row: 0,
        //             max_tile_row: max,
        //             min_tile_col: 0,
        //             max_tile_col: max,
        //         }
        //     })
        //     .collect(),
        tile_matrix_set_limits: vec![],
        crs: crs.clone(),
        epoch: None,
        layers: vec![],
        // layers: vec![GeospatialData {
        //     id: layer_id.to_string(),
        //     title: None,
        //     description: None,
        //     keywords: vec![],
        //     data_type: DataType::Map,
        //     geometry_dimension: None,
        //     feature_type: None,
        //     attribution: None,
        //     license: None,
        //     point_of_contact: None,
        //     publisher: None,
        //     theme: None,
        //     crs: None,
        //     epoch: None,
        //     min_scale_denominator: None,
        //     max_scale_denominator: None,
        //     min_cell_size: None,
        //     max_cell_size: None,
        //     max_tile_matrix: Some(MAX_TILE_MATRIX_LEVEL.to_string()),
        //     min_tile_matrix: Some(0.to_string()), // TODO: add lower limits if data is expensive to process
        //     bounding_box: None,
        //     created: None,
        //     updated: None,
        //     style: None,
        //     geo_data_classes: vec![],
        //     properties_schema: None,
        //     links: vec![],
        // }],
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
        u64,
        u64,
    )>,
    query: web::Query<TileQueryParams>,
) -> OgcApiResult<HttpResponse> {
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

    let query_time = query_time_from_datetime(query.into_inner().datetime)?;

    let ctx = app_ctx.session_context(session);
    let layer = load_layer::<C>(&ctx, data_connector_id, layer_id.clone()).await?;
    let tiling_specification = ctx.execution_context()?.tiling_specification();
    let tile_size = tiling_specification.tile_size_in_pixels;
    let tile_width = u32::try_from(tile_size.x()).unwrap_or(u32::MAX);
    let tile_height = u32::try_from(tile_size.y()).unwrap_or(u32::MAX);
    let execution_context = ctx.execution_context()?;

    let (processor, descriptor) = raster_query_processor_and_descriptor::<C::SessionContext>(
        layer.workflow.clone(),
        execution_context,
    )
    .await?;

    let spatial_reference = descriptor.spatial_reference.as_option().ok_or_else(|| {
        OgcApiError::TileMatrixSetDefinitionNotAvailable {
            tile_matrix_set_id: tile_matrix_set_id.to_string(),
            reason: "Spatial reference of the layer is not available".to_string(),
        }
    })?;

    let tile_matrices = build_tile_matrices(
        descriptor
            .spatial_grid
            .tiling_grid_definition(tiling_specification),
        tiling_specification,
        spatial_reference,
    )?;

    let tile_matrix = tile_matrices.get(tile_matrix as usize).ok_or_else(|| {
        OgcApiError::InvalidTileCoordinates {
            matrix: tile_matrix.to_string(),
            row: tile_row,
            col: tile_col,
        }
    })?;

    let tiling_spatial_grid_definition = descriptor
        .spatial_grid
        .tiling_grid_definition(tiling_specification);
    // let grid_bounds = tiling_spatial_grid_definition.tiling_grid_bounds();
    // let tiling_strategy = tiling_spatial_grid_definition.generate_data_tiling_strategy();

    // let tile_idx_x = grid_bounds.x_min() + tile_col as isize;
    // let tile_idx_y = grid_bounds.y_min() + tile_row as isize;
    // // let tile_min =
    // //     tiling_strategy.tile_idx_to_global_pixel_idx(GridIdx2D::new([tile_idx_y, tile_idx_x]));
    // let tile_min = GridIdx2D::new([
    //     /* x_min: */ grid_bounds.x_min() + tile_col as isize * tile_width as isize,
    //     /* y_min: */ grid_bounds.y_max() - (tile_row as isize + 1) * tile_height as isize,
    // ]);
    // let tile_max = tile_min + GridIdx2D::new([tile_width as isize - 1, tile_height as isize - 1]);

    // dbg!(
    //     tile_matrix,
    //     tile_row,
    //     tile_col,
    //     grid_bounds,
    //     tile_min,
    //     tile_max
    // );

    // let tile_bounds = GridBoundingBox2D::new_unchecked(tile_min, tile_max);

    // let tile_bounds = tile_spatial_bounds(&descriptor, tile_matrix, tile_row, tile_col)?;

    // let query_grid = descriptor
    //     .spatial_grid
    //     .tiling_grid_definition(tiling_specification)
    //     .tiling_spatial_grid_definition()
    //     .spatial_bounds_to_compatible_spatial_grid(tile_bounds);

    let query_rect = RasterQueryRectangle::new(
        tile_grid_bbox(
            tile_matrix,
            &tiling_spatial_grid_definition,
            &tiling_specification,
            tile_row,
            tile_col,
        )?,
        query_time,
        BandSelection::first(),
    );

    let processing_graph_id = ctx.db().register_workflow(layer.workflow.clone()).await?;
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

/// Computes the pixel bounds of a tile in the global pixel grid of the tiling scheme.
/// The resolution is determined by `tile_matrix`, which corresponds to the zoom level in a TMS.
/// The origin and orientation of the tile grid is determined by the `tiling_spatial_grid_definition`.
fn tile_grid_bbox(
    tile_matrix: &TileMatrix,
    tiling_spatial_grid_definition: &TilingSpatialGridDefinition,
    _tiling_specification: &TilingSpecification,
    tile_row: u64,
    tile_col: u64,
) -> OgcApiResult<GridBoundingBox2D> {
    let grid_bounds = tiling_spatial_grid_definition.tiling_grid_bounds();
    let tiling_strategy = tiling_spatial_grid_definition.generate_data_tiling_strategy();

    let tile_grid_bounds = tiling_strategy.raster_spatial_query_to_tiling_grid_box(grid_bounds);
    let tile_index =
        tile_grid_bounds.min_index() + GridIdx2D::new([tile_row as isize, tile_col as isize]);

    let min_pixel_index = tiling_strategy.tile_idx_to_global_pixel_idx(tile_index);
    let max_pixel_index = min_pixel_index
        + GridIdx2D::new([
            tiling_strategy.tile_size_in_pixels.y() as isize,
            tiling_strategy.tile_size_in_pixels.x() as isize,
        ])
        - GridIdx2D::new([1, 1]); // inclusive bounds

    // dbg!(
    //     grid_bounds,
    //     tile_grid_bounds,
    //     tile_index,
    //     min_pixel_index,
    //     max_pixel_index
    // );

    GridBoundingBox2D::new(min_pixel_index, max_pixel_index).map_err(|_source| {
        OgcApiError::InvalidTileCoordinates {
            matrix: tile_matrix.id.clone(),
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

// fn tile_spatial_bounds(
//     descriptor: &RasterResultDescriptor,
//     tile_matrix: u8,
//     tile_row: u64,
//     tile_col: u64,
// ) -> OgcApiResult<SpatialPartition2D> {
//     // if tile_matrix > MAX_TILE_MATRIX_LEVEL {
//     //     return Err(OgcApiError::InvalidTileCoordinates {
//     //         matrix: tile_matrix,
//     //         row: tile_row,
//     //         col: tile_col,
//     //     });
//     // }

//     let matrix_size = 1_u64 << tile_matrix;

//     if tile_row >= matrix_size || tile_col >= matrix_size {
//         return Err(OgcApiError::InvalidTileCoordinates {
//             matrix: tile_matrix.to_string(),
//             row: tile_row,
//             col: tile_col,
//         });
//     }

//     let spatial_bounds = descriptor.spatial_bounds();
//     let upper_left = spatial_bounds.upper_left();

//     let tile_span_x = spatial_bounds.size_x() / matrix_size as f64;
//     let tile_span_y = spatial_bounds.size_y() / matrix_size as f64;

//     let tile_upper_left = Coordinate2D::new(
//         upper_left.x + tile_col as f64 * tile_span_x,
//         upper_left.y - tile_row as f64 * tile_span_y,
//     );
//     let tile_lower_right = Coordinate2D::new(
//         tile_upper_left.x + tile_span_x,
//         tile_upper_left.y - tile_span_y,
//     );

//     SpatialPartition2D::new(tile_upper_left, tile_lower_right)
//         .boxed_context(error::InvalidBoundingBox)
// }

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
    use core::f64;

    use super::*;
    use crate::{
        contexts::{
            ApplicationContext, PostgresContext, PostgresSessionContext, Session, SessionContext,
            SessionId,
        },
        ge_context,
        util::tests::{add_ndvi_to_layers, admin_login, read_body_json, send_test_request},
    };
    use actix_web::{http::header, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::{test_data, util::assert_image_equals};
    use ogcapi_types::tiles::CornerOfOrigin;
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
    async fn it_lists_tilesets_for_collection(app_ctx: PostgresContext<NoTls>) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, data_connector_id, layer_id) = session_and_layer_id(&app_ctx).await;

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
                        "tileMatrixSetId": CUSTOM_TILE_MATRIX_SET_ID,
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
                ]
            })
        );
    }

    #[ge_context::test]
    async fn it_returns_tileset_metadata_with_template_link(app_ctx: PostgresContext<NoTls>) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, data_connector_id, layer_id) = session_and_layer_id(&app_ctx).await;

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
                "tileMatrixSetId": CUSTOM_TILE_MATRIX_SET_ID,
                "links": [
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
        let (session_id, data_connector_id, layer_id) = session_and_layer_id(&app_ctx).await;

        let req = test::TestRequest::get()
			.uri(&format!(
				"/ogc/{data_connector_id}/{layer_id}/collections/{layer_id}/map/tiles/{CUSTOM_TILE_MATRIX_SET_ID}/0/0/2?datetime=2014-01-01T00:00:00Z"
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

        let file_path = test_data!("ogc/tiles/ndvi_ul.png").to_path_buf();
        if !file_path.exists() {
            geoengine_datatypes::util::test::save_test_bytes(&image_bytes, &file_path);
        }

        assert_image_equals(&file_path, &image_bytes);
    }

    #[ge_context::test]
    async fn it_calculates_correct_pixel_bounds_for_tiles(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) = session_and_layer_id(&app_ctx).await;

        let ctx = app_ctx.session_context(app_ctx.session_by_id(session_id).await.unwrap());

        let layer = load_layer::<PostgresContext<NoTls>>(&ctx, data_connector_id, layer_id.clone())
            .await
            .unwrap();
        let tiling_specification = ctx.execution_context().unwrap().tiling_specification();

        let (_processor, descriptor) = raster_query_processor_and_descriptor::<
            PostgresSessionContext<NoTls>,
        >(
            layer.workflow.clone(), ctx.execution_context().unwrap()
        )
        .await
        .unwrap();

        let tile_matrix = TileMatrix {
            id: 0.to_string(),
            title: None,
            description: None,
            keywords: vec![],
            scale_denominator: f64::NAN, // unused
            cell_size: f64::NAN,         // unused
            corner_of_origin: CornerOfOrigin::TopLeft,
            point_of_origin: [-180.0, 90.0],
            tile_width: 512.try_into().unwrap(),
            tile_height: 512.try_into().unwrap(),
            matrix_width: 8.try_into().unwrap(),
            matrix_height: 4.try_into().unwrap(),
            variable_matrix_widths: vec![],
        };

        let tiling_spatial_grid_definition =
            descriptor.tiling_grid_definition(tiling_specification);
        let tiling_strategy = tiling_spatial_grid_definition.generate_data_tiling_strategy();

        let mut tiling_iterator = tiling_strategy.tile_information_iterator_from_pixel_bounds(
            tiling_spatial_grid_definition.tiling_grid_bounds(),
        );

        // let first_tile = tiling_strategy
        //     .tile_idx_iterator_from_grid_bounds(tiling_spatial_grid_definition.tiling_grid_bounds())
        //     .next()
        //     .unwrap();

        // dbg!(first_tile);

        let first_tile_info = tiling_iterator.next().unwrap();
        let last_tile_info = tiling_iterator.last().unwrap();

        assert_eq!(
            first_tile_info.global_upper_left_pixel_idx(),
            GridIdx2D::new([-1024, -2048])
        );
        assert_eq!(
            first_tile_info.global_lower_right_pixel_idx(),
            GridIdx2D::new([-513, -1537])
        );
        assert_eq!(
            last_tile_info.global_upper_left_pixel_idx(),
            GridIdx2D::new([512, 1536])
        );
        assert_eq!(
            last_tile_info.global_lower_right_pixel_idx(),
            GridIdx2D::new([1023, 2047])
        );

        for (tile_row, tile_col, expected_min, expected_max) in [
            [(0, 0, [-1024, -2048], [-513, -1537])],
            [(4, 8, [512, 1536], [1023, 2047])],
        ] {
            assert_eq!(
                tile_grid_bbox(
                    &tile_matrix,
                    &descriptor
                        .spatial_grid
                        .tiling_grid_definition(tiling_specification),
                    &tiling_specification,
                    tile_row,
                    tile_col,
                )
                .unwrap(),
                GridBoundingBox2D::new_unchecked(expected_min, expected_max)
            );
        }
    }
}
