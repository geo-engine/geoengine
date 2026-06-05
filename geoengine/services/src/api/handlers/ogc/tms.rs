use crate::{
    api::handlers::ogc::{
        OgcApiResult,
        error::OgcApiError,
        util::{crs_from_spatial_reference_option, link_creator, raster_workflow_metadata},
    },
    contexts::{ApplicationContext, SessionContext},
    workflows::{registry::WorkflowRegistry, workflow::WorkflowId},
};
use actix_web::web;
use geoengine_datatypes::primitives::AxisAlignedRectangle;
use geoengine_datatypes::raster::GridShape2D;
use geoengine_operators::engine::{ExecutionContext, RasterResultDescriptor};
use ogcapi_types::{
    common::{link_rel::SELF, media_type::JSON},
    tiles::{
        CornerOfOrigin, TileMatrix, TileMatrixSet, TileMatrixSetId, TileMatrixSetItem,
        TileMatrixSets, TilesCrs,
    },
};
use std::num::{NonZeroU16, NonZeroU64};

const CUSTOM_TILE_MATRIX_SET_ID: &str = "GeoEngineCustomTMS";
const CUSTOM_TILE_MATRIX_SET_TITLE: &str = "Custom Grid for Geo Engine";
const STANDARD_PIXEL_SIZE_METERS: f64 = 0.00028;
const MAX_TILE_MATRIX_LEVEL: u8 = 22;

/// OGC API Tile Matrix Set List
///
/// Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).
/// Cf. [OGC Two Dimensional Tile Matrix Set and Tile Set Metadata](https://docs.ogc.org/is/17-083r4/17-083r4.html).
#[utoipa::path(
    tag = "OGC API",
    get,
    path = "/ogc/{processingGraphId}/tileMatrixSets",
    responses(
        (status = 200, description = "OK", body = TileMatrixSets)
    ),
    params(
        ("processingGraphId" = WorkflowId, description = "ID of the processing graph, which is used as collection ID")
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn tile_matrix_sets<C: ApplicationContext>(
    _session: C::Session,
    _app_ctx: web::Data<C>,
    processing_graph_id: web::Path<WorkflowId>,
) -> OgcApiResult<web::Json<TileMatrixSets>> {
    let processing_graph_id = processing_graph_id.into_inner();
    let create_link = link_creator(processing_graph_id);

    Ok(web::Json(TileMatrixSets {
        tile_matrix_sets: vec![TileMatrixSetItem {
            id: Some(TileMatrixSetId::Custom(
                CUSTOM_TILE_MATRIX_SET_ID.to_string(),
            )),
            title: Some(CUSTOM_TILE_MATRIX_SET_TITLE.to_string()),
            uri: None,
            crs: None,
            links: vec![create_link(
                &format!("tileMatrixSets/{CUSTOM_TILE_MATRIX_SET_ID}"),
                SELF,
                JSON,
            )?],
        }],
    }))
}

/// OGC API Tile Matrix Set Definition
///
/// Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).
/// Cf. [OGC Two Dimensional Tile Matrix Set and Tile Set Metadata](https://docs.ogc.org/is/17-083r4/17-083r4.html).
#[utoipa::path(
    tag = "OGC API",
    get,
    path = "/ogc/{processingGraphId}/tileMatrixSets/{tileMatrixSetId}",
    responses(
        (status = 200, description = "OK", body = TileMatrixSet),
        (status = 404, description = "Tile matrix set not found")
    ),
    params(
        ("processingGraphId" = WorkflowId, description = "ID of the processing graph, which is used as collection ID"),
        ("tileMatrixSetId" = String, description = "Tile matrix set identifier")
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn tile_matrix_set<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(WorkflowId, String)>,
) -> OgcApiResult<web::Json<TileMatrixSet>> {
    let (processing_graph_id, tile_matrix_set_id) = path.into_inner();

    if tile_matrix_set_id != CUSTOM_TILE_MATRIX_SET_ID {
        return Err(OgcApiError::TileMatrixSetNotFound { tile_matrix_set_id });
    }

    let ctx = app_ctx.session_context(session);
    let execution_context = ctx.execution_context()?;
    let tile_size = execution_context.tiling_specification().tile_size_in_pixels;
    // let [tile_height, tile_width] = tile_size.into_inner();

    let processing_graph = ctx.db().load_workflow(&processing_graph_id).await?;
    let descriptor =
        raster_workflow_metadata::<C::SessionContext>(processing_graph, execution_context).await?;

    Ok(web::Json(TileMatrixSet {
        id: TileMatrixSetId::Custom(CUSTOM_TILE_MATRIX_SET_ID.to_string()),
        title: Some(CUSTOM_TILE_MATRIX_SET_TITLE.to_string()),
        description: None,
        keywords: Vec::new(),
        uri: None,
        crs: TilesCrs::Simple(crs_from_spatial_reference_option(
            descriptor.spatial_reference,
        )?),
        ordered_axes: Vec::new(),
        well_known_scale_set: None,
        bounding_box: None,
        tile_matrices: build_tile_matrices(&tile_size, &descriptor),
    }))
}

fn build_tile_matrices(
    tile_size: &GridShape2D,
    descriptor: &RasterResultDescriptor,
) -> Vec<TileMatrix> {
    let tile_width = to_non_zero_u16(tile_size.x());
    let tile_height = to_non_zero_u16(tile_size.y());

    let spatial_bounds = descriptor.spatial_bounds();
    let z0_resolution = spatial_bounds.size_x() / f64::from(tile_width.get());
    let point_of_origin = spatial_bounds.upper_left();

    (0..=MAX_TILE_MATRIX_LEVEL)
        .map(|level| {
            let matrix_size = 1_u64 << level;
            let resolution = z0_resolution / f64::from(1_u32 << level);
            let scale_denominator = resolution / STANDARD_PIXEL_SIZE_METERS;

            TileMatrix {
                id: level.to_string(),
                title: None,
                description: None,
                keywords: Vec::new(),
                scale_denominator,
                cell_size: resolution,
                corner_of_origin: CornerOfOrigin::TopLeft,
                point_of_origin: [point_of_origin.x, point_of_origin.y],
                tile_width,
                tile_height,
                matrix_width: to_non_zero_u64(matrix_size),
                matrix_height: to_non_zero_u64(matrix_size),
                variable_matrix_widths: Vec::new(),
            }
        })
        .collect()
}

fn to_non_zero_u16(value: usize) -> NonZeroU16 {
    let value = u16::try_from(value).unwrap_or(u16::MAX);
    NonZeroU16::new(value.max(1)).unwrap_or(NonZeroU16::MIN)
}

fn to_non_zero_u64(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value.max(1)).unwrap_or(NonZeroU64::MIN)
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
    use crate::workflows::registry::WorkflowRegistry;
    use crate::workflows::workflow::WorkflowId;
    use actix_web::{http::header, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::primitives::AxisAlignedRectangle;
    use geoengine_operators::engine::ExecutionContext;
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
    async fn it_lists_custom_tile_matrix_set(app_ctx: PostgresContext<NoTls>) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, processing_graph_id) = session_and_processing_graph_id(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!("/ogc/{processing_graph_id}/tileMatrixSets"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        assert_eq!(
            body,
            serde_json::json!({
                "tileMatrixSets": [
                    {
                        "id": CUSTOM_TILE_MATRIX_SET_ID,
                        "title": CUSTOM_TILE_MATRIX_SET_TITLE,
                        "links": [
                            {
                                "href": format!(
                                    "{server_url}/api/ogc/{processing_graph_id}/tileMatrixSets/{CUSTOM_TILE_MATRIX_SET_ID}"
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
    async fn it_returns_custom_tile_matrix_set_definition(app_ctx: PostgresContext<NoTls>) {
        let (session_id, processing_graph_id) = session_and_processing_graph_id(&app_ctx).await;
        let session = app_ctx.session_by_id(session_id).await.unwrap();
        let ctx = app_ctx.session_context(session);
        let execution_context = ctx.execution_context().unwrap();
        let [tile_height, tile_width] = execution_context
            .tiling_specification()
            .tile_size_in_pixels
            .into_inner();
        let processing_graph = ctx.db().load_workflow(&processing_graph_id).await.unwrap();
        let descriptor = super::raster_workflow_metadata::<
            crate::contexts::PostgresSessionContext<NoTls>,
        >(processing_graph, execution_context)
        .await
        .unwrap();
        let spatial_bounds = descriptor.spatial_bounds();
        let point_of_origin = spatial_bounds.upper_left();
        let z0_resolution = spatial_bounds.size_x() / tile_width as f64;
        let expected_crs = crs_from_spatial_reference_option(descriptor.spatial_reference)
            .unwrap()
            .to_string();

        let req = test::TestRequest::get()
            .uri(&format!(
                "/ogc/{processing_graph_id}/tileMatrixSets/{CUSTOM_TILE_MATRIX_SET_ID}"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        assert_eq!(body["id"], serde_json::json!(CUSTOM_TILE_MATRIX_SET_ID));
        assert_eq!(
            body["title"],
            serde_json::json!(CUSTOM_TILE_MATRIX_SET_TITLE)
        );
        assert_eq!(body["uri"], serde_json::Value::Null);
        assert_eq!(body["crs"], serde_json::json!(expected_crs));

        let tile_matrices = body["tileMatrices"]
            .as_array()
            .expect("tileMatrices should be an array");
        assert_eq!(tile_matrices.len(), usize::from(MAX_TILE_MATRIX_LEVEL) + 1);

        for (level, tile_matrix) in tile_matrices.iter().enumerate() {
            let matrix_size = 1_u64 << level;
            let expected_resolution = z0_resolution / f64::from(1_u32 << level);
            let expected_scale_denominator = expected_resolution / STANDARD_PIXEL_SIZE_METERS;

            assert_eq!(tile_matrix["id"], serde_json::json!(level.to_string()));
            assert_eq!(tile_matrix["cornerOfOrigin"], serde_json::json!("topLeft"));
            assert_eq!(tile_matrix["tileWidth"], serde_json::json!(tile_width));
            assert_eq!(tile_matrix["tileHeight"], serde_json::json!(tile_height));
            assert_eq!(tile_matrix["matrixWidth"], serde_json::json!(matrix_size));
            assert_eq!(tile_matrix["matrixHeight"], serde_json::json!(matrix_size));
            assert_eq!(
                tile_matrix["pointOfOrigin"],
                serde_json::json!([point_of_origin.x, point_of_origin.y])
            );

            let actual_resolution = tile_matrix["cellSize"]
                .as_f64()
                .expect("cellSize should be a number");
            let actual_scale_denominator = tile_matrix["scaleDenominator"]
                .as_f64()
                .expect("scaleDenominator should be a number");

            assert!((actual_resolution - expected_resolution).abs() < 1.0e-12);
            assert!((actual_scale_denominator - expected_scale_denominator).abs() < 1.0e-12);
        }
    }

    #[ge_context::test]
    async fn it_returns_not_found_for_unknown_tile_matrix_set(app_ctx: PostgresContext<NoTls>) {
        let (session_id, processing_graph_id) = session_and_processing_graph_id(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!(
                "/ogc/{processing_graph_id}/tileMatrixSets/UnknownTMS"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 404, "{body}");

        let expected_exception = serde_json::json!({
            "type": "about:blank",
            "title": "Tile matrix set not found",
            "status": 404,
            "detail": "Tile matrix set `UnknownTMS` does not exist"
        });

        let expected_not_found = serde_json::json!({
            "error": "NotFound",
            "message": "Not Found"
        });

        assert!(
            body == expected_exception || body == expected_not_found,
            "unexpected 404 body: {body}"
        );
    }
}
