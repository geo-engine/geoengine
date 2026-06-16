use crate::{
    api::{
        handlers::ogc::{
            OgcApiResult,
            error::OgcApiError,
            util::{
                crs_from_spatial_reference_option, link_creator, load_layer,
                raster_workflow_metadata,
            },
        },
        model::datatypes::{DataProviderId, LayerId},
    },
    contexts::{ApplicationContext, SessionContext},
};
use actix_web::web;
use float_cmp::approx_eq;
use geoengine_datatypes::{
    primitives::AxisAlignedRectangle,
    raster::{GridBounds, GridShapeAccess, TilingSpatialGridDefinition, TilingSpecification},
    spatial_reference::SpatialReference,
};
use geoengine_operators::engine::ExecutionContext;
use ogcapi_types::{
    common::{link_rel::SELF, media_type::JSON},
    tiles::{
        CornerOfOrigin, TileMatrix, TileMatrixSet, TileMatrixSetId, TileMatrixSetItem,
        TileMatrixSets, TilesCrs,
    },
};
use std::{
    f64,
    num::{NonZeroU16, NonZeroU64},
};

pub(super) const CUSTOM_TILE_MATRIX_SET_ID: &str = "GeoEngineCustomTMS";
const CUSTOM_TILE_MATRIX_SET_TITLE: &str = "Custom Grid for Geo Engine";
/// Cf. <https://docs.ogc.org/is/17-083r4/17-083r4.html#6-1-1-1-%C2%A0-tile-matrix-in-a-two-dimensional-space>
const STANDARD_PIXEL_SIZE_METERS: f64 = 0.28e-3;

/// OGC API Tile Matrix Set List
///
/// Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).
/// Cf. [OGC Two Dimensional Tile Matrix Set and Tile Set Metadata](https://docs.ogc.org/is/17-083r4/17-083r4.html).
#[utoipa::path(
    tag = "OGC API",
    get,
    path = "/ogc/{dataConnectorId}/{layerId}/tileMatrixSets",
    responses(
        (status = 200, description = "OK", body = TileMatrixSets)
    ),
    params(
        ("dataConnectorId" = DataProviderId, description = "ID of the data connector"),
        ("layerId" = LayerId, description = "ID of the layer, which is used as collection ID"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn tile_matrix_sets<C: ApplicationContext>(
    _session: C::Session,
    _app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId)>,
) -> OgcApiResult<web::Json<TileMatrixSets>> {
    let (data_connector_id, layer_id) = path.into_inner();
    let create_link = link_creator(data_connector_id, layer_id.clone());

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
    path = "/ogc/{dataConnectorId}/{layerId}/tileMatrixSets/{tileMatrixSetId}",
    responses(
        (status = 200, description = "OK", body = TileMatrixSet),
        (status = 404, description = "Tile matrix set not found")
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
pub async fn tile_matrix_set<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId, TileMatrixSetId)>,
) -> OgcApiResult<web::Json<TileMatrixSet>> {
    let (data_connector_id, layer_id, tile_matrix_set_id) = path.into_inner();

    if tile_matrix_set_id != TileMatrixSetId::Custom(CUSTOM_TILE_MATRIX_SET_ID.to_string()) {
        return Err(OgcApiError::TileMatrixSetNotFound {
            tile_matrix_set_id: tile_matrix_set_id.to_string(),
        });
    }

    let ctx = app_ctx.session_context(session);
    let execution_context = ctx.execution_context()?;

    let layer = load_layer::<C>(&ctx, data_connector_id, layer_id.clone()).await?;

    let tiling_specification = execution_context.tiling_specification();
    let descriptor =
        raster_workflow_metadata::<C::SessionContext>(layer.workflow, execution_context).await?;

    let spatial_reference = descriptor.spatial_reference.as_option().ok_or_else(|| {
        OgcApiError::TileMatrixSetDefinitionNotAvailable {
            tile_matrix_set_id: tile_matrix_set_id.to_string(),
            reason: "Spatial reference of the layer is not available".to_string(),
        }
    })?;

    Ok(web::Json(TileMatrixSet {
        id: TileMatrixSetId::Custom(CUSTOM_TILE_MATRIX_SET_ID.to_string()),
        title: Some(CUSTOM_TILE_MATRIX_SET_TITLE.to_string()),
        description: None,
        keywords: Vec::new(),
        uri: None,
        crs: TilesCrs::Simple(crs_from_spatial_reference_option(
            descriptor.spatial_reference,
        )?),
        ordered_axes: vec!["Lon".into(), "Lat".into()],
        well_known_scale_set: None,
        bounding_box: None,
        tile_matrices: build_tile_matrices(
            descriptor.tiling_grid_definition(tiling_specification),
            tiling_specification,
            spatial_reference,
        )?,
    }))
}

pub fn build_tile_matrices(
    tiling_spatial_grid_definition: TilingSpatialGridDefinition,
    tiling_specification: TilingSpecification,
    spatial_reference: SpatialReference,
) -> OgcApiResult<Vec<TileMatrix>> {
    let [tile_width, tile_height] = tiling_specification.grid_shape_array();
    let tile_width = to_non_zero_u16(tile_width);
    let tile_height = to_non_zero_u16(tile_height);

    let grid_bounds = tiling_spatial_grid_definition.tiling_grid_bounds();
    let tiling_geo_transform = tiling_spatial_grid_definition.tiling_geo_transform();
    let (x_resolution, y_resolution) = (
        tiling_geo_transform.x_pixel_size(),
        tiling_geo_transform.y_pixel_size(),
    );

    if !approx_eq!(f64, x_resolution.abs(), y_resolution.abs()) {
        return Err(OgcApiError::TileMatrixSetDefinitionNotAvailable {
            tile_matrix_set_id: CUSTOM_TILE_MATRIX_SET_ID.to_string(),
            reason: format!(
                "Non-square pixels are not supported (x resolution: {x_resolution}, y resolution: {y_resolution})",
            ),
        });
    }
    let resolution = x_resolution.abs();

    // let spatial_bounds = descriptor.spatial_bounds();
    // let z0_resolution = spatial_bounds.size_x() / f64::from(tile_width.get());
    // let point_of_origin = spatial_bounds.upper_left();

    let meters_per_unit = spatial_reference.meters_per_unit().map_err(|source| {
        OgcApiError::UnknownSpatialReferenceInfo {
            source: Box::new(source),
            from: spatial_reference.into(),
            info: "meters per unit".into(),
        }
    })?;

    let original_x_size = to_non_zero_u64((grid_bounds.x_max() - grid_bounds.x_min() + 1) as u64);
    let original_y_size = to_non_zero_u64((grid_bounds.y_max() - grid_bounds.y_min() + 1) as u64);

    let spatial_bounds = tiling_geo_transform.grid_to_spatial_bounds(&grid_bounds);

    let tiling_strategy = tiling_spatial_grid_definition.generate_data_tiling_strategy();
    let tile_grid_bounds = tiling_strategy.raster_spatial_query_to_tiling_grid_box(grid_bounds);
    let min_pixel_index =
        tiling_strategy.tile_idx_to_global_pixel_idx(tile_grid_bounds.min_index());

    let min_coordinate =
        tiling_geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d(min_pixel_index);
    dbg!(min_pixel_index, min_coordinate);

    Ok(vec![TileMatrix {
        id: 0.to_string(),
        title: None,
        description: None,
        keywords: Vec::new(),
        scale_denominator: scale_denominator(resolution, meters_per_unit),
        cell_size: resolution,
        corner_of_origin: CornerOfOrigin::TopLeft,
        // point_of_origin: [spatial_bounds.upper_left().x, spatial_bounds.upper_left().y],
        point_of_origin: [min_coordinate.x, min_coordinate.y],
        tile_width,
        tile_height,
        matrix_width: original_x_size.div_ceil(NonZeroU64::from(tile_width)),
        matrix_height: original_y_size.div_ceil(NonZeroU64::from(tile_height)),
        variable_matrix_widths: Vec::new(),
    }])

    // we start with the original resolution of the raster and then create levels with exponentially decreasing
    // resolution (i.e. increasing zoom level) until one dimension of the tile matrix is smaller than the tile size.

    // (0..=MAX_TILE_MATRIX_LEVEL)
    //     .map(|level| {
    //         let matrix_y_size = 1_u64 << level;
    //         let matrix_x_size = matrix_y_size * 2; // TODO: works only for EPSG:4326
    //         let resolution = z0_resolution / f64::from(1_u32 << level);

    //         TileMatrix {
    //             id: level.to_string(),
    //             title: None,
    //             description: None,
    //             keywords: Vec::new(),
    //             scale_denominator: scale_denominator(resolution, meters_per_unit)?,
    //             cell_size: resolution,
    //             corner_of_origin: CornerOfOrigin::TopLeft,
    //             point_of_origin: [point_of_origin.x, point_of_origin.y],
    //             tile_width,
    //             tile_height,
    //             matrix_width: to_non_zero_u64(matrix_x_size),
    //             matrix_height: to_non_zero_u64(matrix_y_size),
    //             variable_matrix_widths: Vec::new(),
    //         }
    //     })
    //     .collect()
}

fn to_non_zero_u16(value: usize) -> NonZeroU16 {
    let value = u16::try_from(value).unwrap_or(u16::MAX);
    NonZeroU16::new(value.max(1)).unwrap_or(NonZeroU16::MIN)
}

fn to_non_zero_u64(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value.max(1)).unwrap_or(NonZeroU64::MIN)
}

/// The documentation (<https://docs.ogc.org/is/17-083r4/17-083r4.html#6-1-1-1-%C2%A0-tile-matrix-in-a-two-dimensional-space>) says:
/// `cellSize = scaleDenominator × 0.2810^{−3} / metersPerUnit(crs)`
/// Thus, we have to calculate:
/// `scaleDenominator = cellSize × metersPerUnit(crs) / 0.2810^{−3}`
fn scale_denominator(cell_size: f64, meters_per_unit: f64) -> f64 {
    cell_size * meters_per_unit / STANDARD_PIXEL_SIZE_METERS
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::{
        contexts::{ApplicationContext, PostgresContext, Session, SessionContext, SessionId},
        ge_context,
        layers::listing::LayerCollectionProvider,
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
    async fn it_lists_custom_tile_matrix_set(app_ctx: PostgresContext<NoTls>) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, data_connector_id, layer_id) = session_and_layer_id(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/tileMatrixSets"
            ))
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
                                    "{server_url}/api/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/{CUSTOM_TILE_MATRIX_SET_ID}"
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
        let (session_id, data_connector_id, layer_id) = session_and_layer_id(&app_ctx).await;
        let session = app_ctx.session_by_id(session_id).await.unwrap();
        let ctx = app_ctx.session_context(session);
        let execution_context = ctx.execution_context().unwrap();
        let layer = ctx.db().load_layer(&layer_id.clone().into()).await.unwrap();
        let descriptor = super::raster_workflow_metadata::<
            crate::contexts::PostgresSessionContext<NoTls>,
        >(layer.workflow, execution_context)
        .await
        .unwrap();

        // let spatial_bounds = descriptor.spatial_bounds();
        // let point_of_origin = spatial_bounds.upper_left();
        // let z0_resolution = spatial_bounds.size_x() / tile_width as f64;
        // let expected_crs = crs_from_spatial_reference_option(descriptor.spatial_reference)
        //     .unwrap()
        //     .to_string();

        // let tiling_specification = app_ctx
        //     .session_context(app_ctx.session_by_id(session_id).await.unwrap())
        //     .unwrap()
        //     .execution_context()
        //     .unwrap()
        //     .tiling_specification();

        let execution_context = ctx.execution_context().unwrap();
        let tiling_grid_definition =
            descriptor.tiling_grid_definition(execution_context.tiling_specification());

        let tiling_strategy = tiling_grid_definition.generate_data_tiling_strategy();

        let (x_indices, y_indices) = tiling_strategy
            .tile_idx_iterator_from_grid_bounds(tiling_grid_definition.tiling_grid_bounds())
            .fold(
                (HashSet::<isize>::new(), HashSet::<isize>::new()),
                |acc, tile_idx| {
                    let (mut x_indices, mut y_indices) = acc;
                    x_indices.insert(tile_idx.x());
                    y_indices.insert(tile_idx.y());
                    (x_indices, y_indices)
                },
            );

        let req = test::TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/{CUSTOM_TILE_MATRIX_SET_ID}"
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
        assert_eq!(
            body["crs"],
            serde_json::json!("http://www.opengis.net/def/crs/EPSG/0/4326")
        );

        let tile_matrices = body["tileMatrices"]
            .as_array()
            .expect("tileMatrices should be an array");

        assert_eq!(
            tile_matrices[0],
            serde_json::json!({
                "id": "0",
                "scaleDenominator": 39_756_960.997_597_71,
                "cellSize": 0.1,
                "cornerOfOrigin": "topLeft",
                "pointOfOrigin": [-180., 90.],
                "tileWidth": 512,
                "tileHeight": 512,
                "matrixWidth": x_indices.len(), // 8
                "matrixHeight": y_indices.len(), // 4
            })
        );

        // TODO: test more levels
    }

    #[ge_context::test]
    async fn it_returns_not_found_for_unknown_tile_matrix_set(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) = session_and_layer_id(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/UnknownTMS"
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
