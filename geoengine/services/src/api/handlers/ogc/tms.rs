use crate::{
    api::{
        handlers::ogc::{
            OgcApiResult,
            tms_spec::{
                CustomNativeTMS, CustomWebMercatorTMS, TileMatrixSetProvider,
                TypedTileMatrixSetProvider, WebMercatorQuadTMS,
            },
            util::{
                ensure_layer_exists, get_initialized_raster_operator, link_creator, load_layer,
                reproject_if_necessary,
            },
        },
        model::datatypes::{DataProviderId, LayerId},
    },
    contexts::{ApplicationContext, SessionContext},
};
use actix_web::web;
use geoengine_operators::engine::ExecutionContext;
use ogcapi_types::{
    common::{link_rel::SELF, media_type::JSON},
    tiles::{TileMatrixSet, TileMatrixSetId, TileMatrixSetItem, TileMatrixSets},
};

/// OGC API Tile Matrix Set List
///
/// Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).
/// Cf. [OGC Two Dimensional Tile Matrix Set and Tile Set Metadata](https://docs.ogc.org/is/17-083r4/17-083r4.html).
#[utoipa::path(
    tag = "OGC API",
    get,
    path = "/{dataConnectorId}/{layerId}/tileMatrixSets",
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
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId)>,
) -> OgcApiResult<web::Json<TileMatrixSets>> {
    let (data_connector_id, layer_id) = path.into_inner();

    ensure_layer_exists::<C>(&app_ctx, session, data_connector_id, layer_id.clone()).await?;

    let create_link = link_creator(data_connector_id, layer_id.clone());

    Ok(web::Json(TileMatrixSets {
        tile_matrix_sets: vec![
            // Always include custom TMS
            TileMatrixSetItem {
                id: Some(TileMatrixSetId::Custom(
                    CustomNativeTMS::TILE_MATRIX_SET_ID.to_string(),
                )),
                title: Some(CustomNativeTMS::TILE_MATRIX_SET_TITLE.to_string()),
                uri: None,
                crs: None,
                links: vec![create_link(
                    &format!(
                        "tileMatrixSets/{TILE_MATRIX_SET_ID}",
                        TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
                    ),
                    SELF,
                    JSON,
                )?],
            },
            // Add custom WebMercator TMS
            TileMatrixSetItem {
                id: Some(TileMatrixSetId::Custom(
                    CustomWebMercatorTMS::TILE_MATRIX_SET_ID.to_string(),
                )),
                title: Some(CustomWebMercatorTMS::TILE_MATRIX_SET_TITLE.to_string()),
                uri: None,
                crs: None,
                links: vec![create_link(
                    &format!(
                        "tileMatrixSets/{TILE_MATRIX_SET_ID}",
                        TILE_MATRIX_SET_ID = CustomWebMercatorTMS::TILE_MATRIX_SET_ID
                    ),
                    SELF,
                    JSON,
                )?],
            },
            // Add WebMercatorQuad for compatibility
            TileMatrixSetItem {
                id: Some(TileMatrixSetId::WebMercatorQuad),
                title: Some(WebMercatorQuadTMS::TILE_MATRIX_SET_TITLE.to_string()),
                uri: Some(WebMercatorQuadTMS::TILE_MATRIX_SET_URI.to_string()),
                crs: None,
                links: vec![create_link(
                    &format!(
                        "tileMatrixSets/{TILE_MATRIX_SET_ID}",
                        TILE_MATRIX_SET_ID = WebMercatorQuadTMS::TILE_MATRIX_SET_ID
                    ),
                    SELF,
                    JSON,
                )?],
            },
        ],
    }))
}

/// OGC API Tile Matrix Set Definition
///
/// Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).
/// Cf. [OGC Two Dimensional Tile Matrix Set and Tile Set Metadata](https://docs.ogc.org/is/17-083r4/17-083r4.html).
#[utoipa::path(
    tag = "OGC API",
    get,
    path = "/{dataConnectorId}/{layerId}/tileMatrixSets/{tileMatrixSetId}",
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

    let ctx = app_ctx.session_context(session);
    let execution_context = ctx.execution_context()?;

    let mut layer = load_layer::<C>(&ctx, data_connector_id, layer_id.clone()).await?;
    let mut initialized_operator =
        get_initialized_raster_operator::<C::SessionContext>(&layer, &execution_context).await?;

    let tiling_specification = execution_context.tiling_specification();

    let required_origin_and_resolution =
        TypedTileMatrixSetProvider::required_origin_and_resolution(
            &tile_matrix_set_id,
            initialized_operator.result_descriptor(),
        )?;
    reproject_if_necessary::<C::SessionContext>(
        &mut layer,
        &mut initialized_operator,
        &execution_context,
        TypedTileMatrixSetProvider::required_srs(&tile_matrix_set_id),
        required_origin_and_resolution,
    )
    .await?;

    // Resolve the appropriate TMS provider
    let provider = TypedTileMatrixSetProvider::resolve(
        &tile_matrix_set_id,
        initialized_operator.result_descriptor(),
        tiling_specification,
    )?;

    Ok(web::Json(provider.tile_matrix_set_definition()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::handlers::ogc::{
            test_util::{session_and_3857_layer_id, session_and_4326_layer_id},
            tms_spec::calculate_number_of_zoom_levels,
            util::raster_workflow_metadata,
        },
        contexts::{
            ApplicationContext, ExecutionContextImpl, PostgresContext, PostgresDb, SessionContext,
        },
        ge_context,
        layers::listing::LayerCollectionProvider,
        util::tests::{read_body_json, send_test_request},
    };
    use actix_web::{http::header, test::TestRequest};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::raster::GridShape2D;
    use geoengine_operators::engine::RasterResultDescriptor;
    use ogcapi_types::{
        common::Crs,
        tiles::{CornerOfOrigin, TileMatrix, TilesCrs},
    };
    use pretty_assertions::assert_eq;
    use std::collections::HashSet;
    use tokio_postgres::NoTls;

    #[ge_context::test]
    async fn it_lists_custom_tile_matrix_set(app_ctx: PostgresContext<NoTls>) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;

        let req = TestRequest::get()
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
                        "id":  CustomNativeTMS::TILE_MATRIX_SET_ID,
                        "title": CustomNativeTMS::TILE_MATRIX_SET_TITLE,
                        "links": [
                            {
                                "href": format!(
                                    "{server_url}/api/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/{TILE_MATRIX_SET_ID}", TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
                                ),
                                "rel": "self",
                                "type": "application/json"
                            }
                        ]
                    },
                    {
                        "id":  CustomWebMercatorTMS::TILE_MATRIX_SET_ID,
                        "title": CustomWebMercatorTMS::TILE_MATRIX_SET_TITLE,
                        "links": [
                            {
                                "href": format!(
                                    "{server_url}/api/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/{TILE_MATRIX_SET_ID}", TILE_MATRIX_SET_ID = CustomWebMercatorTMS::TILE_MATRIX_SET_ID
                                ),
                                "rel": "self",
                                "type": "application/json"
                            }
                        ]
                    },
                    {
                        "id": WebMercatorQuadTMS::TILE_MATRIX_SET_ID,
                        "title": WebMercatorQuadTMS::TILE_MATRIX_SET_TITLE,
                        "uri": WebMercatorQuadTMS::TILE_MATRIX_SET_URI,
                        "links": [
                            {
                                "href": format!(
                                    "{server_url}/api/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/{TILE_MATRIX_SET_ID}", TILE_MATRIX_SET_ID = WebMercatorQuadTMS::TILE_MATRIX_SET_ID
                                ),
                                "rel": "self",
                                "type": "application/json"
                            }
                        ]
                    },
                ]
            })
        );
    }

    fn tiling_grid_size(
        execution_context: &ExecutionContextImpl<PostgresDb<NoTls>>,
        descriptor: &RasterResultDescriptor,
    ) -> GridShape2D {
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

        GridShape2D::new_2d(y_indices.len(), x_indices.len())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Test should be comprehensive and readable"
    )]
    #[ge_context::test]
    async fn it_returns_custom_4326_tile_matrix_set_definition(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;
        let session = app_ctx.session_by_id(session_id).await.unwrap();
        let ctx = app_ctx.session_context(session);
        let execution_context = ctx.execution_context().unwrap();
        let layer = ctx.db().load_layer(&layer_id.clone().into()).await.unwrap();
        let descriptor =
            raster_workflow_metadata::<crate::contexts::PostgresSessionContext<NoTls>>(
                layer.workflow,
                execution_context,
            )
            .await
            .unwrap();

        let req = TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/{TILE_MATRIX_SET_ID}",
                TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        let tile_matrix_set = serde_json::from_value::<TileMatrixSet>(body)
            .expect("Response body should be a valid TileMatrixSet JSON");

        assert_eq!(
            tile_matrix_set,
            TileMatrixSet {
                id: TileMatrixSetId::Custom(CustomNativeTMS::TILE_MATRIX_SET_ID.to_string()),
                title: Some(CustomNativeTMS::TILE_MATRIX_SET_TITLE.to_string()),
                description: None,
                keywords: vec![],
                uri: None,
                crs: TilesCrs::Simple(Crs::from_epsg(4326)),
                ordered_axes: ["Lon", "Lat"].iter().map(ToString::to_string).collect(),
                well_known_scale_set: None,
                bounding_box: None,
                tile_matrices: vec![
                    TileMatrix {
                        id: "0".to_string(),
                        title: None,
                        description: None,
                        keywords: vec![],
                        scale_denominator: 159_027_844.000_000_03,
                        cell_size: 0.4,
                        corner_of_origin: CornerOfOrigin::TopLeft,
                        point_of_origin: [-204.8, 204.8],
                        tile_width: 512.try_into().unwrap(),
                        tile_height: 512.try_into().unwrap(),
                        matrix_width: 2.try_into().unwrap(),
                        matrix_height: 2.try_into().unwrap(),
                        variable_matrix_widths: vec![],
                    },
                    TileMatrix {
                        id: "1".to_string(),
                        title: None,
                        description: None,
                        keywords: vec![],
                        scale_denominator: 79_513_922.000_000_01,
                        cell_size: 0.2,
                        corner_of_origin: CornerOfOrigin::TopLeft,
                        point_of_origin: [-204.8, 102.4],
                        tile_width: 512.try_into().unwrap(),
                        tile_height: 512.try_into().unwrap(),
                        matrix_width: 4.try_into().unwrap(),
                        matrix_height: 2.try_into().unwrap(),
                        variable_matrix_widths: vec![],
                    },
                    TileMatrix {
                        id: "2".to_string(),
                        title: None,
                        description: None,
                        keywords: vec![],
                        scale_denominator: 39_756_961.000_000_01,
                        cell_size: 0.1,
                        corner_of_origin: CornerOfOrigin::TopLeft,
                        point_of_origin: [-204.8, 102.4],
                        tile_width: 512.try_into().unwrap(),
                        tile_height: 512.try_into().unwrap(),
                        matrix_width: 8.try_into().unwrap(),
                        matrix_height: 4.try_into().unwrap(),
                        variable_matrix_widths: vec![],
                    },
                ],
            }
        );

        let execution_context = ctx.execution_context().unwrap();

        assert_eq!(
            tiling_grid_size(&execution_context, &descriptor),
            GridShape2D::new_2d(4, 8)
        );

        assert_eq!(
            calculate_number_of_zoom_levels(&descriptor, &execution_context.tiling_specification()),
            3
        );

        // for tile_matrix in &tile_matrix_set.tile_matrices {
        //     let zoom_level = tile_matrix_set.tile_matrices.len() as u32
        //         - tile_matrix.id.parse::<u32>().unwrap()
        //         - 1;
        //     let expected_tiles = calculate_tiles_at_zoom_level(
        //         &descriptor,
        //         &execution_context.tiling_specification(),
        //         zoom_level,
        //     );
        //     let actual_tiles = GridShape2D::new_2d(
        //         tile_matrix.matrix_height.get() as usize,
        //         tile_matrix.matrix_width.get() as usize,
        //     );
        //     assert_eq!(
        //         expected_tiles, actual_tiles,
        //         "Tile count at zoom level {zoom_level} does not match expected value"
        //     );
        // }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Test should be comprehensive and readable"
    )]
    #[ge_context::test]
    async fn it_returns_custom_3857_tile_matrix_set_definition(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) = session_and_3857_layer_id(&app_ctx).await;
        let session = app_ctx.session_by_id(session_id).await.unwrap();
        let ctx = app_ctx.session_context(session);
        let execution_context = ctx.execution_context().unwrap();
        let layer = ctx.db().load_layer(&layer_id.clone().into()).await.unwrap();
        let descriptor =
            raster_workflow_metadata::<crate::contexts::PostgresSessionContext<NoTls>>(
                layer.workflow,
                execution_context,
            )
            .await
            .unwrap();

        let req = TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/{TILE_MATRIX_SET_ID}",
                TILE_MATRIX_SET_ID = CustomNativeTMS::TILE_MATRIX_SET_ID
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        let tile_matrix_set = serde_json::from_value::<TileMatrixSet>(body)
            .expect("Response body should be a valid TileMatrixSet JSON");

        assert_eq!(
            tile_matrix_set,
            TileMatrixSet {
                id: TileMatrixSetId::Custom(CustomNativeTMS::TILE_MATRIX_SET_ID.to_string()),
                title: Some(CustomNativeTMS::TILE_MATRIX_SET_TITLE.to_string()),
                description: None,
                keywords: vec![],
                uri: None,
                crs: TilesCrs::Simple(Crs::from_epsg(3857)),
                ordered_axes: ["X", "Y"].iter().map(ToString::to_string).collect(),
                well_known_scale_set: None,
                bounding_box: None,
                tile_matrices: vec![
                    TileMatrix {
                        id: "0".to_string(),
                        title: None,
                        description: None,
                        keywords: vec![],
                        scale_denominator: 407_286_157.394_767_2,
                        cell_size: 114_040.124_070_534_8,
                        corner_of_origin: CornerOfOrigin::TopLeft,
                        point_of_origin: [-58_354_990.030_488_94, 58_418_366.631_411_66],
                        tile_width: 512.try_into().unwrap(),
                        tile_height: 512.try_into().unwrap(),
                        matrix_width: 2.try_into().unwrap(),
                        matrix_height: 2.try_into().unwrap(),
                        variable_matrix_widths: vec![],
                    },
                    TileMatrix {
                        id: "1".to_string(),
                        title: None,
                        description: None,
                        keywords: vec![],
                        scale_denominator: 203_643_078.697_383_6,
                        cell_size: 57_020.062_035_267_394,
                        corner_of_origin: CornerOfOrigin::TopLeft,
                        point_of_origin: [-29_217_738.330_467_295, 29_167_074.807_319_485],
                        tile_width: 512.try_into().unwrap(),
                        tile_height: 512.try_into().unwrap(),
                        matrix_width: 2.try_into().unwrap(),
                        matrix_height: 2.try_into().unwrap(),
                        variable_matrix_widths: vec![],
                    },
                    TileMatrix {
                        id: "2".to_string(),
                        title: None,
                        description: None,
                        keywords: vec![],
                        scale_denominator: 101_821_539.348_691_8,
                        cell_size: 28_510.031_017_633_697,
                        corner_of_origin: CornerOfOrigin::TopLeft,
                        point_of_origin: [-29_189_228.299_449_66, 29_195_584.838_337_12],
                        tile_width: 512.try_into().unwrap(),
                        tile_height: 512.try_into().unwrap(),
                        matrix_width: 4.try_into().unwrap(),
                        matrix_height: 4.try_into().unwrap(),
                        variable_matrix_widths: vec![],
                    },
                    TileMatrix {
                        id: "3".to_string(),
                        title: None,
                        description: None,
                        keywords: vec![],
                        scale_denominator: 50_910_769.674_345_896,
                        cell_size: 14_255.015_508_816_849,
                        corner_of_origin: CornerOfOrigin::TopLeft,
                        point_of_origin: [-21_890_660.358_935_43, 21_897_016.897_822_894],
                        tile_width: 512.try_into().unwrap(),
                        tile_height: 512.try_into().unwrap(),
                        matrix_width: 6.try_into().unwrap(),
                        matrix_height: 6.try_into().unwrap(),
                        variable_matrix_widths: vec![],
                    },
                ],
            }
        );

        let execution_context = ctx.execution_context().unwrap();

        assert_eq!(
            tiling_grid_size(&execution_context, &descriptor),
            GridShape2D::new_2d(6, 6)
        );

        assert_eq!(
            calculate_number_of_zoom_levels(&descriptor, &execution_context.tiling_specification()),
            4
        );

        // for tile_matrix in &tile_matrix_set.tile_matrices {
        //     let zoom_level = tile_matrix_set.tile_matrices.len() as u32
        //         - tile_matrix.id.parse::<u32>().unwrap()
        //         - 1;
        //     let expected_tiles = calculate_tiles_at_zoom_level(
        //         &descriptor,
        //         &execution_context.tiling_specification(),
        //         zoom_level,
        //     );
        //     let actual_tiles = GridShape2D::new_2d(
        //         tile_matrix.matrix_height.get() as usize,
        //         tile_matrix.matrix_width.get() as usize,
        //     );
        //     assert_eq!(
        //         expected_tiles, actual_tiles,
        //         "Tile count at zoom level {zoom_level} does not match expected value"
        //     );
        // }
    }

    #[ge_context::test]
    async fn it_returns_not_found_for_unknown_tile_matrix_set(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;

        let req = TestRequest::get()
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

    #[allow(
        clippy::too_many_lines,
        reason = "Test should be comprehensive and readable"
    )]
    #[ge_context::test]
    async fn it_returns_a_webmercator_quad_tile_matrix_set_definition(
        app_ctx: PostgresContext<NoTls>,
    ) {
        let (session_id, data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;

        let req = TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/WebMercatorQuad"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        let tile_matrix_set = serde_json::from_value::<TileMatrixSet>(body)
            .expect("Response body should be a valid TileMatrixSet JSON");

        // from <https://raw.githubusercontent.com/opengeospatial/2D-Tile-Matrix-Set/master/registry/json/WebMercatorQuad.json>
        let mut expected_tile_matrix_set = serde_json::from_str::<TileMatrixSet>(include_str!(
            "../../../../../test_data/ogc/tiles/WebMercatorQuad.json"
        ))
        .expect("Response body should be a valid TileMatrixSet JSON");
        expected_tile_matrix_set.crs = TilesCrs::Uri {
            uri: "http://www.opengis.net/def/crs/EPSG/0/3857".to_string(),
        }; // TODO: fix deserialization in `ogcapi` crate to handle this correctly

        assert_eq!(tile_matrix_set, expected_tile_matrix_set);
    }
}
