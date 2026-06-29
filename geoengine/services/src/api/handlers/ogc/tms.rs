use crate::{
    api::{
        handlers::{
            ogc::{
                OgcApiResult,
                error::OgcApiError,
                util::{
                    crs_from_spatial_reference_option, link_creator, load_layer,
                    raster_workflow_metadata,
                },
            },
            spatial_references::{AxisOrder, spatial_reference_specification},
        },
        model::datatypes::{DataProviderId, LayerId},
    },
    contexts::{ApplicationContext, SessionContext},
};
use actix_web::web;
use float_cmp::approx_eq;
use geoengine_datatypes::{
    primitives::{Coordinate2D, SpatialResolution},
    raster::{GridBounds, GridShape2D, GridShapeAccess, GridSize, TilingSpecification},
    spatial_reference::SpatialReference,
};
use geoengine_operators::engine::{ExecutionContext, RasterResultDescriptor};
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
use tracing::warn;

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
        ordered_axes: ordered_axes(spatial_reference)?,
        well_known_scale_set: None,
        bounding_box: None, // TODO: BBox of layer?
        tile_matrices: build_tile_matrices(&descriptor, tiling_specification, spatial_reference)?,
    }))
}

fn ordered_axes(spatial_reference: SpatialReference) -> OgcApiResult<Vec<String>> {
    let specification = spatial_reference_specification(spatial_reference.into())?;

    Ok(match specification.axis_order {
        Some(AxisOrder::NorthEast) => vec!["Lon".into(), "Lat".into()],
        _ => vec!["x".into(), "y".into()],
    })
}

pub fn build_tile_matrices(
    result_descriptor: &RasterResultDescriptor,
    tiling_specification: TilingSpecification,
    spatial_reference: SpatialReference,
) -> OgcApiResult<Vec<TileMatrix>> {
    let [tile_width, tile_height] = tiling_specification.grid_shape_array();
    let tile_width = to_non_zero_u16(tile_width);
    let tile_height = to_non_zero_u16(tile_height);

    let tiling_spatial_grid_definition =
        result_descriptor.tiling_grid_definition(tiling_specification);
    let tiling_geo_transform = tiling_spatial_grid_definition.tiling_geo_transform();
    let (x_resolution, y_resolution) = (
        tiling_geo_transform.x_pixel_size(),
        tiling_geo_transform.y_pixel_size(),
    );

    if !approx_eq!(f64, x_resolution.abs(), y_resolution.abs()) {
        warn!(
            "Non-square pixels detected (x resolution: {x_resolution}, y resolution: {y_resolution})"
        );
    }
    let resolution = f64::min(x_resolution.abs(), y_resolution.abs());

    let meters_per_unit = spatial_reference.meters_per_unit().map_err(|source| {
        OgcApiError::UnknownSpatialReferenceInfo {
            source: Box::new(source),
            from: spatial_reference.into(),
            info: "meters per unit".into(),
        }
    })?;

    let max_zoom_level =
        calculate_number_of_zoom_levels(result_descriptor, &tiling_specification) - 1;
    let base_scale_denominator = scale_denominator(resolution, meters_per_unit);

    Ok((0_u32..=max_zoom_level)
        .rev()
        .zip(
            calculate_tiles_for_zoom_levels(
                result_descriptor,
                &tiling_specification,
                max_zoom_level,
            )
            .into_iter()
            .rev(),
        )
        .map(|(zoom_level, (grid_shape, origin_coordinate))| {
            (
                zoom_level,
                2_u64.saturating_pow(zoom_level) as f64,
                grid_shape,
                origin_coordinate,
            )
        })
        .map(
            |(zoom_level, multiple, grid_shape, origin_coordinate)| TileMatrix {
                id: (max_zoom_level - zoom_level).to_string(),
                title: None,
                description: None,
                keywords: Vec::new(),
                scale_denominator: multiple * base_scale_denominator,
                cell_size: multiple * resolution,
                corner_of_origin: CornerOfOrigin::TopLeft,
                point_of_origin: [origin_coordinate.x, origin_coordinate.y],
                tile_width,
                tile_height,
                matrix_width: to_non_zero_u64(grid_shape.x()),
                matrix_height: to_non_zero_u64(grid_shape.y()),
                variable_matrix_widths: Vec::new(),
            },
        )
        .collect::<Vec<_>>())
}

fn to_non_zero_u16(value: usize) -> NonZeroU16 {
    let value = u16::try_from(value).unwrap_or(u16::MAX);
    NonZeroU16::new(value.max(1)).unwrap_or(NonZeroU16::MIN)
}

fn to_non_zero_u64(value: usize) -> NonZeroU64 {
    NonZeroU64::try_from(value as u64).unwrap_or(NonZeroU64::MIN)
}

/// The documentation (<https://docs.ogc.org/is/17-083r4/17-083r4.html#6-1-1-1-%C2%A0-tile-matrix-in-a-two-dimensional-space>) says:
/// `cellSize = scaleDenominator × 0.2810^{−3} / metersPerUnit(crs)`
/// Thus, we have to calculate:
/// `scaleDenominator = cellSize × metersPerUnit(crs) / 0.2810^{−3}`
fn scale_denominator(cell_size: f64, meters_per_unit: f64) -> f64 {
    cell_size * meters_per_unit / STANDARD_PIXEL_SIZE_METERS
}

/// Calculates the number of zoom levels for a tile matrix set based on the original resolution of the raster and the tile size.
pub fn calculate_number_of_zoom_levels(
    result_descriptor: &RasterResultDescriptor,
    tile_size: &TilingSpecification,
) -> u32 {
    fn levels_per_axis(grid_size: usize, tile_size: usize) -> u32 {
        let tiles_at_max_resolution = grid_size.div_ceil(tile_size);

        if tiles_at_max_resolution <= 1 {
            return 1;
        }

        // equivalent to: ceil(log2(tiles)) using bit operations
        let transitions = usize::BITS - (tiles_at_max_resolution - 1).leading_zeros();

        transitions + 1
    }

    let grid_shape = result_descriptor.spatial_grid_descriptor().grid_shape();
    let [x_size, y_size] = [grid_shape.x(), grid_shape.y()];
    let [tile_size_x, tile_size_y] = [
        tile_size.tile_size_in_pixels.x(),
        tile_size.tile_size_in_pixels.y(),
    ];

    // we stop when one level is 1
    u32::min(
        levels_per_axis(x_size, tile_size_x),
        levels_per_axis(y_size, tile_size_y),
    )
}

/// Calculates the actual number of tiles and origin coordinate for a specific zoom level accounting for origin alignment.
/// This method computes tiles without creating operators by using
/// `SpatialGridDefinition::with_changed_resolution` and tiling definitions.
///
/// Zoom level 0 corresponds to the original resolution of the raster.
///
/// Returns a tuple of (`grid_shape`, `origin_coordinate`) where `origin_coordinate` is the upper-left coordinate of the first tile.
pub fn calculate_tiles_for_zoom_level(
    result_descriptor: &RasterResultDescriptor,
    tiling_specification: &TilingSpecification,
    zoom_level: u32,
) -> (GridShape2D, Coordinate2D) {
    let original_spatial_grid = result_descriptor.spatial_grid_descriptor();
    let original_resolution = original_spatial_grid.geo_transform().spatial_resolution();

    let zoom_factor = 2u32.pow(zoom_level);

    // Compute downsampled resolution
    let downsampled_resolution = SpatialResolution {
        x: original_resolution.x * f64::from(zoom_factor),
        y: original_resolution.y * f64::from(zoom_factor),
    };

    // Create the spatial grid at this zoom level's resolution
    let downsampled_grid = original_spatial_grid.with_changed_resolution(downsampled_resolution);

    // Create the tiling definition for this resolution
    let tiling_def = downsampled_grid.tiling_grid_definition(*tiling_specification);

    // Calculate actual tiles (accounts for origin alignment)
    let grid_bounds = tiling_def.tiling_grid_bounds();
    let tiling_geo_transform = tiling_def.tiling_geo_transform();
    let tiling_strategy = tiling_def.generate_data_tiling_strategy();
    let tile_bounds = tiling_strategy.global_pixel_grid_bounds_to_tile_grid_bounds(grid_bounds);
    let grid_shape = GridShape2D::new(tile_bounds.axis_size());

    // Calculate the origin coordinate of the first tile
    let min_pixel_index = tiling_strategy.tile_idx_to_global_pixel_idx(tile_bounds.min_index());
    let origin_coordinate =
        tiling_geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d(min_pixel_index);

    (grid_shape, origin_coordinate)
}

/// Calculates the actual number of tiles and origin coordinate at each zoom level accounting for origin alignment.
/// This method computes tiles for all zoom levels without creating operators by using
/// `SpatialGridDefinition::with_changed_resolution` and tiling definitions.
///
/// Returns a vector where index 0 corresponds to `zoom_level` 0 (native resolution).
/// Each element contains the grid shape (tile count) and the upper-left coordinate of the first tile.
pub fn calculate_tiles_for_zoom_levels(
    result_descriptor: &RasterResultDescriptor,
    tiling_specification: &TilingSpecification,
    max_zoom_level: u32,
) -> Vec<(GridShape2D, Coordinate2D)> {
    let original_spatial_grid = result_descriptor.spatial_grid_descriptor();
    let original_resolution = original_spatial_grid.geo_transform().spatial_resolution();

    let mut tiles_at_each_level = Vec::new();

    for zoom_level in 0..=max_zoom_level {
        let zoom_factor = 2u32.pow(zoom_level);

        // Compute downsampled resolution
        let downsampled_resolution = SpatialResolution {
            x: original_resolution.x * f64::from(zoom_factor),
            y: original_resolution.y * f64::from(zoom_factor),
        };

        // Create the spatial grid at this zoom level's resolution
        let downsampled_grid =
            original_spatial_grid.with_changed_resolution(downsampled_resolution);

        // Create the tiling definition for this resolution
        let tiling_def = downsampled_grid.tiling_grid_definition(*tiling_specification);

        // Calculate actual tiles (accounts for origin alignment)
        let grid_bounds = tiling_def.tiling_grid_bounds();
        let tiling_geo_transform = tiling_def.tiling_geo_transform();
        let tiling_strategy = tiling_def.generate_data_tiling_strategy();
        let tile_bounds = tiling_strategy.global_pixel_grid_bounds_to_tile_grid_bounds(grid_bounds);
        let actual_tiles = GridShape2D::new(tile_bounds.axis_size());

        // Calculate the origin coordinate of the first tile
        let min_pixel_index = tiling_strategy.tile_idx_to_global_pixel_idx(tile_bounds.min_index());
        let origin_coordinate =
            tiling_geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d(min_pixel_index);

        tiles_at_each_level.push((actual_tiles, origin_coordinate));
    }

    tiles_at_each_level
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::handlers::ogc::util::{
            get_initialized_raster_operator, raster_operator_in_fitting_resolution,
        },
        contexts::{
            ApplicationContext, ExecutionContextImpl, PostgresContext, PostgresDb,
            PostgresSessionContext, Session, SessionContext, SessionId,
        },
        ge_context,
        layers::{layer::Layer, listing::LayerCollectionProvider},
        util::tests::{
            add_file_definition_to_datasets_and_return_layer, add_ndvi_3857_to_layers,
            add_ndvi_to_layers, admin_login, ndvi_255_symbology, read_body_json, send_test_request,
        },
    };
    use actix_web::{http::header, test::TestRequest};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::test_data;
    use geoengine_operators::engine::RasterResultDescriptor;
    use ogcapi_types::common::Crs;
    use pretty_assertions::assert_eq;
    use std::collections::HashSet;
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

    async fn session_and_native_3857_layer_id(
        app_ctx: &PostgresContext<NoTls>,
    ) -> (SessionId, DataProviderId, LayerId) {
        let session = admin_login(app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = ctx.session().id();
        let (data_connector_id, layer_id) = add_file_definition_to_datasets_and_return_layer(
            &ctx.db(),
            test_data!("dataset_defs/ndvi (3587).json"),
            Some(ndvi_255_symbology()),
        )
        .await;

        (session_id, data_connector_id.into(), layer_id.into())
    }

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
        let descriptor = super::raster_workflow_metadata::<
            crate::contexts::PostgresSessionContext<NoTls>,
        >(layer.workflow, execution_context)
        .await
        .unwrap();

        let req = TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/{CUSTOM_TILE_MATRIX_SET_ID}"
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
                id: TileMatrixSetId::Custom(CUSTOM_TILE_MATRIX_SET_ID.to_string()),
                title: Some(CUSTOM_TILE_MATRIX_SET_TITLE.to_string()),
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
        let descriptor = super::raster_workflow_metadata::<
            crate::contexts::PostgresSessionContext<NoTls>,
        >(layer.workflow, execution_context)
        .await
        .unwrap();

        let req = TestRequest::get()
            .uri(&format!(
                "/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/{CUSTOM_TILE_MATRIX_SET_ID}"
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
                id: TileMatrixSetId::Custom(CUSTOM_TILE_MATRIX_SET_ID.to_string()),
                title: Some(CUSTOM_TILE_MATRIX_SET_TITLE.to_string()),
                description: None,
                keywords: vec![],
                uri: None,
                crs: TilesCrs::Simple(Crs::from_epsg(3857)),
                ordered_axes: ["x", "y"].iter().map(ToString::to_string).collect(),
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

    #[test]
    fn it_returns_an_axis_order() {
        assert_eq!(
            ordered_axes(SpatialReference::epsg_4326()).unwrap(),
            vec!["Lon".to_string(), "Lat".to_string()]
        );
        assert_eq!(
            ordered_axes(SpatialReference::web_mercator()).unwrap(),
            vec!["x".to_string(), "y".to_string()]
        );
    }

    /// Test helper: computes tiles at each zoom level for a given layer.
    /// Returns a vector where index corresponds to `zoom_level`, with clear visibility of zoom levels.
    fn get_tiles_for_all_zoom_levels_with_details(
        ctx: &crate::contexts::PostgresSessionContext<NoTls>,
        descriptor: &RasterResultDescriptor,
    ) -> Vec<(u32, GridShape2D)> {
        let execution_context = ctx.execution_context().unwrap();
        let tiling_spec = execution_context.tiling_specification();
        let max_zoom_level = calculate_number_of_zoom_levels(descriptor, &tiling_spec) - 1;

        let tiles = calculate_tiles_for_zoom_levels(descriptor, &tiling_spec, max_zoom_level);

        // Return tuples of (zoom_level, tiles) for clear visibility
        (0..=max_zoom_level)
            .map(|zoom_level| (zoom_level, tiles[zoom_level as usize].0))
            .collect()
    }

    /// Test helper: verifies tiles computed by the method match actual operator initialization.
    async fn assert_tiles_match_operators(
        ctx: &crate::contexts::PostgresSessionContext<NoTls>,
        layer: &Layer,
        tiles_from_method: &[(u32, GridShape2D)],
    ) {
        let execution_context = ctx.execution_context().unwrap();
        let tiling_spec = execution_context.tiling_specification();

        for (zoom_level, expected_tiles) in tiles_from_method {
            let zoom_factor = 2u32.pow(*zoom_level);

            // Initialize operator and resample to the target resolution
            let mut initialized_operator = get_initialized_raster_operator::<
                PostgresSessionContext<NoTls>,
            >(layer, &execution_context)
            .await
            .expect("Failed to initialize operator");

            initialized_operator = raster_operator_in_fitting_resolution::<
                PostgresSessionContext<NoTls>,
            >(
                initialized_operator, &execution_context, zoom_factor
            )
            .await
            .expect("Failed to resample operator to fitting resolution");

            // Calculate actual tiles from the initialized operator's descriptor
            let resampled_descriptor = initialized_operator.result_descriptor();
            let resampled_tiling_def = resampled_descriptor.tiling_grid_definition(tiling_spec);
            let resampled_grid_bounds = resampled_tiling_def.tiling_grid_bounds();
            let resampled_tiling_strategy = resampled_tiling_def.generate_data_tiling_strategy();
            let resampled_tile_bounds = resampled_tiling_strategy
                .global_pixel_grid_bounds_to_tile_grid_bounds(resampled_grid_bounds);
            let actual_from_operator = GridShape2D::new(resampled_tile_bounds.axis_size());

            // Compare: tiles from method should match tiles from initialized operator
            assert_eq!(
                expected_tiles, &actual_from_operator,
                "Zoom level {}: expected tiles {:?}, but operator returned {:?}",
                zoom_level, expected_tiles, actual_from_operator
            );
        }
    }

    #[ge_context::test]
    async fn it_calculates_tiles_for_4326_layer_with_zoom_levels(app_ctx: PostgresContext<NoTls>) {
        let (session_id, _data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;
        let session = app_ctx.session_by_id(session_id).await.unwrap();
        let ctx = app_ctx.session_context(session);
        let execution_context = ctx.execution_context().unwrap();
        let layer = ctx.db().load_layer(&layer_id.clone().into()).await.unwrap();
        let descriptor =
            raster_workflow_metadata::<crate::contexts::PostgresSessionContext<NoTls>>(
                layer.workflow.clone(),
                execution_context,
            )
            .await
            .unwrap();

        let tiles_with_levels = get_tiles_for_all_zoom_levels_with_details(&ctx, &descriptor);

        assert_eq!(
            tiles_with_levels,
            vec![
                (0, GridShape2D::new_2d(4, 8)),
                (1, GridShape2D::new_2d(2, 4)),
                (2, GridShape2D::new_2d(2, 2)),
            ]
        );

        // Verify against actual operator initialization
        assert_tiles_match_operators(&ctx, &layer, &tiles_with_levels).await;
    }

    #[ge_context::test]
    async fn it_calculates_tiles_for_3857_layer_with_zoom_levels(app_ctx: PostgresContext<NoTls>) {
        let (session_id, _data_connector_id, layer_id) = session_and_3857_layer_id(&app_ctx).await;
        let session = app_ctx.session_by_id(session_id).await.unwrap();
        let ctx = app_ctx.session_context(session);
        let execution_context = ctx.execution_context().unwrap();
        let layer = ctx.db().load_layer(&layer_id.clone().into()).await.unwrap();
        let descriptor =
            raster_workflow_metadata::<crate::contexts::PostgresSessionContext<NoTls>>(
                layer.workflow.clone(),
                execution_context,
            )
            .await
            .unwrap();

        let tiles_with_levels = get_tiles_for_all_zoom_levels_with_details(&ctx, &descriptor);

        assert_eq!(
            tiles_with_levels,
            vec![
                (0, GridShape2D::new_2d(6, 6)),
                (1, GridShape2D::new_2d(4, 4)),
                (2, GridShape2D::new_2d(2, 2)),
                (3, GridShape2D::new_2d(2, 2)),
            ]
        );

        // Verify against actual operator initialization
        assert_tiles_match_operators(&ctx, &layer, &tiles_with_levels).await;
    }

    #[ge_context::test]
    async fn it_calculates_tiles_for_native_3857_layer_with_zoom_levels(
        app_ctx: PostgresContext<NoTls>,
    ) {
        let (session_id, _data_connector_id, layer_id) =
            session_and_native_3857_layer_id(&app_ctx).await;
        let session = app_ctx.session_by_id(session_id).await.unwrap();
        let ctx = app_ctx.session_context(session);
        let execution_context = ctx.execution_context().unwrap();
        let layer = ctx.db().load_layer(&layer_id.clone().into()).await.unwrap();
        let descriptor =
            raster_workflow_metadata::<crate::contexts::PostgresSessionContext<NoTls>>(
                layer.workflow.clone(),
                execution_context,
            )
            .await
            .unwrap();

        let tiles_with_levels = get_tiles_for_all_zoom_levels_with_details(&ctx, &descriptor);

        assert_eq!(
            tiles_with_levels,
            vec![
                (0, GridShape2D::new_2d(6, 6)),
                (1, GridShape2D::new_2d(4, 4)),
                (2, GridShape2D::new_2d(2, 2)),
                (3, GridShape2D::new_2d(2, 2)),
            ]
        );

        // Verify against actual operator initialization
        assert_tiles_match_operators(&ctx, &layer, &tiles_with_levels).await;
    }
}
