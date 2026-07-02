use crate::{
    api::{
        handlers::{
            ogc::{
                OgcApiResult,
                error::OgcApiError,
                util::{
                    crs_from_spatial_reference_option, ensure_layer_exists, link_creator,
                    load_layer, raster_workflow_metadata,
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
    raster::{
        GridBoundingBox2D, GridBounds, GridIdx2D, GridShape2D, GridShapeAccess, GridSize,
        TilingSpatialGridDefinition, TilingSpecification,
    },
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

pub(super) const CUSTOM_TILE_MATRIX_SET_ID: &str = "Custom";
const CUSTOM_TILE_MATRIX_SET_TITLE: &str = "Custom Grid for Geo Engine";

pub(super) const WEBMERCATOR_TILE_MATRIX_SET_ID: &str = "WebMercatorQuad";
const WEBMERCATOR_TILE_MATRIX_SET_TITLE: &str = "Google Maps Compatible for the World";
const WEBMERCATOR_TILE_MATRIX_SET_URI: &str =
    "http://www.opengis.net/def/tilematrixset/OGC/1.0/WebMercatorQuad";

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
            },
            // Add WebMercatorQuad for compatibility
            TileMatrixSetItem {
                id: Some(TileMatrixSetId::WebMercatorQuad),
                title: Some(WEBMERCATOR_TILE_MATRIX_SET_TITLE.to_string()),
                uri: Some(WEBMERCATOR_TILE_MATRIX_SET_URI.to_string()),
                crs: None,
                links: vec![create_link(
                    &format!("tileMatrixSets/{WEBMERCATOR_TILE_MATRIX_SET_ID}"),
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

    let layer = load_layer::<C>(&ctx, data_connector_id, layer_id.clone()).await?;

    let tiling_specification = execution_context.tiling_specification();
    let descriptor =
        raster_workflow_metadata::<C::SessionContext>(layer.workflow, execution_context).await?;

    // Resolve the appropriate TMS provider
    let provider = TypedTileMatrixSetProvider::resolve(
        &tile_matrix_set_id,
        &descriptor,
        tiling_specification,
    )?;

    Ok(web::Json(provider.tile_matrix_set_definition()?))
}

/// Trait for Tile Matrix Set providers that support both metadata and tile rendering.
pub trait TileMatrixSetProvider: Send + Sync {
    /// Returns the tile matrix set ID
    fn id(&self) -> TileMatrixSetId;

    /// Returns the tile matrix set definition, including all tile matrices
    fn tile_matrix_set_definition(&self) -> OgcApiResult<TileMatrixSet>;

    /// Returns the tile matrix definitions for all zoom levels
    fn tile_matrices(&self) -> OgcApiResult<Vec<TileMatrix>>;

    /// Returns the CRS for this tile matrix set
    fn tiles_crs(&self) -> OgcApiResult<TilesCrs>;

    /// Returns the ordered axes labels (e.g., `["Lon", "Lat"]` or `["x", "y"]`)
    fn ordered_axes(&self) -> OgcApiResult<Vec<String>>;

    /// Validates that tile coordinates are within valid bounds for the given zoom level
    fn validate_tile_coordinates(
        &self,
        tile_matrix: u8,
        tile_row: u32,
        tile_col: u32,
    ) -> OgcApiResult<()>;

    /// Computes the pixel bounds of a tile in the global pixel grid
    fn tile_grid_bbox(
        &self,
        tile_matrix: u8,
        tile_row: u32,
        tile_col: u32,
    ) -> OgcApiResult<GridBoundingBox2D>;
}

/// A wrapper Tile Matrix Set Provider
pub enum TypedTileMatrixSetProvider {
    Custom(GeoEngineCustomTMS),
    WebMercatorQuad(WebMercatorQuadTMS),
}

impl TypedTileMatrixSetProvider {
    /// Factory function to create the appropriate TMS provider for a given ID and layer
    fn resolve(
        tile_matrix_set_id: &TileMatrixSetId,
        result_descriptor: &RasterResultDescriptor,
        tiling_specification: TilingSpecification,
    ) -> OgcApiResult<Self> {
        match tile_matrix_set_id {
            TileMatrixSetId::Custom(id) if id == CUSTOM_TILE_MATRIX_SET_ID => {
                let tms = GeoEngineCustomTMS::new(result_descriptor.clone(), tiling_specification)?;
                Ok(TypedTileMatrixSetProvider::Custom(tms))
            }
            TileMatrixSetId::WebMercatorQuad => Ok(TypedTileMatrixSetProvider::WebMercatorQuad(
                WebMercatorQuadTMS,
            )),
            TileMatrixSetId::Custom(_) => Err(OgcApiError::TileMatrixSetNotFound {
                tile_matrix_set_id: tile_matrix_set_id.to_string(),
            }),
        }
    }
}

impl TileMatrixSetProvider for TypedTileMatrixSetProvider {
    fn id(&self) -> TileMatrixSetId {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => provider.id(),
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => provider.id(),
        }
    }

    fn tile_matrix_set_definition(&self) -> OgcApiResult<TileMatrixSet> {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => provider.tile_matrix_set_definition(),
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => {
                provider.tile_matrix_set_definition()
            }
        }
    }

    fn tile_matrices(&self) -> OgcApiResult<Vec<TileMatrix>> {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => provider.tile_matrices(),
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => provider.tile_matrices(),
        }
    }

    fn tiles_crs(&self) -> OgcApiResult<TilesCrs> {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => provider.tiles_crs(),
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => provider.tiles_crs(),
        }
    }

    fn ordered_axes(&self) -> OgcApiResult<Vec<String>> {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => provider.ordered_axes(),
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => provider.ordered_axes(),
        }
    }

    fn validate_tile_coordinates(
        &self,
        tile_matrix: u8,
        tile_row: u32,
        tile_col: u32,
    ) -> OgcApiResult<()> {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => {
                provider.validate_tile_coordinates(tile_matrix, tile_row, tile_col)
            }
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => {
                provider.validate_tile_coordinates(tile_matrix, tile_row, tile_col)
            }
        }
    }

    fn tile_grid_bbox(
        &self,
        tile_matrix: u8,
        tile_row: u32,
        tile_col: u32,
    ) -> OgcApiResult<GridBoundingBox2D> {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => {
                provider.tile_grid_bbox(tile_matrix, tile_row, tile_col)
            }
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => {
                provider.tile_grid_bbox(tile_matrix, tile_row, tile_col)
            }
        }
    }
}

/// Custom tile matrix set implementation that computes TMS dynamically per layer
/// as a perfect fit to Geo Engine's internal tiling.
pub struct GeoEngineCustomTMS {
    result_descriptor: RasterResultDescriptor,
    tiling_specification: TilingSpecification,
    spatial_reference: SpatialReference,
}

impl GeoEngineCustomTMS {
    fn new(
        result_descriptor: RasterResultDescriptor,
        tiling_specification: TilingSpecification,
    ) -> OgcApiResult<Self> {
        let spatial_reference =
            result_descriptor
                .spatial_reference
                .as_option()
                .ok_or_else(|| OgcApiError::TileMatrixSetDefinitionNotAvailable {
                    tile_matrix_set_id: CUSTOM_TILE_MATRIX_SET_ID.to_string(),
                    reason: "Spatial reference of the layer is not available".to_string(),
                })?;

        Ok(GeoEngineCustomTMS {
            result_descriptor,
            tiling_specification,
            spatial_reference,
        })
    }
}

impl TileMatrixSetProvider for GeoEngineCustomTMS {
    fn id(&self) -> TileMatrixSetId {
        TileMatrixSetId::Custom(CUSTOM_TILE_MATRIX_SET_ID.to_string())
    }

    fn tile_matrix_set_definition(&self) -> OgcApiResult<TileMatrixSet> {
        Ok(TileMatrixSet {
            id: self.id(),
            title: CUSTOM_TILE_MATRIX_SET_TITLE.to_string().into(),
            description: None,
            keywords: Vec::new(),
            uri: None,
            crs: self.tiles_crs()?,
            ordered_axes: self.ordered_axes()?,
            well_known_scale_set: None,
            bounding_box: None,
            tile_matrices: self.tile_matrices()?,
        })
    }

    fn tile_matrices(&self) -> OgcApiResult<Vec<TileMatrix>> {
        build_tile_matrices(
            &self.result_descriptor,
            self.tiling_specification,
            self.spatial_reference,
        )
    }

    fn tiles_crs(&self) -> OgcApiResult<TilesCrs> {
        crs_from_spatial_reference_option(self.result_descriptor.spatial_reference)
            .map(TilesCrs::Simple)
    }

    fn ordered_axes(&self) -> OgcApiResult<Vec<String>> {
        ordered_axes(self.spatial_reference)
    }

    fn validate_tile_coordinates(
        &self,
        tile_matrix: u8,
        tile_row: u32,
        tile_col: u32,
    ) -> OgcApiResult<()> {
        let tile_matrices = build_tile_matrices(
            &self.result_descriptor,
            self.tiling_specification,
            self.spatial_reference,
        )?;
        if tile_matrix as usize >= tile_matrices.len() {
            return Err(OgcApiError::TileMatrixNotFound {
                tile_matrix: tile_matrix.to_string(),
            });
        }

        let tile_matrix_def = &tile_matrices[tile_matrices.len() - 1 - tile_matrix as usize];
        let max_row = tile_matrix_def.matrix_height.get() as u32;
        let max_col = tile_matrix_def.matrix_width.get() as u32;

        if tile_row >= max_row || tile_col >= max_col {
            return Err(OgcApiError::TileCoordinatesOutOfBounds {
                matrix: tile_matrix.to_string(),
                row: tile_row,
                col: tile_col,
            });
        }

        Ok(())
    }

    fn tile_grid_bbox(
        &self,
        tile_matrix: u8,
        tile_row: u32,
        tile_col: u32,
    ) -> OgcApiResult<GridBoundingBox2D> {
        let tiling_spatial_grid_definition = self
            .result_descriptor
            .tiling_grid_definition(self.tiling_specification);
        compute_tile_grid_bbox(
            tile_matrix,
            &tiling_spatial_grid_definition,
            tile_row,
            tile_col,
        )
    }
}

/// Web Mercator Quad tile matrix set (OGC standard, fixed to EPSG:3857)
pub struct WebMercatorQuadTMS;

impl TileMatrixSetProvider for WebMercatorQuadTMS {
    fn id(&self) -> TileMatrixSetId {
        TileMatrixSetId::WebMercatorQuad
    }

    fn tile_matrix_set_definition(&self) -> OgcApiResult<TileMatrixSet> {
        Ok(TileMatrixSet {
            id: self.id(),
            title: WEBMERCATOR_TILE_MATRIX_SET_TITLE.to_string().into(),
            description: None,
            keywords: Vec::new(),
            uri: Some(WEBMERCATOR_TILE_MATRIX_SET_URI.to_string()),
            crs: self.tiles_crs()?,
            ordered_axes: vec!["X".to_string(), "Y".to_string()],
            well_known_scale_set: "http://www.opengis.net/def/wkss/OGC/1.0/GoogleMapsCompatible"
                .to_string()
                .into(),
            bounding_box: None,
            tile_matrices: self.tile_matrices()?,
        })
    }

    /// OGC standard `WebMercatorQuad` tile matrices (25 levels, 0-24)
    /// From: <https://raw.githubusercontent.com/opengeospatial/2D-Tile-Matrix-Set/master/registry/json/WebMercatorQuad.json>
    fn tile_matrices(&self) -> OgcApiResult<Vec<TileMatrix>> {
        const WIDTH_AND_HEIGHT: NonZeroU16 =
            NonZeroU16::new(256).expect("256 is a valid non-zero u16");

        #[allow(clippy::unreadable_literal)]
        const DATA: &[(u8, f64, f64, u64, u64)] = &[
            (0, 559082264.028717, 156543.033928041, 1, 1),
            (1, 279541132.014358, 78271.5169640204, 2, 2),
            (2, 139770566.007179, 39135.7584820102, 4, 4),
            (3, 69885283.0035897, 19567.8792410051, 8, 8),
            (4, 34942641.5017948, 9783.93962050256, 16, 16),
            (5, 17471320.7508974, 4891.96981025128, 32, 32),
            (6, 8735660.37544871, 2445.98490512564, 64, 64),
            (7, 4367830.18772435, 1222.99245256282, 128, 128),
            (8, 2183915.09386217, 611.49622628141, 256, 256),
            (9, 1091957.54693108, 305.748113140704, 512, 512),
            (10, 545978.773465544, 152.874056570352, 1024, 1024),
            (11, 272989.386732772, 76.4370282851762, 2048, 2048),
            (12, 136494.693366386, 38.2185141425881, 4096, 4096),
            (13, 68247.346683193, 19.109257071294, 8192, 8192),
            (14, 34123.6733415964, 9.55462853564703, 16384, 16384),
            (15, 17061.8366707982, 4.77731426782351, 32768, 32768),
            (16, 8530.91833539913, 2.38865713391175, 65536, 65536),
            (17, 4265.45916769956, 1.19432856695587, 131072, 131072),
            (18, 2132.72958384978, 0.597164283477939, 262144, 262144),
            (19, 1066.36479192489, 0.29858214173897, 524288, 524288),
            (20, 533.182395962445, 0.149291070869485, 1048576, 1048576),
            (21, 266.591197981222, 0.0746455354347424, 2097152, 2097152),
            (22, 133.295598990611, 0.0373227677173712, 4194304, 4194304),
            (23, 66.6477994953056, 0.0186613838586856, 8388608, 8388608),
            (24, 33.3238997476528, 0.0093306919293428, 16777216, 16777216),
        ];
        let matrices = DATA
            .iter()
            .map(|&(id, scale, cell, mw, mh)| TileMatrix {
                id: id.to_string(),
                scale_denominator: scale,
                cell_size: cell,
                corner_of_origin: CornerOfOrigin::TopLeft,
                point_of_origin: [-20_037_508.342_789_2, 20_037_508.342_789_2],
                tile_width: WIDTH_AND_HEIGHT,
                tile_height: WIDTH_AND_HEIGHT,
                matrix_width: NonZeroU64::new(mw).expect("tile matrix dimension is non-zero"),
                matrix_height: NonZeroU64::new(mh).expect("tile matrix dimension is non-zero"),
                title: None,
                description: None,
                keywords: Vec::new(),
                variable_matrix_widths: Vec::new(),
            })
            .collect();
        Ok(matrices)
    }

    fn tiles_crs(&self) -> OgcApiResult<TilesCrs> {
        Ok(TilesCrs::Uri {
            uri: "http://www.opengis.net/def/crs/EPSG/0/3857".to_string(),
        })
    }

    fn ordered_axes(&self) -> OgcApiResult<Vec<String>> {
        Ok(vec!["X".to_string(), "Y".to_string()])
    }

    fn validate_tile_coordinates(
        &self,
        tile_matrix: u8,
        tile_row: u32,
        tile_col: u32,
    ) -> OgcApiResult<()> {
        const WEBMERCATOR_ZOOM_LEVELS: u8 = 25; // 0-24

        if tile_matrix >= WEBMERCATOR_ZOOM_LEVELS {
            return Err(OgcApiError::TileMatrixNotFound {
                tile_matrix: tile_matrix.to_string(),
            });
        }

        // Matrix width/height = 2^zoom_level
        let max_tile_index = (1u64 << tile_matrix) - 1;

        if u64::from(tile_row) > max_tile_index || u64::from(tile_col) > max_tile_index {
            return Err(OgcApiError::TileCoordinatesOutOfBounds {
                matrix: tile_matrix.to_string(),
                row: tile_row,
                col: tile_col,
            });
        }

        Ok(())
    }

    fn tile_grid_bbox(
        &self,
        _tile_matrix: u8,
        _tile_row: u32,
        _tile_col: u32,
    ) -> OgcApiResult<GridBoundingBox2D> {
        // WebMercator tile grid computation is different and will be handled separately
        // For now, return error as this requires Web Mercator specific grid calculations
        Err(OgcApiError::TileMatrixSetDefinitionNotAvailable {
            tile_matrix_set_id: WEBMERCATOR_TILE_MATRIX_SET_ID.to_string(),
            reason: "Web Mercator tile grid calculation requires projection support".to_string(),
        })
    }
}

/// Computes the pixel bounds of a tile in the global pixel grid for native custom TMS
fn compute_tile_grid_bbox(
    tile_matrix: u8,
    tiling_spatial_grid_definition: &TilingSpatialGridDefinition,
    tile_row: u32,
    tile_col: u32,
) -> OgcApiResult<GridBoundingBox2D> {
    let _grid_bounds = tiling_spatial_grid_definition.tiling_grid_bounds();
    let tiling_strategy = tiling_spatial_grid_definition.generate_data_tiling_strategy();

    let tile_index = GridIdx2D::new([tile_row as isize, tile_col as isize]);

    let min_pixel_index = tiling_strategy.tile_idx_to_global_pixel_idx(tile_index);
    let max_pixel_index = GridIdx2D::new([
        min_pixel_index.x() + tiling_strategy.tile_size_in_pixels.x() as isize - 1,
        min_pixel_index.y() + tiling_strategy.tile_size_in_pixels.y() as isize - 1,
    ]);

    GridBoundingBox2D::new(min_pixel_index, max_pixel_index).map_err(|_source| {
        OgcApiError::TileGridBboxComputationFailed {
            matrix: tile_matrix.to_string(),
            row: tile_row,
            col: tile_col,
        }
    })
}

fn ordered_axes(spatial_reference: SpatialReference) -> OgcApiResult<Vec<String>> {
    let specification = spatial_reference_specification(spatial_reference.into())?;

    Ok(match specification.axis_order {
        Some(AxisOrder::NorthEast) => vec!["Lon".into(), "Lat".into()],
        _ => vec!["X".into(), "Y".into()],
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
                    },
                    {
                        "id": "WebMercatorQuad",
                        "title": WEBMERCATOR_TILE_MATRIX_SET_TITLE,
                        "uri": WEBMERCATOR_TILE_MATRIX_SET_URI,
                        "links": [
                            {
                                "href": format!(
                                    "{server_url}/api/ogc/{data_connector_id}/{layer_id}/tileMatrixSets/WebMercatorQuad"
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

    #[test]
    fn it_returns_an_axis_order() {
        assert_eq!(
            ordered_axes(SpatialReference::epsg_4326()).unwrap(),
            vec!["Lon".to_string(), "Lat".to_string()]
        );
        assert_eq!(
            ordered_axes(SpatialReference::web_mercator()).unwrap(),
            vec!["X".to_string(), "Y".to_string()]
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

    #[test]
    fn it_calculates_scale_denominator_correctly() {
        // Test with standard Web Mercator (EPSG:3857)
        // Cell size = 10 meters, meters per unit = 1
        // Expected: 10 * 1 / 0.00028 ≈ 35,714.29
        let scale_denom = scale_denominator(10.0, 1.0);
        approx_eq!(f64, scale_denom, 35_714.285_714_285_7);

        // Test with zero cell size
        let scale_denom = scale_denominator(0.0, 1.0);
        approx_eq!(f64, scale_denom, 0.0);

        // Test with different meters per unit
        let scale_denom = scale_denominator(1.0, 1.0);
        approx_eq!(f64, scale_denom, 3_571.428_571_428_571);

        let scale_denom = scale_denominator(1.0, 10.0);
        approx_eq!(f64, scale_denom, 35_714.285_714_285_7);
    }

    #[test]
    fn it_converts_values_to_non_zero_u16() {
        // Normal cases
        assert_eq!(to_non_zero_u16(1), 1u16.try_into().unwrap());
        assert_eq!(to_non_zero_u16(512), 512u16.try_into().unwrap());
        assert_eq!(to_non_zero_u16(65535), 65535u16.try_into().unwrap());

        // Edge case: zero becomes 1
        assert_eq!(to_non_zero_u16(0), 1u16.try_into().unwrap());

        // Edge case: overflow wraps to u16::MAX
        let result = to_non_zero_u16(usize::MAX);
        assert!(result.get() > 0);

        // Common tile size
        assert_eq!(to_non_zero_u16(256), 256u16.try_into().unwrap());
    }

    #[test]
    fn it_converts_values_to_non_zero_u64() {
        // Normal cases
        assert_eq!(to_non_zero_u64(1), 1u64.try_into().unwrap());
        assert_eq!(to_non_zero_u64(1024), 1024u64.try_into().unwrap());

        // Edge case: zero becomes 1
        let result = to_non_zero_u64(0);
        assert_eq!(result.get(), 1);

        // Large value
        assert_eq!(to_non_zero_u64(1_000_000), 1_000_000u64.try_into().unwrap());

        // Common grid dimensions
        assert_eq!(to_non_zero_u64(2048), 2048u64.try_into().unwrap());
    }

    #[ge_context::test]
    async fn it_calculates_tiles_for_individual_zoom_levels_4326(app_ctx: PostgresContext<NoTls>) {
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

        let tiling_spec = ctx.execution_context().unwrap().tiling_specification();
        let max_zoom = calculate_number_of_zoom_levels(&descriptor, &tiling_spec) - 1;

        // Test individual zoom levels using calculate_tiles_for_zoom_level
        // Zoom level 0 (native resolution)
        let (tiles_z0, origin_z0) = calculate_tiles_for_zoom_level(&descriptor, &tiling_spec, 0);
        assert_eq!(tiles_z0, GridShape2D::new_2d(4, 8)); // From integration test
        assert!(origin_z0.x.is_finite());
        assert!(origin_z0.y.is_finite());

        // Zoom level 1 (2x downsampled)
        let (tiles_z1, origin_z1) = calculate_tiles_for_zoom_level(&descriptor, &tiling_spec, 1);
        assert_eq!(tiles_z1, GridShape2D::new_2d(2, 4));
        assert!(origin_z1.x.is_finite());
        assert!(origin_z1.y.is_finite());

        // Zoom level 2 (4x downsampled)
        let (tiles_z2, origin_z2) = calculate_tiles_for_zoom_level(&descriptor, &tiling_spec, 2);
        assert_eq!(tiles_z2, GridShape2D::new_2d(2, 2));
        assert!(origin_z2.x.is_finite());
        assert!(origin_z2.y.is_finite());

        // Verify consistency with calculate_tiles_for_zoom_levels
        let all_tiles = calculate_tiles_for_zoom_levels(&descriptor, &tiling_spec, max_zoom);
        assert_eq!(all_tiles[0].0, tiles_z0);
        assert_eq!(all_tiles[1].0, tiles_z1);
        assert_eq!(all_tiles[2].0, tiles_z2);
    }

    #[ge_context::test]
    async fn it_calculates_tiles_for_individual_zoom_levels_3857(app_ctx: PostgresContext<NoTls>) {
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

        let tiling_spec = ctx.execution_context().unwrap().tiling_specification();
        let max_zoom = calculate_number_of_zoom_levels(&descriptor, &tiling_spec) - 1;

        // Test individual zoom levels
        let (tiles_z0, _origin_z0) = calculate_tiles_for_zoom_level(&descriptor, &tiling_spec, 0);
        assert_eq!(tiles_z0, GridShape2D::new_2d(6, 6));

        let (tiles_z1, _origin_z1) = calculate_tiles_for_zoom_level(&descriptor, &tiling_spec, 1);
        assert_eq!(tiles_z1, GridShape2D::new_2d(4, 4));

        let (tiles_z2, _origin_z2) = calculate_tiles_for_zoom_level(&descriptor, &tiling_spec, 2);
        assert_eq!(tiles_z2, GridShape2D::new_2d(2, 2));

        let (tiles_z3, _origin_z3) = calculate_tiles_for_zoom_level(&descriptor, &tiling_spec, 3);
        assert_eq!(tiles_z3, GridShape2D::new_2d(2, 2));

        // Verify consistency with calculate_tiles_for_zoom_levels
        let all_tiles = calculate_tiles_for_zoom_levels(&descriptor, &tiling_spec, max_zoom);
        assert_eq!(all_tiles[0].0, tiles_z0);
        assert_eq!(all_tiles[1].0, tiles_z1);
        assert_eq!(all_tiles[2].0, tiles_z2);
        assert_eq!(all_tiles[3].0, tiles_z3);
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
