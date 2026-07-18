use crate::api::handlers::{
    ogc::{
        OgcApiResult,
        error::{self, OgcApiError},
        util::{
            OriginAndResolution, crs_from_spatial_reference_option, to_non_zero_u16,
            to_non_zero_u64,
        },
    },
    spatial_references::{AxisOrder, spatial_reference_specification},
};
use float_cmp::approx_eq;
use geoengine_datatypes::{
    operations::reproject::suggest_pixel_size_like_gdal_helper,
    primitives::{Coordinate2D, SpatialResolution},
    raster::{
        GridBoundingBox2D, GridBounds, GridIdx2D, GridShape2D, GridShapeAccess, GridSize,
        TilingSpatialGridDefinition, TilingSpecification,
    },
    spatial_reference::SpatialReference,
};
use geoengine_operators::engine::{RasterResultDescriptor, ResultDescriptor};
use ogcapi_types::tiles::{CornerOfOrigin, TileMatrix, TileMatrixSet, TileMatrixSetId, TilesCrs};
use snafu::OptionExt;
use std::{
    f64,
    num::{NonZeroU16, NonZeroU64},
};
use tracing::warn;

/// Cf. <https://docs.ogc.org/is/17-083r4/17-083r4.html#6-1-1-1-%C2%A0-tile-matrix-in-a-two-dimensional-space>
const STANDARD_PIXEL_SIZE_METERS: f64 = 0.28e-3;

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

    /// Computes the pixel bounds of a tile in the global pixel grid of the tiling scheme.
    ///
    /// The resolution is determined by `tile_matrix`, which corresponds to the zoom level in a TMS.
    /// The origin and orientation of the tile grid is determined by the `tiling_spatial_grid_definition`.
    fn tile_grid_bbox(
        &self,
        tiling_spatial_grid_definition: &TilingSpatialGridDefinition,
        tile_matrix: u8,
        tile_row: u32,
        tile_col: u32,
    ) -> OgcApiResult<GridBoundingBox2D>;

    /// Returns the maximum matrix ID for this tile matrix set
    fn max_matrix_id(&self) -> u32;

    /// Returns the spatial resolution for a given tile matrix ID
    fn spatial_resolution(&self, matrix_id: u8) -> SpatialResolution;

    /// Returns the grid shape and origin for a given tile matrix ID
    fn grid_shape_and_origin(&self, matrix_id: u8) -> (GridShape2D, Coordinate2D);

    /// Returns the tile size in pixels for a given tile matrix ID
    fn tile_size_in_pixels(&self, matrix_id: u8) -> GridShape2D;
}

/// A wrapper Tile Matrix Set Provider
pub enum TypedTileMatrixSetProvider {
    Custom(CustomNativeTMS),
    CustomWebMercator(CustomWebMercatorTMS),
    WebMercatorQuad(WebMercatorQuadTMS),
}

impl TypedTileMatrixSetProvider {
    /// Factory function to create the appropriate TMS provider for a given ID and layer
    pub fn resolve(
        tile_matrix_set_id: &TileMatrixSetId,
        result_descriptor: &RasterResultDescriptor,
        tiling_specification: TilingSpecification,
    ) -> OgcApiResult<Self> {
        match tile_matrix_set_id {
            TileMatrixSetId::Custom(id) if id == CustomNativeTMS::TILE_MATRIX_SET_ID => {
                let tms = CustomNativeTMS::new(result_descriptor.clone(), tiling_specification)?;
                Ok(TypedTileMatrixSetProvider::Custom(tms))
            }
            TileMatrixSetId::Custom(id) if id == CustomWebMercatorTMS::TILE_MATRIX_SET_ID => {
                let tms =
                    CustomWebMercatorTMS::new(result_descriptor.clone(), tiling_specification)?;
                Ok(TypedTileMatrixSetProvider::CustomWebMercator(tms))
            }
            TileMatrixSetId::WebMercatorQuad => Ok(TypedTileMatrixSetProvider::WebMercatorQuad(
                WebMercatorQuadTMS::new(result_descriptor.clone()),
            )),
            TileMatrixSetId::Custom(_) => Err(OgcApiError::TileMatrixSetNotFound {
                tile_matrix_set_id: tile_matrix_set_id.to_string(),
            }),
        }
    }

    /// Ensures that the tile matrix set exists. Returns an error if it does not exist.
    pub fn ensure_exists(tile_matrix_set_id: &TileMatrixSetId) -> OgcApiResult<()> {
        match tile_matrix_set_id {
            TileMatrixSetId::Custom(id) if id == CustomNativeTMS::TILE_MATRIX_SET_ID => Ok(()),
            TileMatrixSetId::Custom(id) if id == CustomWebMercatorTMS::TILE_MATRIX_SET_ID => Ok(()),
            TileMatrixSetId::WebMercatorQuad => Ok(()),
            TileMatrixSetId::Custom(_) => Err(OgcApiError::TileMatrixSetNotFound {
                tile_matrix_set_id: tile_matrix_set_id.to_string(),
            }),
        }
    }

    /// Returns the required spatial reference for a given tile matrix set ID, if applicable.
    pub fn required_srs(tile_matrix_set_id: &TileMatrixSetId) -> Option<SpatialReference> {
        match tile_matrix_set_id {
            TileMatrixSetId::WebMercatorQuad => Some(SpatialReference::web_mercator()),
            TileMatrixSetId::Custom(id) if id == CustomWebMercatorTMS::TILE_MATRIX_SET_ID => {
                Some(SpatialReference::web_mercator())
            }
            TileMatrixSetId::Custom(_) => None,
        }
    }

    /// Returns the required origin for a given tile matrix set ID, if applicable.
    pub fn required_origin_and_resolution(
        tile_matrix_set_id: &TileMatrixSetId,
        result_descriptor: &RasterResultDescriptor,
    ) -> OgcApiResult<Option<OriginAndResolution>> {
        match tile_matrix_set_id {
            TileMatrixSetId::WebMercatorQuad => {
                let spatial_reference = result_descriptor
                    .spatial_reference()
                    .as_option()
                    .context(error::MissingSpatialReference)?;
                let source_resolution = result_descriptor.spatial_grid.spatial_resolution();
                let web_mercator = SpatialReference::web_mercator();

                let resolution = if spatial_reference == web_mercator {
                    source_resolution
                } else {
                    suggest_pixel_size_like_gdal_helper(
                        result_descriptor.spatial_bounds(),
                        source_resolution,
                        spatial_reference,
                        web_mercator,
                    )
                    .map_err(crate::error::Error::from)?
                };

                Ok(Some(OriginAndResolution {
                    origin: Coordinate2D::new(
                        WebMercatorQuadTMS::ORIGIN[0],
                        WebMercatorQuadTMS::ORIGIN[1],
                    ),
                    resolution: WebMercatorQuadTMS::find_next_best_resolution(resolution),
                }))
            }
            // For custom tile matrix sets, we use the layer's native resolution and origin, so we don't need to require a specific one here.
            TileMatrixSetId::Custom(_) => Ok(None),
        }
    }
}

impl TileMatrixSetProvider for TypedTileMatrixSetProvider {
    fn id(&self) -> TileMatrixSetId {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => provider.id(),
            TypedTileMatrixSetProvider::CustomWebMercator(provider) => provider.id(),
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => provider.id(),
        }
    }

    fn tile_matrix_set_definition(&self) -> OgcApiResult<TileMatrixSet> {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => provider.tile_matrix_set_definition(),
            TypedTileMatrixSetProvider::CustomWebMercator(provider) => {
                provider.tile_matrix_set_definition()
            }
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => {
                provider.tile_matrix_set_definition()
            }
        }
    }

    fn tile_matrices(&self) -> OgcApiResult<Vec<TileMatrix>> {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => provider.tile_matrices(),
            TypedTileMatrixSetProvider::CustomWebMercator(provider) => provider.tile_matrices(),
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => provider.tile_matrices(),
        }
    }

    fn tiles_crs(&self) -> OgcApiResult<TilesCrs> {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => provider.tiles_crs(),
            TypedTileMatrixSetProvider::CustomWebMercator(provider) => provider.tiles_crs(),
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => provider.tiles_crs(),
        }
    }

    fn ordered_axes(&self) -> OgcApiResult<Vec<String>> {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => provider.ordered_axes(),
            TypedTileMatrixSetProvider::CustomWebMercator(provider) => provider.ordered_axes(),
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => provider.ordered_axes(),
        }
    }

    fn tile_grid_bbox(
        &self,
        tiling_spatial_grid_definition: &TilingSpatialGridDefinition,
        tile_matrix: u8,
        tile_row: u32,
        tile_col: u32,
    ) -> OgcApiResult<GridBoundingBox2D> {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => provider.tile_grid_bbox(
                tiling_spatial_grid_definition,
                tile_matrix,
                tile_row,
                tile_col,
            ),
            TypedTileMatrixSetProvider::CustomWebMercator(provider) => provider.tile_grid_bbox(
                tiling_spatial_grid_definition,
                tile_matrix,
                tile_row,
                tile_col,
            ),
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => provider.tile_grid_bbox(
                tiling_spatial_grid_definition,
                tile_matrix,
                tile_row,
                tile_col,
            ),
        }
    }

    fn max_matrix_id(&self) -> u32 {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => provider.max_matrix_id(),
            TypedTileMatrixSetProvider::CustomWebMercator(provider) => provider.max_matrix_id(),
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => provider.max_matrix_id(),
        }
    }

    fn spatial_resolution(&self, matrix_id: u8) -> SpatialResolution {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => provider.spatial_resolution(matrix_id),
            TypedTileMatrixSetProvider::CustomWebMercator(provider) => {
                provider.spatial_resolution(matrix_id)
            }
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => {
                provider.spatial_resolution(matrix_id)
            }
        }
    }

    fn grid_shape_and_origin(&self, matrix_id: u8) -> (GridShape2D, Coordinate2D) {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => {
                provider.grid_shape_and_origin(matrix_id)
            }
            TypedTileMatrixSetProvider::CustomWebMercator(provider) => {
                provider.grid_shape_and_origin(matrix_id)
            }
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => {
                provider.grid_shape_and_origin(matrix_id)
            }
        }
    }

    fn tile_size_in_pixels(&self, matrix_id: u8) -> GridShape2D {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => provider.tile_size_in_pixels(matrix_id),
            TypedTileMatrixSetProvider::CustomWebMercator(provider) => {
                provider.tile_size_in_pixels(matrix_id)
            }
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => {
                provider.tile_size_in_pixels(matrix_id)
            }
        }
    }
}

/// Custom tile matrix set implementation that computes TMS dynamically per layer
/// as a perfect fit to Geo Engine's internal tiling.
pub struct CustomNativeTMS {
    result_descriptor: RasterResultDescriptor,
    tiling_specification: TilingSpecification,
    spatial_reference: SpatialReference,
}

impl CustomNativeTMS {
    pub const TILE_MATRIX_SET_ID: &str = "Custom";
    pub const TILE_MATRIX_SET_TITLE: &str = "Custom Grid for Geo Engine";

    fn new(
        result_descriptor: RasterResultDescriptor,
        tiling_specification: TilingSpecification,
    ) -> OgcApiResult<Self> {
        let spatial_reference =
            result_descriptor
                .spatial_reference
                .as_option()
                .ok_or_else(|| OgcApiError::TileMatrixSetDefinitionNotAvailable {
                    tile_matrix_set_id: Self::TILE_MATRIX_SET_ID.to_string(),
                    reason: "Spatial reference of the layer is not available".to_string(),
                })?;

        Ok(CustomNativeTMS {
            result_descriptor,
            tiling_specification,
            spatial_reference,
        })
    }
}

impl TileMatrixSetProvider for CustomNativeTMS {
    fn id(&self) -> TileMatrixSetId {
        TileMatrixSetId::Custom(Self::TILE_MATRIX_SET_ID.to_string())
    }

    fn tile_matrix_set_definition(&self) -> OgcApiResult<TileMatrixSet> {
        Ok(TileMatrixSet {
            id: self.id(),
            title: Self::TILE_MATRIX_SET_TITLE.to_string().into(),
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
        crs_from_spatial_reference_option(self.result_descriptor.spatial_reference())
            .map(TilesCrs::Simple)
    }

    fn ordered_axes(&self) -> OgcApiResult<Vec<String>> {
        ordered_axes(self.spatial_reference)
    }

    fn tile_grid_bbox(
        &self,
        tiling_spatial_grid_definition: &TilingSpatialGridDefinition,
        tile_matrix: u8,
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
            - GridIdx2D::new([1, 1]); // inclusive bounds, e.g. we expect max-min to be 511 and not 512 for a 512 pixel tile

        GridBoundingBox2D::new(min_pixel_index, max_pixel_index).map_err(|_source| {
            OgcApiError::InvalidTileCoordinates {
                matrix: tile_matrix.to_string(),
                row: tile_row,
                col: tile_col,
            }
        })
    }

    fn max_matrix_id(&self) -> u32 {
        let number_of_zoom_levels =
            calculate_number_of_zoom_levels(&self.result_descriptor, &self.tiling_specification);
        number_of_zoom_levels.saturating_sub(1)
    }

    fn spatial_resolution(&self, matrix_id: u8) -> SpatialResolution {
        let zoom_level = self.max_matrix_id().saturating_sub(u32::from(matrix_id));
        let multiple_of_resolution = 2u32.pow(zoom_level);

        self.result_descriptor.spatial_grid.spatial_resolution() * f64::from(multiple_of_resolution)
    }

    fn grid_shape_and_origin(&self, matrix_id: u8) -> (GridShape2D, Coordinate2D) {
        let zoom_level = self.max_matrix_id().saturating_sub(u32::from(matrix_id));

        calculate_tiles_for_zoom_level(
            &self.result_descriptor,
            &self.tiling_specification,
            zoom_level,
        )
    }

    fn tile_size_in_pixels(&self, _matrix_id: u8) -> GridShape2D {
        self.tiling_specification.grid_shape()
    }
}

/// Custom tile matrix set implementation that computes TMS dynamically per layer
/// as a perfect fit to Geo Engine's internal tiling.
///
/// Requires that the layer is in EPSG:3857 (Web Mercator).
pub struct CustomWebMercatorTMS(CustomNativeTMS);

impl CustomWebMercatorTMS {
    pub const TILE_MATRIX_SET_ID: &str = "CustomWebMercator";
    pub const TILE_MATRIX_SET_TITLE: &str = "Custom Grid for Geo Engine (Web Mercator)";

    fn new(
        result_descriptor: RasterResultDescriptor,
        tiling_specification: TilingSpecification,
    ) -> OgcApiResult<Self> {
        Ok(CustomWebMercatorTMS(CustomNativeTMS::new(
            result_descriptor,
            tiling_specification,
        )?))
    }
}

impl TileMatrixSetProvider for CustomWebMercatorTMS {
    fn id(&self) -> TileMatrixSetId {
        TileMatrixSetId::Custom(Self::TILE_MATRIX_SET_ID.into())
    }

    fn tile_matrix_set_definition(&self) -> OgcApiResult<TileMatrixSet> {
        self.0.tile_matrix_set_definition()
    }

    fn tile_matrices(&self) -> OgcApiResult<Vec<TileMatrix>> {
        self.0.tile_matrices()
    }

    fn tiles_crs(&self) -> OgcApiResult<TilesCrs> {
        self.0.tiles_crs()
    }

    fn ordered_axes(&self) -> OgcApiResult<Vec<String>> {
        self.0.ordered_axes()
    }

    fn tile_grid_bbox(
        &self,
        tiling_spatial_grid_definition: &TilingSpatialGridDefinition,
        tile_matrix: u8,
        tile_row: u32,
        tile_col: u32,
    ) -> OgcApiResult<GridBoundingBox2D> {
        self.0.tile_grid_bbox(
            tiling_spatial_grid_definition,
            tile_matrix,
            tile_row,
            tile_col,
        )
    }

    fn max_matrix_id(&self) -> u32 {
        self.0.max_matrix_id()
    }

    fn spatial_resolution(&self, matrix_id: u8) -> SpatialResolution {
        self.0.spatial_resolution(matrix_id)
    }

    fn grid_shape_and_origin(&self, matrix_id: u8) -> (GridShape2D, Coordinate2D) {
        self.0.grid_shape_and_origin(matrix_id)
    }

    fn tile_size_in_pixels(&self, matrix_id: u8) -> GridShape2D {
        self.0.tile_size_in_pixels(matrix_id)
    }
}

/// Web Mercator Quad tile matrix set (OGC standard, fixed to EPSG:3857)
pub struct WebMercatorQuadTMS {
    result_descriptor: RasterResultDescriptor,
}

impl WebMercatorQuadTMS {
    pub const TILE_MATRIX_SET_ID: &str = "WebMercatorQuad";
    pub const TILE_MATRIX_SET_TITLE: &str = "Google Maps Compatible for the World";
    pub const TILE_MATRIX_SET_URI: &str =
        "http://www.opengis.net/def/tilematrixset/OGC/1.0/WebMercatorQuad";

    /// From <https://raw.githubusercontent.com/opengeospatial/2D-Tile-Matrix-Set/master/registry/json/WebMercatorQuad.json>
    ///
    /// `id`, `scaleDenominator`, `cellSize`, `matrixWidth`, `matrixHeight`
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
    /// From <https://raw.githubusercontent.com/opengeospatial/2D-Tile-Matrix-Set/master/registry/json/WebMercatorQuad.json>
    ///
    /// The origin of the Web Mercator Quad tile matrix set is at the top-left corner of the world in EPSG:3857 coordinates.
    #[allow(clippy::unreadable_literal)]
    const ORIGIN: [f64; 2] = [-20037508.3427892, 20037508.3427892];
    /// From <https://raw.githubusercontent.com/opengeospatial/2D-Tile-Matrix-Set/master/registry/json/WebMercatorQuad.json>
    ///
    /// The tile width and height for the Web Mercator Quad tile matrix set is fixed at 256 pixels.
    const WIDTH_AND_HEIGHT: NonZeroU16 = NonZeroU16::new(256).expect("256 is a valid non-zero u16");

    fn new(result_descriptor: RasterResultDescriptor) -> Self {
        WebMercatorQuadTMS { result_descriptor }
    }

    /// Finds the first resolution in [`DATA`] that is equal to or smaller than the given `native_resolution`.
    fn find_next_best_resolution(native_resolution: SpatialResolution) -> SpatialResolution {
        for &(_, _, cell_size, _, _) in Self::DATA {
            let resolution = cell_size; // cell size equals resolution in meters per pixel for Web Mercator Quad
            if native_resolution.x <= resolution && native_resolution.y <= resolution {
                return SpatialResolution::new_unchecked(resolution, resolution);
            }
        }
        // If no suitable resolution is found, return the smallest available resolution
        let &(_, _, cell_size, _, _) = Self::DATA.last().expect("DATA should not be empty");
        SpatialResolution::new_unchecked(cell_size, cell_size)
    }
}

impl TileMatrixSetProvider for WebMercatorQuadTMS {
    fn id(&self) -> TileMatrixSetId {
        TileMatrixSetId::WebMercatorQuad
    }

    fn tile_matrix_set_definition(&self) -> OgcApiResult<TileMatrixSet> {
        Ok(TileMatrixSet {
            id: self.id(),
            title: Self::TILE_MATRIX_SET_TITLE.to_string().into(),
            description: None,
            keywords: Vec::new(),
            uri: Some(Self::TILE_MATRIX_SET_URI.to_string()),
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
        let matrices = Self::DATA
            .iter()
            .map(|&(id, scale, cell, mw, mh)| TileMatrix {
                id: id.to_string(),
                scale_denominator: scale,
                cell_size: cell,
                corner_of_origin: CornerOfOrigin::TopLeft,
                point_of_origin: Self::ORIGIN,
                tile_width: Self::WIDTH_AND_HEIGHT,
                tile_height: Self::WIDTH_AND_HEIGHT,
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

    fn tile_grid_bbox(
        &self,
        tiling_spatial_grid_definition: &TilingSpatialGridDefinition,
        tile_matrix: u8,
        tile_row: u32,
        tile_col: u32,
    ) -> OgcApiResult<GridBoundingBox2D> {
        // Clamp to max zoom level
        let matrix_id = tile_matrix.min(self.max_matrix_id() as u8);

        // Get total pixel bounds from tiling_spatial_grid_definition (at this zoom level)
        let grid_bounds = tiling_spatial_grid_definition.tiling_grid_bounds();
        let [total_pixels_y, total_pixels_x] = grid_bounds.axis_size();
        let [min_y, min_x] = grid_bounds.min_index().0;

        // Get number of tiles in y and x direction from DATA
        let (_, _, _, matrix_width, matrix_height) = Self::DATA[matrix_id as usize];

        // Validate tile coordinates are within bounds
        if u64::from(tile_col) >= matrix_width || u64::from(tile_row) >= matrix_height {
            return Err(OgcApiError::TileCoordinatesOutOfBounds {
                matrix: matrix_id.to_string(),
                row: tile_row,
                col: tile_col,
            });
        }

        // Calculate tile size in pixels from total pixels and tile grid dimensions
        let tile_size_x = (total_pixels_x as isize) / (matrix_width as isize);
        let tile_size_y = (total_pixels_y as isize) / (matrix_height as isize);

        // Calculate pixel bounds for this specific tile, relative to grid bounds min
        let min_pixel_x = min_x + (tile_col as isize) * tile_size_x;
        let min_pixel_y = min_y + (tile_row as isize) * tile_size_y;

        // For the last tile in each dimension, extend to the actual grid bounds
        // to account for rounding from integer division
        let max_pixel_x = if u64::from(tile_col + 1) >= matrix_width {
            grid_bounds.x_max()
        } else {
            min_pixel_x + tile_size_x - 1
        };
        let max_pixel_y = if u64::from(tile_row + 1) >= matrix_height {
            grid_bounds.y_max()
        } else {
            min_pixel_y + tile_size_y - 1
        };

        let min_index = GridIdx2D::new([min_pixel_y, min_pixel_x]);
        let max_index = GridIdx2D::new([max_pixel_y, max_pixel_x]);

        GridBoundingBox2D::new(min_index, max_index).map_err(|_source| {
            OgcApiError::TileGridBboxComputationFailed {
                matrix: matrix_id.to_string(),
                row: tile_row,
                col: tile_col,
            }
        })
    }

    fn max_matrix_id(&self) -> u32 {
        24 // OGC Web Mercator Quad has exactly 25 levels (0-24)
    }

    /// Returns the spatial resolution for a given tile matrix ID (zoom level) in meters per pixel.
    ///
    /// The resolution is calculated based on the cell size defined in the OGC Web Mercator Quad specification.
    ///
    /// In detail, the resolution is computed as follows:
    /// 1. Clamp the provided `matrix_id` to the maximum zoom level (24).
    /// 2. Retrieve the cell size for the given `matrix_id` from the predefined `DATA` array.
    /// 3. Create a `SpatialResolution` object using the cell size (which is already in meters).
    /// 4. Calculate the multiples of the base resolution of the layer to find the closest matching resolution.
    /// 5. Round down the resolution multiple to the nearest power of 2.
    /// 6. Return the final spatial resolution, ensuring it does not go below the base resolution of the layer.
    ///
    fn spatial_resolution(&self, mut matrix_id: u8) -> SpatialResolution {
        matrix_id = matrix_id.min(self.max_matrix_id() as u8); // Clamp to max zoom level

        let (_, _, cell_size, _, _) = Self::DATA[matrix_id as usize]; // cellSize in meters

        // cellSize is already in meters and > 0, so we can create a SpatialResolution directly
        let target_spatial_resolution = SpatialResolution::new_unchecked(cell_size, cell_size);

        // Find the closest zoom level (matching or slightly finer) to a multiple of result descriptor's resolution
        let base_resolution = self.result_descriptor.spatial_grid.spatial_resolution();
        let multiples_of_resolution = target_spatial_resolution.div_floor(base_resolution);

        if multiples_of_resolution.x <= 1.0 || multiples_of_resolution.y <= 1.0 {
            return base_resolution; // Don't go below the base resolution of the layer
        }

        let mut resolution_multiple =
            f64::min(multiples_of_resolution.x, multiples_of_resolution.y);
        resolution_multiple = 2_f64.powf(resolution_multiple.log2().floor()); // Round down to nearest power of 2

        base_resolution * resolution_multiple
    }

    fn grid_shape_and_origin(&self, matrix_id: u8) -> (GridShape2D, Coordinate2D) {
        let matrix_id = matrix_id.min(self.max_matrix_id() as u8); // Clamp to max zoom level
        let (_, _, _, matrix_width, matrix_height) = Self::DATA[matrix_id as usize];

        let grid_shape = GridShape2D::new_2d(matrix_height as usize, matrix_width as usize);
        let origin_coordinate = Coordinate2D::new(Self::ORIGIN[0], Self::ORIGIN[1]);

        (grid_shape, origin_coordinate)
    }

    fn tile_size_in_pixels(&self, _matrix_id: u8) -> GridShape2D {
        GridShape2D::new_2d(
            Self::WIDTH_AND_HEIGHT.get() as usize,
            Self::WIDTH_AND_HEIGHT.get() as usize,
        )
    }
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

fn ordered_axes(spatial_reference: SpatialReference) -> OgcApiResult<Vec<String>> {
    let specification = spatial_reference_specification(spatial_reference.into())?;

    Ok(match specification.axis_order {
        Some(AxisOrder::NorthEast) => vec!["Lon".into(), "Lat".into()],
        _ => vec!["X".into(), "Y".into()],
    })
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

/// The documentation (<https://docs.ogc.org/is/17-083r4/17-083r4.html#6-1-1-1-%C2%A0-tile-matrix-in-a-two-dimensional-space>) says:
/// `cellSize = scaleDenominator × 0.2810^{−3} / metersPerUnit(crs)`
/// Thus, we have to calculate:
/// `scaleDenominator = cellSize × metersPerUnit(crs) / 0.2810^{−3}`
fn scale_denominator(cell_size: f64, meters_per_unit: f64) -> f64 {
    cell_size * meters_per_unit / STANDARD_PIXEL_SIZE_METERS
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
fn calculate_tiles_for_zoom_levels(
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
        api::handlers::ogc::{
            test_util::{
                calculate_number_of_tiles, grid_bbox, merge_bounds, session_and_3857_layer_id,
                session_and_4326_layer_id, session_and_native_3857_layer_id,
            },
            util::{
                get_initialized_raster_operator, load_layer, raster_workflow_metadata,
                reproject_if_necessary,
            },
        },
        contexts::{ApplicationContext, PostgresContext, PostgresSessionContext, SessionContext},
        ge_context,
        layers::{layer::Layer, listing::LayerCollectionProvider},
    };
    use geoengine_operators::engine::ExecutionContext;
    use tokio_postgres::NoTls;

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

        let tiling_spatial_grid_definition =
            result_descriptor.tiling_grid_definition(tiling_specification);
        let tiling_strategy = tiling_spatial_grid_definition.generate_data_tiling_strategy();

        let tiling_iterator = tiling_strategy.tile_information_iterator_from_pixel_bounds(
            tiling_spatial_grid_definition.tiling_grid_bounds(),
        );

        let provider = TypedTileMatrixSetProvider::resolve(
            &TileMatrixSetId::Custom(CustomNativeTMS::TILE_MATRIX_SET_ID.to_string()),
            &result_descriptor,
            tiling_specification,
        )
        .unwrap();

        let (matrix_width, matrix_height) = (8, 4);

        let (mut tile_row, mut tile_col) = (0, 0);
        let mut num_tiles = 0;
        for tile_info in tiling_iterator {
            assert_eq!(
                provider
                    .tile_grid_bbox(&tiling_spatial_grid_definition, 0, tile_row, tile_col)
                    .unwrap(),
                tile_info.global_pixel_bounds(),
                "Tile row {tile_row}, tile col {tile_col}: Expected {}",
                tile_info.global_pixel_bounds()
            );

            tile_col += 1;
            if tile_col >= matrix_width {
                tile_col = 0;
                tile_row += 1;
            }

            num_tiles += 1;
        }
        assert_eq!(num_tiles, matrix_width * matrix_height);
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

        let tiling_spatial_grid_definition =
            result_descriptor.tiling_grid_definition(tiling_specification);
        let tiling_strategy = tiling_spatial_grid_definition.generate_data_tiling_strategy();

        let tiling_iterator = tiling_strategy.tile_information_iterator_from_pixel_bounds(
            tiling_spatial_grid_definition.tiling_grid_bounds(),
        );

        let provider = TypedTileMatrixSetProvider::resolve(
            &TileMatrixSetId::Custom(CustomNativeTMS::TILE_MATRIX_SET_ID.to_string()),
            &result_descriptor,
            tiling_specification,
        )
        .unwrap();

        let (matrix_width, matrix_height) = (6, 6);

        let (mut tile_row, mut tile_col) = (0, 0);
        let mut num_tiles = 0;
        for tile_info in tiling_iterator {
            assert_eq!(
                provider
                    .tile_grid_bbox(&tiling_spatial_grid_definition, 0, tile_row, tile_col)
                    .unwrap(),
                tile_info.global_pixel_bounds(),
                "Tile row {tile_row}, tile col {tile_col}: Expected {}",
                tile_info.global_pixel_bounds()
            );

            tile_col += 1;
            if tile_col >= matrix_width {
                tile_col = 0;
                tile_row += 1;
            }

            num_tiles += 1;
        }

        assert_eq!(num_tiles, matrix_width * matrix_height);
    }

    #[ge_context::test]
    async fn it_calculates_correct_pixel_bounds_for_4326_overview_tiles(
        app_ctx: PostgresContext<NoTls>,
    ) {
        let (session_id, data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;

        let ctx = app_ctx.session_context(app_ctx.session_by_id(session_id).await.unwrap());

        let layer = load_layer::<PostgresContext<NoTls>>(&ctx, data_connector_id, layer_id.clone())
            .await
            .unwrap();
        let tiling_specification = ctx.execution_context().unwrap().tiling_specification();
        let mut initialized_operator = get_initialized_raster_operator::<
            PostgresSessionContext<NoTls>,
        >(&layer, &ctx.execution_context().unwrap())
        .await
        .unwrap();
        let provider = TypedTileMatrixSetProvider::resolve(
            &TileMatrixSetId::Custom(CustomNativeTMS::TILE_MATRIX_SET_ID.to_string()),
            initialized_operator.result_descriptor(),
            tiling_specification,
        )
        .unwrap();
        let max_matrix_id = provider.max_matrix_id();
        let matrix_id = (max_matrix_id.saturating_sub(2)) as u8; // zoom_level 2
        let new_resolution = provider.spatial_resolution(matrix_id);
        initialized_operator = initialized_operator
            .optimize_and_reinitialize(new_resolution, &ctx.execution_context().unwrap())
            .await
            .unwrap();
        let result_descriptor = initialized_operator.result_descriptor();

        let tiling_spatial_grid_definition =
            result_descriptor.tiling_grid_definition(tiling_specification);
        let tiling_strategy = tiling_spatial_grid_definition.generate_data_tiling_strategy();

        let tiling_iterator = tiling_strategy.tile_information_iterator_from_pixel_bounds(
            tiling_spatial_grid_definition.tiling_grid_bounds(),
        );

        let provider = TypedTileMatrixSetProvider::resolve(
            &TileMatrixSetId::Custom(CustomNativeTMS::TILE_MATRIX_SET_ID.to_string()),
            result_descriptor,
            tiling_specification,
        )
        .unwrap();

        let (matrix_width, _matrix_height) = (2, 1);
        // the y-area overlaps two tiles, so the output will merge the two tiles in the y-direction, resulting in 2x1 tiles
        let (actual_width, actual_height) = (2, 2);
        let actual_matrix_size = calculate_number_of_tiles(&tiling_spatial_grid_definition);

        assert_eq!(
            actual_matrix_size,
            GridShape2D::new_2d(actual_height, actual_width)
        );

        let (mut tile_row, mut tile_col) = (0, 0);
        let mut num_tiles = 0;
        for tile_info in tiling_iterator {
            // this still holds, the y-bounds will be corrected afterwards
            assert_eq!(
                provider
                    .tile_grid_bbox(&tiling_spatial_grid_definition, 0, tile_row, tile_col)
                    .unwrap(),
                tile_info.global_pixel_bounds(),
                "Tile row {tile_row}, tile col {tile_col}"
            );

            tile_col += 1;
            if tile_col >= matrix_width {
                tile_col = 0;
                tile_row += 1;
            }

            num_tiles += 1;
        }

        assert_eq!(num_tiles, actual_width * actual_height);
    }

    #[ge_context::test]
    async fn it_calculates_webmercator_tile_bounds(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) =
            session_and_native_3857_layer_id(&app_ctx).await;

        let ctx = app_ctx.session_context(app_ctx.session_by_id(session_id).await.unwrap());

        let layer = load_layer::<PostgresContext<NoTls>>(&ctx, data_connector_id, layer_id.clone())
            .await
            .unwrap();
        let execution_context = ctx.execution_context().unwrap();
        let initialized_operator =
            get_initialized_raster_operator::<PostgresSessionContext<NoTls>>(
                &layer,
                &execution_context,
            )
            .await
            .unwrap();

        let tms_spec = TypedTileMatrixSetProvider::resolve(
            &TileMatrixSetId::WebMercatorQuad,
            initialized_operator.result_descriptor(),
            execution_context.tiling_specification(),
        )
        .unwrap();

        // Test case: 0/0/0 - full world at zoom 0
        let tile_matrix = 0;
        let tiling_grid_definition = initialized_operator
            .optimize_and_reinitialize(
                tms_spec.spatial_resolution(tile_matrix), // level 0/0
                &execution_context,
            )
            .await
            .unwrap()
            .result_descriptor()
            .tiling_grid_definition(execution_context.tiling_specification());
        let bounds_0_0_0 = tms_spec
            .tile_grid_bbox(&tiling_grid_definition, tile_matrix, 0, 0)
            .unwrap();
        assert_eq!(
            bounds_0_0_0,
            grid_bbox([-178, -178], [178, 177]),
            "Bounds mismatch for tile 0/0/0"
        );
        assert_eq!(
            bounds_0_0_0,
            tiling_grid_definition.tiling_grid_bounds(),
            "Total pixel bounds mismatch for tile 0/0/0"
        );

        // Test case: zoom level 1 - all four tiles (2x2 matrix)
        let tile_matrix = 1;
        let tiling_grid_definition = initialized_operator
            .optimize_and_reinitialize(
                tms_spec.spatial_resolution(tile_matrix), // level 0/0
                &execution_context,
            )
            .await
            .unwrap()
            .result_descriptor()
            .tiling_grid_definition(execution_context.tiling_specification());

        // Check each tile individually - ensure all four tiles exist and have valid bounds
        let bounds_1_0_0 = tms_spec
            .tile_grid_bbox(&tiling_grid_definition, tile_matrix, 0, 0)
            .unwrap();
        assert_eq!(
            bounds_1_0_0,
            grid_bbox([-355, -356], [0, -2]),
            "Bounds mismatch for tile 1/0/0"
        );

        let bounds_1_1_0 = tms_spec
            .tile_grid_bbox(&tiling_grid_definition, tile_matrix, 0, 1)
            .unwrap();
        assert_eq!(
            bounds_1_1_0,
            grid_bbox([-355, -1], [0, 354]),
            "Bounds mismatch for tile 1/1/0"
        );

        let bounds_1_0_1 = tms_spec
            .tile_grid_bbox(&tiling_grid_definition, tile_matrix, 1, 0)
            .unwrap();
        assert_eq!(
            bounds_1_0_1,
            grid_bbox([1, -356], [357, -2]),
            "Bounds mismatch for tile 1/0/1"
        );

        let bounds_1_1_1 = tms_spec
            .tile_grid_bbox(&tiling_grid_definition, tile_matrix, 1, 1)
            .unwrap();
        assert_eq!(
            bounds_1_1_1,
            grid_bbox([1, -1], [357, 354]),
            "Bounds mismatch for tile 1/1/1"
        );

        // Verify that the four tiles together should cover the total bounds
        // by checking that at least one tile touches each edge of the total bounds
        assert_eq!(
            merge_bounds([bounds_1_0_0, bounds_1_0_1, bounds_1_1_0, bounds_1_1_1]),
            tiling_grid_definition.tiling_grid_bounds(),
            "The merged bounds of all four tiles at zoom level 1 should equal the total bounds"
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
            // Initialize operator and resample to the target resolution
            let mut initialized_operator = get_initialized_raster_operator::<
                PostgresSessionContext<NoTls>,
            >(layer, &execution_context)
            .await
            .expect("Failed to initialize operator");

            let provider = TypedTileMatrixSetProvider::resolve(
                &TileMatrixSetId::Custom(CustomNativeTMS::TILE_MATRIX_SET_ID.to_string()),
                initialized_operator.result_descriptor(),
                tiling_spec,
            )
            .unwrap();
            let max_matrix_id = provider.max_matrix_id();
            let matrix_id = (max_matrix_id.saturating_sub(*zoom_level)) as u8;
            let new_resolution = provider.spatial_resolution(matrix_id);
            initialized_operator = initialized_operator
                .optimize_and_reinitialize(new_resolution, &execution_context)
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
                "Zoom level {zoom_level}: expected tiles {expected_tiles:?}, but operator returned {actual_from_operator:?}"
            );
        }
    }

    /// Test error cases: invalid TMS IDs, invalid zoom levels, and bounds checking
    #[ge_context::test]
    async fn it_handles_error_cases_for_tile_matrix_set_operations(
        app_ctx: PostgresContext<NoTls>,
    ) {
        let (session_id, _data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;
        let session = app_ctx.session_by_id(session_id).await.unwrap();
        let ctx = app_ctx.session_context(session);
        let layer = ctx.db().load_layer(&layer_id.clone().into()).await.unwrap();
        let descriptor =
            raster_workflow_metadata::<crate::contexts::PostgresSessionContext<NoTls>>(
                layer.workflow.clone(),
                ctx.execution_context().unwrap(),
            )
            .await
            .unwrap();

        let tiling_spec = ctx.execution_context().unwrap().tiling_specification();

        // Error case 1: Invalid TMS ID
        let result_invalid_tms = TypedTileMatrixSetProvider::ensure_exists(
            &TileMatrixSetId::Custom("NonExistent".to_string()),
        );
        assert!(
            result_invalid_tms.is_err(),
            "Should reject non-existent tile matrix set ID"
        );

        // Error case 2: Invalid zoom level (out of bounds)
        // Try to access zoom level beyond max for WebMercatorQuad
        let provider = TypedTileMatrixSetProvider::resolve(
            &TileMatrixSetId::WebMercatorQuad,
            &descriptor,
            tiling_spec,
        )
        .unwrap();

        let max_level = provider.max_matrix_id();
        let invalid_level = max_level + 1;

        // Accessing resolution for invalid zoom should fail or return invalid value
        // (depends on implementation, but shouldn't panic)
        let _ = provider.spatial_resolution(invalid_level as u8);

        // Error case 3: Out-of-bounds tile coordinates
        let tiling_spatial_grid_definition = descriptor.tiling_grid_definition(tiling_spec);

        // Try to access tile beyond grid bounds (row too high)
        let result_oob_row = provider.tile_grid_bbox(&tiling_spatial_grid_definition, 0, 1000, 0);
        assert!(
            result_oob_row.is_err(),
            "Should reject out-of-bounds tile row"
        );

        // Try to access tile beyond grid bounds (col too high)
        let result_oob_col = provider.tile_grid_bbox(&tiling_spatial_grid_definition, 0, 0, 1000);
        assert!(
            result_oob_col.is_err(),
            "Should reject out-of-bounds tile col"
        );
    }

    /// Test `GeoEngineCustomWebMercatorTMS` variant properties via `TypedTileMatrixSetProvider`
    #[ge_context::test]
    async fn it_validates_custom_web_mercator_tms_properties(app_ctx: PostgresContext<NoTls>) {
        let (session_id, _data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;
        let session = app_ctx.session_by_id(session_id).await.unwrap();
        let ctx = app_ctx.session_context(session);
        let mut layer = ctx.db().load_layer(&layer_id.clone().into()).await.unwrap();
        let execution_context = ctx.execution_context().unwrap();

        let tiling_spec = execution_context.tiling_specification();
        // Verify CustomWebMercator is recognized and requires Web Mercator SRS
        let tms_id = TileMatrixSetId::Custom("CustomWebMercator".to_string());

        assert!(
            TypedTileMatrixSetProvider::ensure_exists(&tms_id).is_ok(),
            "CustomWebMercator should be recognized as valid TMS"
        );

        let required_srs = TypedTileMatrixSetProvider::required_srs(&tms_id);
        assert_eq!(
            required_srs,
            Some(SpatialReference::web_mercator()),
            "Required SRS should be Web Mercator (EPSG:3857)"
        );

        let mut initialized_operator = get_initialized_raster_operator::<
            PostgresSessionContext<NoTls>,
        >(&layer, &execution_context)
        .await
        .expect("Failed to initialize operator");

        let required_origin_and_resolution =
            TypedTileMatrixSetProvider::required_origin_and_resolution(
                &tms_id,
                initialized_operator.result_descriptor(),
            )
            .unwrap();
        assert!(
            required_origin_and_resolution.is_none(),
            "CustomWebMercator should not require specific origin and resolution"
        );

        reproject_if_necessary::<PostgresSessionContext<NoTls>>(
            &mut layer,
            &mut initialized_operator,
            &execution_context,
            required_srs,
            required_origin_and_resolution,
        )
        .await
        .unwrap();

        let tms_spec = TypedTileMatrixSetProvider::resolve(
            &tms_id,
            initialized_operator.result_descriptor(),
            tiling_spec,
        )
        .expect("Should resolve CustomWebMercator TMS");

        // Verify TMS level 0
        let tile_matrices = tms_spec.tile_matrices().unwrap();
        let level_0 = tile_matrices.first().unwrap();
        assert_eq!(
            level_0,
            &TileMatrix {
                id: 0.to_string(),
                title: None,
                description: None,
                keywords: vec![],
                scale_denominator: 407_286_157.394_767_17,
                cell_size: 114_040.124_070_534_79,
                corner_of_origin: CornerOfOrigin::TopLeft,
                point_of_origin: [-58_354_990.030_488_94, 58_418_366.631_411_66],
                tile_width: to_non_zero_u16(512),
                tile_height: to_non_zero_u16(512),
                matrix_width: to_non_zero_u64(2),
                matrix_height: to_non_zero_u64(2),
                variable_matrix_widths: vec![],
            },
            "Level 0 properties should match expected values"
        );

        let initialized_operator =
            get_initialized_raster_operator::<PostgresSessionContext<NoTls>>(
                &layer,
                &execution_context,
            )
            .await
            .expect("Failed to initialize operator");
        let tiling_grid_definition = initialized_operator
            .optimize_and_reinitialize(
                tms_spec.spatial_resolution(0), // level 0/0
                &execution_context,
            )
            .await
            .unwrap()
            .result_descriptor()
            .tiling_grid_definition(tiling_spec);
        let bbox_0_0_0 = tms_spec
            .tile_grid_bbox(&tiling_grid_definition, 0, 0, 0)
            .unwrap();

        assert_eq!(
            bbox_0_0_0,
            grid_bbox([-512, -512], [-1, -1]),
            "Tile 0/0/0 bounds should match expected values"
        );
    }
}
