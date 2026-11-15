use crate::api::handlers::{
    ogc::{
        OgcApiResult,
        error::{self, OgcApiError},
        util::{crs_from_spatial_reference_option, to_non_zero_u16, to_non_zero_u64},
    },
    spatial_references::{AxisOrder, SpatialReferenceCache},
};
use float_cmp::approx_eq;
use geoengine_datatypes::{
    primitives::{Coordinate2D, SpatialResolution},
    raster::{
        GridBoundingBox2D, GridBounds, GridIdx2D, GridShape2D, GridShapeAccess, GridSize,
        TilingSpatialGridDefinition, TilingSpecification,
    },
    spatial_reference::{CoordinateProjection, DefaultCoordinateProjector, SpatialReference},
    util::PositiveFiniteF64,
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
    fn tile_matrix_set_definition(
        &self,
        spatial_reference_cache: &SpatialReferenceCache,
    ) -> OgcApiResult<TileMatrixSet>;

    /// Returns the tile matrix definitions for all zoom levels
    fn tile_matrices(&self) -> OgcApiResult<Vec<TileMatrix>>;

    /// Returns the CRS for this tile matrix set
    fn tiles_crs(&self) -> OgcApiResult<TilesCrs>;

    /// Returns the ordered axes labels (e.g., `["Lon", "Lat"]` or `["x", "y"]`)
    fn ordered_axes(
        &self,
        spatial_reference_cache: &SpatialReferenceCache,
    ) -> OgcApiResult<Vec<String>>;

    /// The pixels of a tile in `tiling_spatial_grid_definition`, a grid at the resolution of
    /// `tile_matrix` (see [`Self::spatial_resolution`]) that is aligned to its origin.
    fn tile_grid_bbox(
        &self,
        tiling_spatial_grid_definition: &TilingSpatialGridDefinition,
        tile_matrix: u8,
        tile_row: u32,
        tile_col: u32,
    ) -> OgcApiResult<GridBoundingBox2D> {
        let (tiles, origin) = self.grid_shape_and_origin(tile_matrix);
        if tile_row as usize >= tiles.y() || tile_col as usize >= tiles.x() {
            return Err(OgcApiError::TileCoordinatesOutOfBounds {
                matrix: tile_matrix.to_string(),
                row: tile_row,
                col: tile_col,
            });
        }
        let tile_size = self.tile_size_in_pixels(tile_matrix);
        let geo_transform = tiling_spatial_grid_definition.tiling_geo_transform();
        // the pixel under the center of the tile matrix's first pixel, so that rounding of the origin does not matter
        let first_pixel = geo_transform.coordinate_to_grid_idx_2d(
            origin
                + Coordinate2D::new(
                    geo_transform.x_pixel_size().get() / 2.,
                    geo_transform.y_pixel_size().get() / 2.,
                ),
        );
        let (tile_height, tile_width) = (tile_size.y() as isize, tile_size.x() as isize);
        let min = first_pixel
            + GridIdx2D::new([
                tile_row as isize * tile_height,
                tile_col as isize * tile_width,
            ]);

        GridBoundingBox2D::new(min, min + GridIdx2D::new([tile_height - 1, tile_width - 1]))
            .map_err(|_source| OgcApiError::InvalidTileCoordinates {
                matrix: tile_matrix.to_string(),
                row: tile_row,
                col: tile_col,
            })
    }

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
                WebMercatorQuadTMS,
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

    /// The layer's result descriptor as `tile_matrix_set_id` sees it, computed without operators:
    /// for `CustomWebMercator` and a layer in another spatial reference, on the grid that a
    /// `Reprojection` to EPSG:3857 with `DataBounds` derives; otherwise the layer's.
    pub fn layer_descriptor(
        tile_matrix_set_id: &TileMatrixSetId,
        result_descriptor: &RasterResultDescriptor,
    ) -> OgcApiResult<RasterResultDescriptor> {
        let spatial_reference = result_descriptor
            .spatial_reference()
            .as_option()
            .context(error::MissingSpatialReference)?;
        // `WebMercatorQuad` has fixed tile matrices, only the custom ones derive them from the layer
        let custom = matches!(tile_matrix_set_id, TileMatrixSetId::Custom(_));
        match Self::required_srs(tile_matrix_set_id) {
            Some(web_mercator) if custom && spatial_reference != web_mercator => {
                let projector =
                    DefaultCoordinateProjector::from_known_srs(spatial_reference, web_mercator)
                        .map_err(crate::error::Error::from)?;
                let spatial_grid = result_descriptor
                    .spatial_grid_descriptor()
                    .reproject_clipped(&projector)
                    .map_err(crate::error::Error::from)?
                    .ok_or(geoengine_operators::error::Error::ReprojectionFailed)
                    .map_err(crate::error::Error::from)?;
                Ok(RasterResultDescriptor {
                    spatial_reference: web_mercator.into(),
                    spatial_grid,
                    ..result_descriptor.clone()
                })
            }
            _ => Ok(result_descriptor.clone()),
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

    fn tile_matrix_set_definition(
        &self,
        spatial_reference_cache: &SpatialReferenceCache,
    ) -> OgcApiResult<TileMatrixSet> {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => {
                provider.tile_matrix_set_definition(spatial_reference_cache)
            }
            TypedTileMatrixSetProvider::CustomWebMercator(provider) => {
                provider.tile_matrix_set_definition(spatial_reference_cache)
            }
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => {
                provider.tile_matrix_set_definition(spatial_reference_cache)
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

    fn ordered_axes(
        &self,
        spatial_reference_cache: &SpatialReferenceCache,
    ) -> OgcApiResult<Vec<String>> {
        match self {
            TypedTileMatrixSetProvider::Custom(provider) => {
                provider.ordered_axes(spatial_reference_cache)
            }
            TypedTileMatrixSetProvider::CustomWebMercator(provider) => {
                provider.ordered_axes(spatial_reference_cache)
            }
            TypedTileMatrixSetProvider::WebMercatorQuad(provider) => {
                provider.ordered_axes(spatial_reference_cache)
            }
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

    fn tile_matrix_set_definition(
        &self,
        spatial_reference_cache: &SpatialReferenceCache,
    ) -> OgcApiResult<TileMatrixSet> {
        Ok(TileMatrixSet {
            id: self.id(),
            title: Self::TILE_MATRIX_SET_TITLE.to_string().into(),
            description: None,
            keywords: Vec::new(),
            uri: None,
            crs: self.tiles_crs()?,
            ordered_axes: self.ordered_axes(spatial_reference_cache)?,
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

    fn ordered_axes(
        &self,
        spatial_reference_cache: &SpatialReferenceCache,
    ) -> OgcApiResult<Vec<String>> {
        ordered_axes(self.spatial_reference, spatial_reference_cache)
    }

    fn max_matrix_id(&self) -> u32 {
        let number_of_zoom_levels =
            calculate_number_of_zoom_levels(&self.result_descriptor, &self.tiling_specification);
        number_of_zoom_levels.saturating_sub(1)
    }

    fn spatial_resolution(&self, matrix_id: u8) -> SpatialResolution {
        let zoom_level = self.max_matrix_id().saturating_sub(u32::from(matrix_id));
        let multiple_of_resolution = 2u32.pow(zoom_level);

        // the zoom levels are bounded by the grid size, so the coarsest resolution still covers a finite extent
        self.result_descriptor.spatial_grid.spatial_resolution()
            * PositiveFiniteF64::new_unchecked(f64::from(multiple_of_resolution))
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

    fn tile_matrix_set_definition(
        &self,
        spatial_reference_cache: &SpatialReferenceCache,
    ) -> OgcApiResult<TileMatrixSet> {
        self.0.tile_matrix_set_definition(spatial_reference_cache)
    }

    fn tile_matrices(&self) -> OgcApiResult<Vec<TileMatrix>> {
        self.0.tile_matrices()
    }

    fn tiles_crs(&self) -> OgcApiResult<TilesCrs> {
        self.0.tiles_crs()
    }

    fn ordered_axes(
        &self,
        spatial_reference_cache: &SpatialReferenceCache,
    ) -> OgcApiResult<Vec<String>> {
        self.0.ordered_axes(spatial_reference_cache)
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
pub struct WebMercatorQuadTMS;

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
}

impl TileMatrixSetProvider for WebMercatorQuadTMS {
    fn id(&self) -> TileMatrixSetId {
        TileMatrixSetId::WebMercatorQuad
    }

    fn tile_matrix_set_definition(
        &self,
        _spatial_reference_cache: &SpatialReferenceCache,
    ) -> OgcApiResult<TileMatrixSet> {
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

    fn ordered_axes(
        &self,
        _spatial_reference_cache: &SpatialReferenceCache,
    ) -> OgcApiResult<Vec<String>> {
        Ok(vec!["X".to_string(), "Y".to_string()])
    }

    fn max_matrix_id(&self) -> u32 {
        24 // OGC Web Mercator Quad has exactly 25 levels (0-24)
    }

    /// The cell size of the tile matrix (zoom level) in meters per pixel
    fn spatial_resolution(&self, matrix_id: u8) -> SpatialResolution {
        let matrix_id = matrix_id.min(self.max_matrix_id() as u8); // Clamp to max zoom level
        let (_, _, cell_size, _, _) = Self::DATA[matrix_id as usize];
        SpatialResolution::new_unchecked(cell_size, cell_size)
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
        tiling_geo_transform.x_pixel_size().get(),
        tiling_geo_transform.y_pixel_size().get(),
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

fn ordered_axes(
    spatial_reference: SpatialReference,
    spatial_reference_cache: &SpatialReferenceCache,
) -> OgcApiResult<Vec<String>> {
    Ok(
        match spatial_reference_cache.axis_order(spatial_reference.into()) {
            Ok(AxisOrder::NorthEast) => vec!["Lon".into(), "Lat".into()],
            // the specification of a reference without axis order gave X, Y, not an error
            Ok(AxisOrder::EastNorth)
            | Err(crate::error::Error::AxisOrderingNotKnownForSrs { .. }) => {
                vec!["X".into(), "Y".into()]
            }
            Err(e) => return Err(e.into()),
        },
    )
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
        let transitions = (tiles_at_max_resolution - 1).bit_width();

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
    let downsampled_resolution =
        original_resolution * PositiveFiniteF64::new_unchecked(f64::from(zoom_factor));

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
        let downsampled_resolution =
            original_resolution * PositiveFiniteF64::new_unchecked(f64::from(zoom_factor));

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
                calculate_number_of_tiles, grid_bbox, session_and_3857_layer_id,
                session_and_4326_layer_id, session_and_native_3857_layer_id,
            },
            util::{get_initialized_raster_operator, load_layer, raster_processing_graph_metadata},
        },
        contexts::{ApplicationContext, PostgresContext, PostgresSessionContext, SessionContext},
        ge_context,
        layers::{layer::Layer, listing::LayerCollectionProvider},
    };
    use geoengine_operators::engine::{ExecutionContext, RasterOperator};
    use tokio_postgres::NoTls;

    #[test]
    fn it_returns_an_axis_order() {
        assert_eq!(
            ordered_axes(
                SpatialReference::epsg_4326(),
                &SpatialReferenceCache::default()
            )
            .unwrap(),
            vec!["Lon".to_string(), "Lat".to_string()]
        );
        assert_eq!(
            ordered_axes(
                SpatialReference::web_mercator(),
                &SpatialReferenceCache::default()
            )
            .unwrap(),
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
        let descriptor = raster_processing_graph_metadata::<
            crate::contexts::PostgresSessionContext<NoTls>,
        >(layer.processing_graph.clone(), execution_context)
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
        let descriptor = raster_processing_graph_metadata::<
            crate::contexts::PostgresSessionContext<NoTls>,
        >(layer.processing_graph.clone(), execution_context)
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
        let descriptor = raster_processing_graph_metadata::<
            crate::contexts::PostgresSessionContext<NoTls>,
        >(layer.processing_graph.clone(), execution_context)
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
        let descriptor = raster_processing_graph_metadata::<
            crate::contexts::PostgresSessionContext<NoTls>,
        >(layer.processing_graph.clone(), execution_context)
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
        let descriptor = raster_processing_graph_metadata::<
            crate::contexts::PostgresSessionContext<NoTls>,
        >(layer.processing_graph.clone(), execution_context)
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

        // the grid of the layer is the finest tile matrix
        let finest = provider.max_matrix_id() as u8;
        let (mut tile_row, mut tile_col) = (0, 0);
        let mut num_tiles = 0;
        for tile_info in tiling_iterator {
            assert_eq!(
                provider
                    .tile_grid_bbox(&tiling_spatial_grid_definition, finest, tile_row, tile_col)
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

        // the grid of the layer is the finest tile matrix
        let finest = provider.max_matrix_id() as u8;
        let (mut tile_row, mut tile_col) = (0, 0);
        let mut num_tiles = 0;
        for tile_info in tiling_iterator {
            assert_eq!(
                provider
                    .tile_grid_bbox(&tiling_spatial_grid_definition, finest, tile_row, tile_col)
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

    /// A tile is 256 × 256 pixels of the grid that `render_tile` warps the layer onto, and starts
    /// at the tile's corner in the `WebMercatorQuad` tile matrix
    #[ge_context::test]
    async fn it_calculates_webmercator_tile_bounds(app_ctx: PostgresContext<NoTls>) {
        let (session_id, data_connector_id, layer_id) =
            session_and_native_3857_layer_id(&app_ctx).await;

        let ctx = app_ctx.session_context(app_ctx.session_by_id(session_id).await.unwrap());
        let layer = load_layer::<PostgresContext<NoTls>>(&ctx, data_connector_id, layer_id.clone())
            .await
            .unwrap();
        let execution_context = ctx.execution_context().unwrap();
        let tiling_specification = execution_context.tiling_specification();
        let result_descriptor = get_initialized_raster_operator::<PostgresSessionContext<NoTls>>(
            &layer,
            &execution_context,
        )
        .await
        .unwrap()
        .result_descriptor()
        .clone();

        let tms_spec = TypedTileMatrixSetProvider::resolve(
            &TileMatrixSetId::WebMercatorQuad,
            &result_descriptor,
            tiling_specification,
        )
        .unwrap();

        for (tile_matrix, tile_row, tile_col) in [(0, 0, 0), (1, 0, 1), (1, 1, 0), (5, 10, 16)] {
            let (_, origin) = tms_spec.grid_shape_and_origin(tile_matrix);
            let resolution = tms_spec.spatial_resolution(tile_matrix);
            let grid = geoengine_operators::processing::warped_spatial_grid(
                &result_descriptor,
                &geoengine_operators::processing::WarpParams {
                    target_spatial_reference: None,
                    output_resolution: Some(resolution),
                    output_origin_reference: Some(origin),
                    method: geoengine_operators::processing::WarpMethod::NearestNeighbor,
                },
            )
            .unwrap()
            .tiling_grid_definition(tiling_specification);

            let bounds = tms_spec
                .tile_grid_bbox(&grid, tile_matrix, tile_row, tile_col)
                .unwrap();

            assert_eq!(
                bounds.axis_size(),
                [256, 256],
                "tile {tile_matrix}/{tile_row}/{tile_col}"
            );
            let upper_left = grid
                .tiling_geo_transform()
                .grid_idx_to_pixel_upper_left_coordinate_2d(bounds.min_index());
            let tile_size = 256. * resolution.x().get();
            assert!(
                approx_eq!(
                    f64,
                    upper_left.x,
                    origin.x + f64::from(tile_col) * tile_size,
                    epsilon = 1e-6
                ) && approx_eq!(
                    f64,
                    upper_left.y,
                    origin.y - f64::from(tile_row) * tile_size,
                    epsilon = 1e-6
                ),
                "tile {tile_matrix}/{tile_row}/{tile_col} starts at {upper_left:?}"
            );
        }
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
            raster_processing_graph_metadata::<crate::contexts::PostgresSessionContext<NoTls>>(
                layer.processing_graph.clone(),
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
    #[allow(clippy::too_many_lines)]
    async fn it_validates_custom_web_mercator_tms_properties(app_ctx: PostgresContext<NoTls>) {
        let (session_id, _data_connector_id, layer_id) = session_and_4326_layer_id(&app_ctx).await;
        let session = app_ctx.session_by_id(session_id).await.unwrap();
        let ctx = app_ctx.session_context(session);
        let layer = ctx.db().load_layer(&layer_id.clone().into()).await.unwrap();
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

        let initialized_operator =
            get_initialized_raster_operator::<PostgresSessionContext<NoTls>>(
                &layer,
                &execution_context,
            )
            .await
            .expect("Failed to initialize operator");

        let layer_descriptor = TypedTileMatrixSetProvider::layer_descriptor(
            &tms_id,
            initialized_operator.result_descriptor(),
        )
        .unwrap();

        // the grid of a `Reprojection` with `DataBounds`, as the tile path reprojected the layer before
        let reprojection = RasterOperator::typed(geoengine_operators::processing::Reprojection {
            params: geoengine_operators::processing::ReprojectionParams {
                target_spatial_reference: SpatialReference::web_mercator(),
                derive_out_spec:
                    geoengine_operators::processing::DeriveOutRasterSpecsSource::DataBounds,
            },
            sources: geoengine_operators::engine::SingleRasterOrVectorSource {
                source: geoengine_operators::util::input::RasterOrVectorOperator::Raster(
                    layer
                        .processing_graph
                        .operator
                        .clone()
                        .get_raster()
                        .unwrap(),
                ),
            },
        })
        .initialize(
            geoengine_operators::engine::ProcessingGraphOperatorPath::initialize_root(),
            &execution_context,
        )
        .await
        .unwrap();
        assert_eq!(
            layer_descriptor.spatial_grid,
            reprojection.result_descriptor().spatial_grid
        );
        assert_eq!(
            layer_descriptor.spatial_reference,
            SpatialReference::web_mercator().into()
        );

        let tms_spec = TypedTileMatrixSetProvider::resolve(&tms_id, &layer_descriptor, tiling_spec)
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

        // the grid that `render_tile` warps the layer onto
        let (_, origin) = tms_spec.grid_shape_and_origin(0);
        let tiling_grid_definition = geoengine_operators::processing::warped_spatial_grid(
            initialized_operator.result_descriptor(),
            &geoengine_operators::processing::WarpParams {
                target_spatial_reference: Some(SpatialReference::web_mercator()),
                output_resolution: Some(tms_spec.spatial_resolution(0)),
                output_origin_reference: Some(origin),
                method: geoengine_operators::processing::WarpMethod::NearestNeighbor,
            },
        )
        .unwrap()
        .tiling_grid_definition(tiling_spec);
        let bbox_0_0_0 = tms_spec
            .tile_grid_bbox(&tiling_grid_definition, 0, 0, 0)
            .unwrap();

        assert_eq!(bbox_0_0_0.axis_size(), [512, 512]);
        let upper_left = tiling_grid_definition
            .tiling_geo_transform()
            .grid_idx_to_pixel_upper_left_coordinate_2d(bbox_0_0_0.min_index());
        assert!(
            approx_eq!(f64, upper_left.x, origin.x, epsilon = 1e-6)
                && approx_eq!(f64, upper_left.y, origin.y, epsilon = 1e-6),
            "Tile 0/0/0 should start at the origin of tile matrix 0, starts at {upper_left:?}"
        );
    }
}
