use crate::api::model::datatypes::{
    DataProviderId, LayerId, SpatialReference, SpatialReferenceAuthority,
};
use actix_http::StatusCode;
use geoengine_datatypes::error::ErrorSource;
use ogcapi_types::common::Exception;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(
    visibility(pub(crate)),
    context(suffix(false)), // disables default `Snafu` suffix
)]
pub enum OgcApiError {
    #[snafu(display("Internal error"), context(false))]
    Internal { source: crate::error::Error },

    #[snafu(display("Invalid processing graph: Expected `raster`, found `{found}`"))]
    ExpectedRaster { found: String },

    #[snafu(display("Layer `{layer_id}` does not exist in provider `{data_connector_id}`"))]
    LayerNotFound {
        data_connector_id: DataProviderId,
        layer_id: LayerId,
        source: crate::error::Error,
    },

    #[snafu(display("Collection `{collection_id}` does not exist"))]
    CollectionNotFound { collection_id: LayerId },

    #[snafu(display("Tile matrix set `{tile_matrix_set_id}` does not exist"))]
    TileMatrixSetNotFound { tile_matrix_set_id: String },

    #[snafu(display(
        "Tile matrix set `{tile_matrix_set_id}` definition is not available: {reason}"
    ))]
    TileMatrixSetDefinitionNotAvailable {
        tile_matrix_set_id: String,
        reason: String,
    },

    #[snafu(display("Invalid tile coordinates: matrix={matrix}, row={row}, col={col}"))]
    InvalidTileCoordinates { matrix: String, row: u32, col: u32 },

    #[snafu(display("Received 3D bounding box, but only 2D is supported."))]
    Unsupported3DBoundingBox { coords: [f64; 6] },

    #[snafu(display("Invalid bounding box: {source}"))]
    InvalidBoundingBox { source: Box<dyn ErrorSource> },

    #[snafu(display("Error while initializing processing graph: {source}"))]
    InitializingProcessingGraph { source: Box<dyn ErrorSource> },

    #[snafu(display("Missing spatial reference in result descriptor"))]
    MissingSpatialReference,

    #[snafu(display("Missing time in query"))]
    MissingTime,

    #[snafu(display("Unsupported spatial reference authority: {from}"))]
    UnsupportedSpatialReferenceAuthority { from: SpatialReferenceAuthority },

    #[snafu(display("Unknown info `{info}` for spatial reference authority: {from}"))]
    UnknownSpatialReferenceInfo {
        source: Box<dyn ErrorSource>,
        from: SpatialReference,
        info: String,
    },
}

impl OgcApiError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::CollectionNotFound { .. }
            | Self::LayerNotFound { .. }
            | Self::TileMatrixSetNotFound { .. } => StatusCode::NOT_FOUND,
            Self::ExpectedRaster { .. }
            | Self::InvalidBoundingBox { .. }
            | Self::InvalidTileCoordinates { .. }
            | Self::MissingSpatialReference
            | Self::MissingTime
            | Self::Unsupported3DBoundingBox { .. } => StatusCode::BAD_REQUEST,
            Self::Internal { .. }
            | Self::InitializingProcessingGraph { .. }
            | Self::TileMatrixSetDefinitionNotAvailable { .. }
            | Self::UnknownSpatialReferenceInfo { .. }
            | Self::UnsupportedSpatialReferenceAuthority { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    pub fn title(&self) -> &str {
        match self {
            Self::CollectionNotFound { .. } => "Collection not found",
            Self::ExpectedRaster { .. } => "Invalid processing graph",
            Self::InitializingProcessingGraph { .. } => "Error while initializing processing graph",
            Self::Internal { .. } => "An internal error occurred",
            Self::InvalidBoundingBox { .. } => "Invalid bounding box",
            Self::InvalidTileCoordinates { .. } => "Invalid tile coordinates",
            Self::LayerNotFound { .. } => "Layer not found",
            Self::MissingSpatialReference => "Missing spatial reference",
            Self::MissingTime => "Missing time",
            Self::TileMatrixSetDefinitionNotAvailable { .. } => {
                "Tile matrix set definition not available"
            }
            Self::TileMatrixSetNotFound { .. } => "Tile matrix set not found",
            Self::UnknownSpatialReferenceInfo { .. } => "Unknown spatial reference info",
            Self::Unsupported3DBoundingBox { .. } => "Unsupported 3D bounding box",
            Self::UnsupportedSpatialReferenceAuthority { .. } => {
                "Unsupported spatial reference authority"
            }
        }
    }
}

impl From<&OgcApiError> for Exception {
    fn from(error: &OgcApiError) -> Self {
        Exception::new_from_status(error.status_code().into()) // TODO: custom error type URIs
            .title(error.title())
            .detail(error.to_string())
    }
}

impl actix_web::error::ResponseError for OgcApiError {
    fn status_code(&self) -> StatusCode {
        self.status_code()
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        let exception = Exception::from(self);
        actix_web::HttpResponse::build(self.status_code()).json(&exception)
    }
}

#[cfg(test)]
mod tests {
    use actix_web::ResponseError;

    use super::*;

    #[test]
    fn it_returns_correct_status_codes_for_ogc_api_errors() {
        // Test 404 errors
        assert_eq!(
            OgcApiError::CollectionNotFound {
                collection_id: LayerId("test_collection".into()),
            }
            .status_code(),
            StatusCode::NOT_FOUND
        );

        assert_eq!(
            OgcApiError::TileMatrixSetNotFound {
                tile_matrix_set_id: "UnknownTMS".to_string(),
            }
            .status_code(),
            StatusCode::NOT_FOUND
        );

        // Test 400 errors
        assert_eq!(
            OgcApiError::ExpectedRaster {
                found: "vector".to_string(),
            }
            .status_code(),
            StatusCode::BAD_REQUEST
        );

        assert_eq!(
            OgcApiError::MissingSpatialReference.status_code(),
            StatusCode::BAD_REQUEST
        );

        // Test 500 errors
        assert_eq!(
            OgcApiError::Internal {
                source: crate::error::Error::InvalidUuid,
            }
            .status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );

        assert_eq!(
            OgcApiError::UnsupportedSpatialReferenceAuthority {
                from: SpatialReferenceAuthority::Epsg,
            }
            .status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn it_generates_valid_exception_response_with_correct_json_structure() {
        let error = OgcApiError::TileMatrixSetNotFound {
            tile_matrix_set_id: "TestTMS".to_string(),
        };

        let response = error.error_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // Verify Exception structure
        let exception = Exception::from(&error);
        let json = serde_json::to_value(&exception).expect("Failed to serialize exception");

        assert_eq!(json["status"], 404);
        assert_eq!(json["title"], "Tile matrix set not found");
        assert!(json["detail"].is_string());
        assert!(json["detail"].as_str().unwrap().contains("TestTMS"));
    }
}
