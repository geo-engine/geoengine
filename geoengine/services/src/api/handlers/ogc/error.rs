use crate::{api::model::datatypes::SpatialReferenceAuthority, workflows::workflow::WorkflowId};
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

    #[snafu(display("Collection `{collection_id}` does not exist"))]
    CollectionNotFound { collection_id: WorkflowId },

    #[snafu(display("Tile matrix set `{tile_matrix_set_id}` does not exist"))]
    TileMatrixSetNotFound { tile_matrix_set_id: String },

    #[snafu(display("Received 3D bounding box, but only 2D is supported."))]
    Unsupported3DBoundingBox { coords: [f64; 6] },

    #[snafu(display("Invalid bounding box: {source}"))]
    InvalidBoundingBox { source: Box<dyn ErrorSource> },

    #[snafu(display("Error while initializing processing graph: {source}"))]
    InitializingProcessingGraph { source: Box<dyn ErrorSource> },

    #[snafu(display("Missing spatial reference in result descriptor"))]
    MissingSpatialReference,

    #[snafu(display("Unsupported spatial reference authority: {from}"))]
    UnsupportedSpatialReferenceAuthority { from: SpatialReferenceAuthority },
}

impl OgcApiError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            OgcApiError::CollectionNotFound { .. } | OgcApiError::TileMatrixSetNotFound { .. } => {
                StatusCode::NOT_FOUND
            }
            OgcApiError::ExpectedRaster { .. }
            | OgcApiError::Unsupported3DBoundingBox { .. }
            | OgcApiError::InvalidBoundingBox { .. }
            | OgcApiError::MissingSpatialReference => StatusCode::BAD_REQUEST,
            OgcApiError::Internal { .. }
            | OgcApiError::InitializingProcessingGraph { .. }
            | OgcApiError::UnsupportedSpatialReferenceAuthority { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    pub fn title(&self) -> &str {
        match self {
            OgcApiError::Internal { .. } => "An internal error occurred",
            OgcApiError::ExpectedRaster { .. } => "Invalid processing graph",
            OgcApiError::CollectionNotFound { .. } => "Collection not found",
            OgcApiError::TileMatrixSetNotFound { .. } => "Tile matrix set not found",
            OgcApiError::Unsupported3DBoundingBox { .. } => "Unsupported 3D bounding box",
            OgcApiError::InvalidBoundingBox { .. } => "Invalid bounding box",
            OgcApiError::InitializingProcessingGraph { .. } => {
                "Error while initializing processing graph"
            }
            OgcApiError::MissingSpatialReference => "Missing spatial reference",
            OgcApiError::UnsupportedSpatialReferenceAuthority { .. } => {
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
