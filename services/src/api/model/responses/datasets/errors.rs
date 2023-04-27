use crate::error;
use crate::handlers::ErrorResponse;
use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use snafu::prelude::*;
use strum::IntoStaticStr;

#[derive(Debug, Snafu, IntoStaticStr)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum CreateDatasetError {
    UploadNotFound { source: error::Error },
    OnlyAdminsCanCreateDatasetFromVolume,
    AdminsCannotCreateDatasetFromUpload,
    CannotResolveUploadFilePath { source: error::Error },
    JsonValidationFailed { source: error::Error },
    DatabaseAccessError { source: error::Error },
    CannotAccessConfig { source: error::Error },
    UnknownVolume,
}

impl ResponseError for CreateDatasetError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(ErrorResponse::from(self))
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Self::DatabaseAccessError { .. } | Self::CannotAccessConfig { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            _ => StatusCode::BAD_REQUEST,
        }
    }
}

#[derive(Debug, Snafu, IntoStaticStr)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum GetDatasetError {
    CannotLoadDataset { source: error::Error },
}

impl ResponseError for GetDatasetError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(ErrorResponse::from(self))
    }

    fn status_code(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}
