use crate::api::model::responses::ErrorResponse;
use crate::error;
use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use geoengine_datatypes::util::helpers::ge_report;
use snafu::prelude::*;
use std::fmt;
use strum::IntoStaticStr;

#[derive(Snafu, IntoStaticStr)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum CreateDatasetError {
    UploadNotFound {
        source: error::Error,
    },
    OnlyAdminsCanCreateDatasetFromVolume,
    AdminsCannotCreateDatasetFromUpload,
    CannotResolveUploadFilePath {
        source: error::Error,
    },
    #[snafu(display("Cannot create dataset: {source}"))]
    CannotCreateDataset {
        source: error::Error,
    },
    JsonValidationFailed {
        source: error::Error,
    },
    DatabaseAccessError {
        source: error::Error,
    },
    CannotAccessConfig {
        source: error::Error,
    },
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

impl fmt::Debug for CreateDatasetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ge_report(self))
    }
}

#[derive(Snafu, IntoStaticStr)]
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

impl fmt::Debug for GetDatasetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ge_report(self))
    }
}

#[derive(Snafu, IntoStaticStr)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum UpdateDatasetError {
    #[snafu(display("Cannot load dataset for update"))]
    CannotLoadDatasetForUpdate { source: error::Error },
    #[snafu(display("Cannot update dataset"))]
    CannotUpdateDataset { source: error::Error },
}

impl ResponseError for UpdateDatasetError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(ErrorResponse::from(self))
    }

    fn status_code(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}

impl fmt::Debug for UpdateDatasetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ge_report(self))
    }
}

#[derive(Snafu, IntoStaticStr)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum AddDatasetTilesError {
    #[snafu(display("Cannot load dataset for adding tiles"))]
    CannotLoadDatasetForAddingTiles { source: error::Error },
    #[snafu(display("Cannot add tiles to dataset: {source}"))]
    CannotAddTilesToDataset { source: error::Error },
}

impl ResponseError for AddDatasetTilesError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(ErrorResponse::from(self))
    }

    fn status_code(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}

impl fmt::Debug for AddDatasetTilesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ge_report(self))
    }
}
