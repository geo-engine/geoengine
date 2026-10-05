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
    CannotLoadDatasetForAddingTiles {
        source: error::Error,
    },
    #[snafu(display("Cannot add tiles to dataset: {source}"))]
    CannotAddTilesToDataset {
        source: error::Error,
    },
    #[snafu(display("Cannot add tiles to dataset that is not a GdalMultiBand"))]
    DatasetIsNotGdalMultiBand,
    #[snafu(display("Tile file path does not exist: {file_path}"))]
    TileFilePathDoesNotExist {
        file_path: String,
        absolute_path: String,
    },
    DatasetIsMissingDataPath,
    TileFilePathNotRelative {
        file_path: String,
    },
    TileFileMustBeExternalPath {
        file_path: String,
    },
    CannotOpenTileFile {
        source: geoengine_operators::error::Error,
        file_path: String,
    },
    CannotGetRasterDescriptorFromTileFile {
        source: geoengine_operators::error::Error,
        file_path: String,
    },
    TileFileDataTypeMismatch {
        expected: geoengine_datatypes::raster::RasterDataType,
        found: geoengine_datatypes::raster::RasterDataType,
        file_path: String,
    },
    TileFileSpatialReferenceMismatch {
        expected: geoengine_datatypes::spatial_reference::SpatialReferenceOption,
        found: geoengine_datatypes::spatial_reference::SpatialReferenceOption,
        file_path: String,
    },
    TileFileBandDoesNotExist {
        band_count: u32,
        found: u32,
        file_path: String,
    },
    TileFileGeoTransformMismatch {
        expected: geoengine_datatypes::raster::GeoTransform,
        found: geoengine_datatypes::raster::GeoTransform,
        file_path: String,
    },
    InvalidTileFileGeoTransform {
        file_path: String,
    },
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

#[derive(Snafu, IntoStaticStr)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum MdProbeError {
    #[snafu(display("No files to probe"))]
    NoFilesToProbe,
    #[snafu(display("Cannot resolve the data path: {message}"))]
    CannotResolveDataPath { message: String },
    #[snafu(display("Cannot probe the MD arrays: {message}"))]
    ProbeFailed { message: String },
    #[snafu(display("A z batch must hold at least one slice"))]
    InvalidMaxZBatchSize,
    #[snafu(display("Invalid probe file path '{file_path}': {message}"))]
    InvalidProbeFilePath { file_path: String, message: String },
}

impl ResponseError for MdProbeError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(ErrorResponse::from(self))
    }

    fn status_code(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}

impl fmt::Debug for MdProbeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ge_report(self))
    }
}

#[derive(Snafu, IntoStaticStr)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum AddDatasetMdTilesError {
    #[snafu(display("Cannot load dataset for adding MD tiles"))]
    CannotLoadDatasetForAddingMdTiles { source: error::Error },
    #[snafu(display("Cannot add MD tiles to dataset: {source}"))]
    CannotAddMdTilesToDataset { source: error::Error },
    #[snafu(display("Cannot add MD tiles to dataset that is not an MdGdalSource"))]
    DatasetIsNotMdGdal,
    #[snafu(display("Dataset is missing a data path"))]
    MdDatasetIsMissingDataPath,
    /// the file path failed one of the shared tile path checks
    InvalidMdTileFile {
        source: AddDatasetTilesError,
        file_path: String,
    },
    CannotOpenMdTileFile {
        source: geoengine_operators::error::Error,
        file_path: String,
    },
    MdTileBandDoesNotExist {
        band_count: u32,
        found: u32,
        file_path: String,
    },
    MdTileGeoTransformMismatch {
        expected: geoengine_datatypes::raster::GeoTransform,
        found: geoengine_datatypes::raster::GeoTransform,
        file_path: String,
    },
    /// the array's z size disagrees with the declared time axis
    MdTileSliceCountMismatch {
        expected: usize,
        found: usize,
        file_path: String,
    },
    #[snafu(display(
        "MD tile '{file_path}' declares no time steps; one interval per z slice is required"
    ))]
    MdTileMissingTimeSteps { file_path: String },
    #[snafu(display(
        "MD tile '{file_path}' declares a time descriptor that does not describe its time steps"
    ))]
    MdTileTimeAxisInconsistent { file_path: String },
    #[snafu(display("MD tile '{file_path}' declares time steps that do not run in time order"))]
    MdTileTimeAxisUnordered { file_path: String },
}

impl ResponseError for AddDatasetMdTilesError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(ErrorResponse::from(self))
    }

    fn status_code(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}

impl fmt::Debug for AddDatasetMdTilesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ge_report(self))
    }
}
