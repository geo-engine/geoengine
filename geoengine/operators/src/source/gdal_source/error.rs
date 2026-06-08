use std::io;

use geoengine_datatypes::raster::{GridBoundingBox2D, RasterDataType};
use ipc_channel::IpcError;
use snafu::Snafu;

use crate::source::{
    IpcProcessError,
    gdal_source::{GdalProcessPoolError, GdalRasterLoaderError},
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum GdalSourceError {
    #[snafu(display("Unsupported raster type: {raster_type:?}"))]
    UnsupportedRasterType {
        raster_type: RasterDataType,
    },

    #[snafu(display("Unsupported spatial query: {spatial_query:?}"))]
    IncompatibleSpatialQuery {
        spatial_query: GridBoundingBox2D,
    },

    #[snafu(display("GdalProcess with an unknown error: {error}"))]
    UnknownErrorHappenedWhileReading {
        error: String,
    },

    #[snafu(display("Encountered a poisoned ProcessData Mutex"))]
    ProcessLockPoisoned {
        error: String,
    },

    #[snafu(display("Error while sending data through ipc_channel: {error}"))]
    IpcSendError {
        error: IpcError,
    },

    #[snafu(display("Error while receiving data through ipc_channel: {error}"))]
    IpcReceiveError {
        error: IpcError,
    },

    #[snafu(display("Error while setting up the GdalSource reading process: {reason}"))]
    ProcessSetupFailed {
        reason: io::Error,
    },

    #[snafu(display("Error in the GdalSource reading process: {source}"))]
    IpcProcessError {
        source: IpcProcessError,
    },

    GdalRasterLoader {
        source: GdalRasterLoaderError,
    },

    PoolInitializationError,

    WorkerPanic,

    GdalProcessPoolError {
        source: GdalProcessPoolError,
    },
}

impl From<GdalRasterLoaderError> for GdalSourceError {
    fn from(source: GdalRasterLoaderError) -> Self {
        GdalSourceError::GdalRasterLoader { source }
    }
}

impl From<GdalProcessPoolError> for GdalSourceError {
    fn from(source: GdalProcessPoolError) -> Self {
        GdalSourceError::GdalProcessPoolError { source }
    }
}
