use crate::source::gdal_worker_process::{GdalProcessPoolError, process_common::IpcProcessError};
use geoengine_datatypes::raster::RasterDataType;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum MdGdalSourceError {
    #[snafu(display("Unsupported raster type: {raster_type:?}"))]
    UnsupportedRasterType {
        raster_type: RasterDataType,
    },

    #[snafu(display(
        "MD data only supports requesting either the first band (z=time) or a band index (z=band): {message}"
    ))]
    UnsupportedBandRequest {
        message: String,
    },

    #[snafu(display("Error in the MD GdalSource reading process: {source}"))]
    IpcProcessError {
        source: IpcProcessError,
    },

    GdalProcessPoolError {
        source: GdalProcessPoolError,
    },

    #[snafu(display("MD GdalSource probe error: {message}"))]
    ProbeError {
        message: String,
    },
}

impl From<GdalProcessPoolError> for MdGdalSourceError {
    fn from(source: GdalProcessPoolError) -> Self {
        MdGdalSourceError::GdalProcessPoolError { source }
    }
}
