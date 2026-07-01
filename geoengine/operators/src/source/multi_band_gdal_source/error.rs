use crate::source::gdal_in::{GdalProcessPoolError, process_common::IpcProcessError};
use geoengine_datatypes::raster::{GridBoundingBox2D, RasterDataType};
use snafu::Snafu;

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

    #[snafu(display("Error in the GdalSource reading process: {source}"))]
    IpcProcessError {
        source: IpcProcessError,
    },

    GdalProcessPoolError {
        source: GdalProcessPoolError,
    },
}

impl From<GdalProcessPoolError> for GdalSourceError {
    fn from(source: GdalProcessPoolError) -> Self {
        GdalSourceError::GdalProcessPoolError { source }
    }
}
