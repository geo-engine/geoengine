use geoengine_datatypes::raster::{GridBoundingBox2D, RasterDataType};
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum GdalSourceError {
    #[snafu(display("Unsupported raster type: {raster_type:?}"))]
    UnsupportedRasterType { raster_type: RasterDataType },

    #[snafu(display("Unsupported spatial query: {spatial_query:?}"))]
    IncompatibleSpatialQuery { spatial_query: GridBoundingBox2D },
}
