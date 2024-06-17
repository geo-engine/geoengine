use geoengine_datatypes::{primitives::TimeInterval, raster::RasterDataType};
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum GdalSourceError {
    #[snafu(display("Unsupported raster type: {raster_type:?}"))]
    UnsupportedRasterType { raster_type: RasterDataType },
    
    #[snafu(display("Missing metadata from a data provider to fill the query time interval. Query time {query_time} < First element time {first_element_time}"))]
    MissingTemporalDataAtQueryStart {
        query_time: TimeInterval,
        first_element_time: TimeInterval,
    },
    #[snafu(display("Missing metadata from a data provider to fill the query time interval. Query time {query_time} > Last element time {last_element_time}"))]
    MissingTemporalDataAtQueryEnd {
        query_time: TimeInterval,
        last_element_time: TimeInterval,
    },
    #[snafu(display("Missing metadata from a data provider to fill the query time interval. Query time {query_time}. Only element time {only_element_time}"))]
    MissingTemporalDataOnlyElement {
        query_time: TimeInterval,
        only_element_time: TimeInterval,
    },

}
