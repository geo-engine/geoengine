mod db_types;
mod gdal_dataset_params;
mod grid_and_properties;
pub mod process_common;
pub mod process_impl;
mod process_pool;
pub mod reader;
pub mod reader_mode;

pub use gdal_dataset_params::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalMetadataMapping,
    GdalRetryOptions, GdalSourceTimePlaceholder, TimeReference,
};
pub use grid_and_properties::GridAndProperties;
pub use process_impl::{OpenTelemetryConfig, WorkerConfig, WorkerLoggingConfig};
pub use process_pool::{GdalPoolDispatcher, GdalProcessPool, GdalProcessPoolError};
pub use reader::{GdalPoolReader, GdalProcessReadResult};
pub use reader_mode::{GdalReaderMode, OverviewReaderState, ReaderState};

pub trait GdalProcessPoolAccess {
    fn get_gdal_pool(&self) -> &std::sync::Arc<GdalProcessPool>;
    fn get_gdal_worker(&self) -> GdalPoolDispatcher {
        GdalPoolDispatcher::new(self.get_gdal_pool().clone())
    }
}
