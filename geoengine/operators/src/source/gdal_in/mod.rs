mod db_types;
mod gdal_dataset_params;
mod grid_and_properties;
pub mod process_common;
pub mod process_impl;
mod process_pool;

pub use gdal_dataset_params::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalMetadataMapping,
    GdalRetryOptions, GdalSourceTimePlaceholder, TimeReference,
};
pub use grid_and_properties::GridAndProperties;
pub use process_pool::{GdalPoolWorkerInstance, GdalProcessPool, GdalProcessPoolError};

pub trait GdalProcessPoolAccess {
    fn get_gdal_pool(&self) -> &std::sync::Arc<GdalProcessPool>;
    fn get_gdal_worker(&self) -> GdalPoolWorkerInstance {
        GdalPoolWorkerInstance::new(self.get_gdal_pool().clone())
    }
}
