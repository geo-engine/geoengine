mod create_from_processing_graph;
pub(crate) mod dataset_listing_provider;
pub mod external; // TODO: move to layers/external
pub mod listing;
mod name;
pub mod postgres;
pub mod storage;
pub mod upload;

#[cfg(test)]
pub(crate) use create_from_processing_graph::RasterDatasetFromProcessingGraphResult;
pub(crate) use create_from_processing_graph::{
    RasterDatasetFromProcessingGraph, RasterDatasetFromProcessingGraphParams,
    schedule_raster_dataset_from_processing_graph_task,
};
pub use name::{DatasetIdAndName, DatasetName, DatasetNameError};
pub use storage::AddDataset;
