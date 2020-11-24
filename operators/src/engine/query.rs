use crate::util::Result;
use geoengine_datatypes::dataset::DataSetId;
use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution, TimeInterval};
use std::collections::HashMap;

/// A spatio-temporal rectangle for querying data
#[derive(Copy, Clone, Debug)]
pub struct QueryRectangle {
    pub bbox: BoundingBox2D,
    pub time_interval: TimeInterval,
    pub spatial_resolution: SpatialResolution,
}

#[derive(Debug, Clone)]
pub enum LoadingInfo {
    Gdal(GdalLoadingInfo),
}

// TODO: find appropriate place for loading infos
#[derive(Debug, Clone)]
pub struct GdalLoadingInfo {
    pub path: String,
    // TODO:
}

pub trait QueryContext: Send + Sync {
    fn chunk_byte_size(&self) -> usize;
    // TODO: make async
    fn loading_info(&self, data_set: DataSetId) -> Result<LoadingInfo>;
}

pub struct MockQueryContext {
    pub chunk_byte_size: usize,
    pub loading_infos: HashMap<DataSetId, LoadingInfo>,
}

impl MockQueryContext {
    pub fn new(chunk_byte_size: usize) -> Self {
        Self {
            chunk_byte_size,
            loading_infos: Default::default(),
        }
    }
}

impl QueryContext for MockQueryContext {
    fn chunk_byte_size(&self) -> usize {
        self.chunk_byte_size
    }

    fn loading_info(&self, data_set: DataSetId) -> Result<LoadingInfo> {
        self.loading_infos.get(&data_set).map(Clone::clone).ok_or(
            crate::error::Error::LoadingInfo {
                reason: "Data set not known".into(),
            },
        )
    }
}
