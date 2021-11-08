use std::sync::Arc;

/// A spatio-temporal rectangle for querying data
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, SpatialPartition2D, SpatialPartitioned, SpatialResolution,
    TimeInterval,
};
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};

use crate::util::create_rayon_thread_pool;

/// A spatio-temporal rectangle for querying data with a bounding box
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRectangle<SpatialBounds: AxisAlignedRectangle> {
    pub spatial_bounds: SpatialBounds,
    pub time_interval: TimeInterval,
    pub spatial_resolution: SpatialResolution,
}

pub type VectorQueryRectangle = QueryRectangle<BoundingBox2D>;
pub type RasterQueryRectangle = QueryRectangle<SpatialPartition2D>;
pub type PlotQueryRectangle = QueryRectangle<BoundingBox2D>;

impl SpatialPartitioned for VectorQueryRectangle {
    fn spatial_partition(&self) -> SpatialPartition2D {
        SpatialPartition2D::with_bbox_and_resolution(self.spatial_bounds, self.spatial_resolution)
    }
}

impl SpatialPartitioned for RasterQueryRectangle {
    fn spatial_partition(&self) -> SpatialPartition2D {
        self.spatial_bounds
    }
}

impl From<VectorQueryRectangle> for RasterQueryRectangle {
    fn from(value: VectorQueryRectangle) -> Self {
        Self {
            spatial_bounds: value.spatial_partition(),
            time_interval: value.time_interval,
            spatial_resolution: value.spatial_resolution,
        }
    }
}

pub trait QueryContext: Send + Sync {
    fn chunk_byte_size(&self) -> usize;
    fn thread_pool(&self) -> &Arc<ThreadPool>;
}

pub struct MockQueryContext {
    pub chunk_byte_size: usize,
    pub thread_pool: Arc<ThreadPool>,
}

impl Default for MockQueryContext {
    fn default() -> Self {
        Self {
            chunk_byte_size: 1024 * 1024,
            thread_pool: create_rayon_thread_pool(0),
        }
    }
}

impl MockQueryContext {
    pub fn new(chunk_byte_size: usize) -> Self {
        Self {
            chunk_byte_size,
            ..Default::default()
        }
    }

    pub fn with_chunk_size_and_thread_count(chunk_byte_size: usize, num_threads: usize) -> Self {
        Self {
            chunk_byte_size,
            thread_pool: create_rayon_thread_pool(num_threads),
        }
    }
}

impl QueryContext for MockQueryContext {
    fn chunk_byte_size(&self) -> usize {
        self.chunk_byte_size
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.thread_pool
    }
}
