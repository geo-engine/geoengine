use std::sync::Arc;

use crate::concurrency::{ThreadPool, ThreadPoolContext, ThreadPoolContextCreator};

/// A spatio-temporal rectangle for querying data
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, SpatialPartition2D, SpatialPartitioned, SpatialResolution,
    TimeInterval,
};
use serde::{Deserialize, Serialize};

/// A spatio-temporal rectangle for querying data with a bounding box
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    fn thread_pool_context(&self) -> &ThreadPoolContext;
}

pub struct MockQueryContext {
    pub chunk_byte_size: usize,
    pub thread_pool: ThreadPoolContext,
}

impl Default for MockQueryContext {
    fn default() -> Self {
        Self {
            chunk_byte_size: 1024 * 1024,
            thread_pool: Arc::new(ThreadPool::default()).create_context(),
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
            thread_pool: Arc::new(ThreadPool::new(num_threads)).create_context(),
        }
    }
}

impl QueryContext for MockQueryContext {
    fn chunk_byte_size(&self) -> usize {
        self.chunk_byte_size
    }

    fn thread_pool_context(&self) -> &ThreadPoolContext {
        &self.thread_pool
    }
}
