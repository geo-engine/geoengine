use std::sync::Arc;

use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution, TimeInterval};

use crate::concurrency::{ThreadPool, ThreadPoolContext, ThreadPoolContextCreator};

/// A spatio-temporal rectangle for querying data
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct QueryRectangle {
    pub bbox: BoundingBox2D,
    pub time_interval: TimeInterval,
    pub spatial_resolution: SpatialResolution,
}

pub trait QueryContext: Send + Sync {
    fn chunk_byte_size(&self) -> usize;
    fn thread_pool(&self) -> &ThreadPoolContext;
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

    fn thread_pool(&self) -> &ThreadPoolContext {
        &self.thread_pool
    }
}
