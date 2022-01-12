use std::sync::Arc;

pub use geoengine_datatypes::primitives::{
    PlotQueryRectangle, QueryRectangle, RasterQueryRectangle, VectorQueryRectangle,
};
use rayon::ThreadPool;

use crate::util::create_rayon_thread_pool;

/// Defines the size in bytes of a vector data chunk
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChunkByteSize(usize);

impl ChunkByteSize {
    pub const MIN: ChunkByteSize = ChunkByteSize(usize::MIN);
    pub const MAX: ChunkByteSize = ChunkByteSize(usize::MAX);

    pub fn new(cbs: usize) -> Self {
        ChunkByteSize(cbs)
    }

    pub fn bytes(self) -> usize {
        self.0
    }
}

impl From<usize> for ChunkByteSize {
    fn from(size: usize) -> Self {
        ChunkByteSize(size)
    }
}

impl From<ChunkByteSize> for usize {
    fn from(cbs: ChunkByteSize) -> Self {
        cbs.0
    }
}

impl Default for ChunkByteSize {
    fn default() -> Self {
        Self(1024 * 1024) // TODO: find reasonable default
    }
}

pub trait QueryContext: Send + Sync {
    fn chunk_byte_size(&self) -> ChunkByteSize;
    fn thread_pool(&self) -> &Arc<ThreadPool>;
}

pub struct MockQueryContext {
    pub chunk_byte_size: ChunkByteSize,
    pub thread_pool: Arc<ThreadPool>,
}

impl Default for MockQueryContext {
    fn default() -> Self {
        Self {
            chunk_byte_size: ChunkByteSize::default(),
            thread_pool: create_rayon_thread_pool(0),
        }
    }
}

impl MockQueryContext {
    pub fn new(chunk_byte_size: ChunkByteSize) -> Self {
        Self {
            chunk_byte_size,
            ..Default::default()
        }
    }

    pub fn with_chunk_size_and_thread_count(
        chunk_byte_size: ChunkByteSize,
        num_threads: usize,
    ) -> Self {
        Self {
            chunk_byte_size,
            thread_pool: create_rayon_thread_pool(num_threads),
        }
    }
}

impl QueryContext for MockQueryContext {
    fn chunk_byte_size(&self) -> ChunkByteSize {
        self.chunk_byte_size
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.thread_pool
    }
}
