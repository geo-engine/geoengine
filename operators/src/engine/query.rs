use std::sync::Arc;

use crate::util::create_rayon_thread_pool;
use geoengine_datatypes::util::test::TestDefault;
use rayon::ThreadPool;
use serde::{Serialize, Deserialize};

/// Defines the size in bytes of a vector data chunk
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
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

impl TestDefault for ChunkByteSize {
    fn test_default() -> Self {
        Self(1024 * 1024)
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

impl TestDefault for MockQueryContext {
    fn test_default() -> Self {
        Self {
            chunk_byte_size: ChunkByteSize::test_default(),
            thread_pool: create_rayon_thread_pool(0),
        }
    }
}

impl MockQueryContext {
    pub fn new(chunk_byte_size: ChunkByteSize) -> Self {
        Self {
            chunk_byte_size,
            thread_pool: create_rayon_thread_pool(0),
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
