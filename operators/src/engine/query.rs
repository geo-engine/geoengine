use std::sync::Arc;

use crate::util::create_rayon_thread_pool;
use geoengine_datatypes::util::test::TestDefault;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use stream_cancel::{Trigger, Valve};

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

    fn valve(&self) -> &Valve;
    fn valve_trigger(&mut self) -> Option<Trigger>;
}

pub struct MockQueryContext {
    pub chunk_byte_size: ChunkByteSize,
    pub thread_pool: Arc<ThreadPool>,
    pub valve: Valve,
    pub valve_trigger: Option<Trigger>,
}

impl TestDefault for MockQueryContext {
    fn test_default() -> Self {
        let (trigger, valve) = Valve::new();
        Self {
            chunk_byte_size: ChunkByteSize::test_default(),
            thread_pool: create_rayon_thread_pool(0),
            valve,
            valve_trigger: Some(trigger),
        }
    }
}

impl MockQueryContext {
    pub fn new(chunk_byte_size: ChunkByteSize) -> Self {
        let (trigger, valve) = Valve::new();
        Self {
            chunk_byte_size,
            thread_pool: create_rayon_thread_pool(0),
            valve,
            valve_trigger: Some(trigger),
        }
    }

    pub fn with_chunk_size_and_thread_count(
        chunk_byte_size: ChunkByteSize,
        num_threads: usize,
    ) -> Self {
        let (trigger, valve) = Valve::new();
        Self {
            chunk_byte_size,
            thread_pool: create_rayon_thread_pool(num_threads),
            valve,
            valve_trigger: Some(trigger),
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

    fn valve(&self) -> &Valve {
        &self.valve
    }

    fn valve_trigger(&mut self) -> Option<Trigger> {
        self.valve_trigger.take()
    }
}
