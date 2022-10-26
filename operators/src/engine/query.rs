use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};

use crate::util::create_rayon_thread_pool;
use geoengine_datatypes::util::test::TestDefault;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};

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

    fn extensions(&self) -> &QueryContextExtensions;
}

#[derive(Default)]
pub struct QueryContextExtensions {
    map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl QueryContextExtensions {
    pub fn insert<T: 'static + Send + Sync>(&mut self, val: T) -> Option<T> {
        self.map
            .insert(TypeId::of::<T>(), Box::new(val))
            .and_then(downcast_owned)
    }

    pub fn get<T: 'static + Send + Sync>(&self) -> Option<&T> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref())
    }
}

fn downcast_owned<T: 'static + Send + Sync>(boxed: Box<dyn Any + Send + Sync>) -> Option<T> {
    boxed.downcast().ok().map(|boxed| *boxed)
}

pub struct MockQueryContext {
    pub chunk_byte_size: ChunkByteSize,
    pub thread_pool: Arc<ThreadPool>,

    pub extensions: QueryContextExtensions,
}

impl TestDefault for MockQueryContext {
    fn test_default() -> Self {
        Self {
            chunk_byte_size: ChunkByteSize::test_default(),
            thread_pool: create_rayon_thread_pool(0),
            extensions: QueryContextExtensions::default(),
        }
    }
}

impl MockQueryContext {
    pub fn new(chunk_byte_size: ChunkByteSize) -> Self {
        Self {
            chunk_byte_size,
            thread_pool: create_rayon_thread_pool(0),
            extensions: QueryContextExtensions::default(),
        }
    }

    pub fn with_chunk_size_and_thread_count(
        chunk_byte_size: ChunkByteSize,
        num_threads: usize,
    ) -> Self {
        Self {
            chunk_byte_size,
            thread_pool: create_rayon_thread_pool(num_threads),
            extensions: QueryContextExtensions::default(),
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

    fn extensions(&self) -> &QueryContextExtensions {
        &self.extensions
    }
}
