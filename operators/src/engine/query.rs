use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::util::create_rayon_thread_pool;
use futures::Stream;
use geoengine_datatypes::util::test::TestDefault;
use pin_project::pin_project;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use stream_cancel::{Trigger, Valve, Valved};

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

    fn abort_registration(&self) -> &QueryAbortRegistration;
    fn abort_trigger(&mut self) -> Option<QueryAbortTrigger>;
}

pub struct QueryAbortRegistration {
    valve: Valve,
}

impl QueryAbortRegistration {
    pub fn new() -> (Self, QueryAbortTrigger) {
        let (trigger, valve) = Valve::new();

        (Self { valve }, QueryAbortTrigger { trigger })
    }

    pub fn wrap<S: Stream>(&self, stream: S) -> QueryAbortWrapper<S> {
        QueryAbortWrapper {
            valved: self.valve.wrap(stream),
        }
    }
}

#[pin_project(project = AbortWrapperProjection)]
pub struct QueryAbortWrapper<S> {
    #[pin]
    valved: Valved<S>,
}

impl<S> Stream for QueryAbortWrapper<S>
where
    S: Stream,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().valved.poll_next(cx)
    }
}

pub struct QueryAbortTrigger {
    trigger: Trigger,
}

impl QueryAbortTrigger {
    pub fn abort(self) {
        self.trigger.cancel();
    }
}

pub struct MockQueryContext {
    pub chunk_byte_size: ChunkByteSize,
    pub thread_pool: Arc<ThreadPool>,
    pub abort_registration: QueryAbortRegistration,
    pub abort_trigger: Option<QueryAbortTrigger>,
}

impl TestDefault for MockQueryContext {
    fn test_default() -> Self {
        let (abort_registration, abort_trigger) = QueryAbortRegistration::new();
        Self {
            chunk_byte_size: ChunkByteSize::test_default(),
            thread_pool: create_rayon_thread_pool(0),
            abort_registration,
            abort_trigger: Some(abort_trigger),
        }
    }
}

impl MockQueryContext {
    pub fn new(chunk_byte_size: ChunkByteSize) -> Self {
        let (abort_registration, abort_trigger) = QueryAbortRegistration::new();
        Self {
            chunk_byte_size,
            thread_pool: create_rayon_thread_pool(0),
            abort_registration,
            abort_trigger: Some(abort_trigger),
        }
    }

    pub fn with_chunk_size_and_thread_count(
        chunk_byte_size: ChunkByteSize,
        num_threads: usize,
    ) -> Self {
        let (abort_registration, abort_trigger) = QueryAbortRegistration::new();
        Self {
            chunk_byte_size,
            thread_pool: create_rayon_thread_pool(num_threads),
            abort_registration,
            abort_trigger: Some(abort_trigger),
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

    fn abort_registration(&self) -> &QueryAbortRegistration {
        &self.abort_registration
    }

    fn abort_trigger(&mut self) -> Option<QueryAbortTrigger> {
        self.abort_trigger.take()
    }
}
