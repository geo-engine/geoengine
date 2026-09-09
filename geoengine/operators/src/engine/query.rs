use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    cache::shared_cache::SharedCache,
    error,
    meta::quota::QuotaChecker,
    source::gdal_worker_process::{GdalProcessPool, GdalProcessPoolAccess},
    util::create_rayon_thread_pool,
};
use crate::{meta::quota::QuotaTracking, util::Result};
use futures::Stream;
use geoengine_datatypes::{raster::TilingSpecification, util::test::TestDefault};
use pin_project::pin_project;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

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

pub trait QueryContext: Send + Sync + GdalProcessPoolAccess {
    fn chunk_byte_size(&self) -> ChunkByteSize;
    fn tiling_specification(&self) -> TilingSpecification;
    fn thread_pool(&self) -> &Arc<ThreadPool>;

    fn quota_tracking(&self) -> Option<&QuotaTracking>;

    fn quota_checker(&self) -> Option<&QuotaChecker>;

    fn cache(&self) -> Option<Arc<SharedCache>>;

    fn abort_registration(&self) -> &QueryAbortRegistration;
    fn abort_trigger(&mut self) -> Result<QueryAbortTrigger>;

    fn gdal_process_pool(&self) -> &Arc<GdalProcessPool> {
        self.get_gdal_pool()
    }
}

/// This type allow wrapping multiple streams with `QueryAbortWrapper`s that
/// can all be aborted at the same time using the corresponding `QueryAbortTrigger`.
pub struct QueryAbortRegistration {
    token: CancellationToken,
}

impl QueryAbortRegistration {
    pub fn new() -> (Self, QueryAbortTrigger) {
        let token = CancellationToken::new();

        (
            Self {
                token: token.clone(),
            },
            QueryAbortTrigger { token },
        )
    }

    /// Wraps a query result stream so that it yields `Error::QueryCanceled` when the query is
    /// aborted, instead of ending silently. This way the cancellation propagates through stream
    /// combinators like `try_fold` and the query tree stops producing output.
    pub fn wrap<S, T>(&self, stream: S) -> QueryAbortWrapper<S>
    where
        S: Stream<Item = Result<T>>,
    {
        QueryAbortWrapper {
            cancelled: self.token.clone().cancelled_owned(),
            inner: stream,
            ended: false,
        }
    }
}

/// This type wraps a stream and yields `Error::QueryCanceled` when the query is aborted using
/// the corresponding `QueryAbortTrigger` from its `QueryAbortRegistration`. After the error, the
/// stream ends.
#[pin_project]
pub struct QueryAbortWrapper<S> {
    #[pin]
    cancelled: WaitForCancellationFutureOwned,
    #[pin]
    inner: S,
    ended: bool,
}

impl<S, T> Stream for QueryAbortWrapper<S>
where
    S: Stream<Item = Result<T>>,
{
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if *this.ended {
            return Poll::Ready(None);
        }

        // Check the cancellation token before polling the inner stream so that no further items
        // are requested after an abort. Polling also registers a waker, so `cancel()` will
        // re-invoke this future.
        if this.cancelled.poll(cx).is_ready() {
            *this.ended = true;
            return Poll::Ready(Some(Err(error::Error::QueryCanceled)));
        }

        match this.inner.poll_next(cx) {
            Poll::Ready(None) => {
                *this.ended = true;
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

/// This type allows aborting all streams that were wrapped using the corresponding
/// `QueryAbortRegistration`.
pub struct QueryAbortTrigger {
    token: CancellationToken,
}

impl QueryAbortTrigger {
    pub fn abort(self) {
        self.token.cancel();
    }
}

pub struct MockQueryContext {
    pub chunk_byte_size: ChunkByteSize,
    pub tiling_specification: TilingSpecification,
    pub thread_pool: Arc<ThreadPool>,

    pub cache: Option<Arc<SharedCache>>,
    pub quota_tracking: Option<QuotaTracking>,
    pub quota_checker: Option<QuotaChecker>,

    pub abort_registration: QueryAbortRegistration,
    pub abort_trigger: Option<QueryAbortTrigger>,

    gdal_process_pool: Arc<GdalProcessPool>,
}

impl MockQueryContext {
    pub(super) fn new(
        chunk_byte_size: ChunkByteSize,
        tiling_specification: TilingSpecification,
        gdal_process_pool: Arc<GdalProcessPool>,
    ) -> Self {
        let (abort_registration, abort_trigger) = QueryAbortRegistration::new();
        Self {
            chunk_byte_size,
            tiling_specification,
            thread_pool: create_rayon_thread_pool(0),
            cache: None,
            quota_checker: None,
            quota_tracking: None,
            abort_registration,
            abort_trigger: Some(abort_trigger),
            gdal_process_pool,
        }
    }

    pub(super) fn new_with_query_extensions(
        chunk_byte_size: ChunkByteSize,
        tiling_specification: TilingSpecification,
        gdal_process_pool: Arc<GdalProcessPool>,
        cache: Option<Arc<SharedCache>>,
        quota_tracking: Option<QuotaTracking>,
        quota_checker: Option<QuotaChecker>,
    ) -> Self {
        let (abort_registration, abort_trigger) = QueryAbortRegistration::new();
        Self {
            chunk_byte_size,
            tiling_specification,
            thread_pool: create_rayon_thread_pool(0),
            cache,
            quota_checker,
            quota_tracking,
            abort_registration,
            abort_trigger: Some(abort_trigger),
            gdal_process_pool,
        }
    }

    pub(super) fn with_chunk_size_and_thread_count(
        chunk_byte_size: ChunkByteSize,
        tiling_specification: TilingSpecification,
        num_threads: usize,
        gdal_process_pool: Arc<GdalProcessPool>,
    ) -> Self {
        let (abort_registration, abort_trigger) = QueryAbortRegistration::new();
        Self {
            chunk_byte_size,
            tiling_specification,
            thread_pool: create_rayon_thread_pool(num_threads),
            gdal_process_pool,
            cache: None,
            quota_checker: None,
            quota_tracking: None,
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

    fn abort_trigger(&mut self) -> Result<QueryAbortTrigger> {
        self.abort_trigger
            .take()
            .ok_or(error::Error::AbortTriggerAlreadyUsed)
    }

    fn tiling_specification(&self) -> TilingSpecification {
        self.tiling_specification
    }

    fn quota_tracking(&self) -> Option<&QuotaTracking> {
        self.quota_tracking.as_ref()
    }

    fn quota_checker(&self) -> Option<&QuotaChecker> {
        self.quota_checker.as_ref()
    }

    fn cache(&self) -> Option<Arc<SharedCache>> {
        self.cache.clone()
    }
}

impl GdalProcessPoolAccess for MockQueryContext {
    fn get_gdal_pool(&self) -> &Arc<GdalProcessPool> {
        &self.gdal_process_pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::{self, StreamExt};

    #[tokio::test]
    async fn it_yields_query_canceled_error_when_aborted() {
        let (registration, trigger) = QueryAbortRegistration::new();
        let mut stream = Box::pin(registration.wrap(stream::iter(vec![Ok(1), Ok(2)])));

        assert!(matches!(stream.next().await, Some(Ok(1))));

        trigger.abort();

        assert!(matches!(
            stream.next().await,
            Some(Err(error::Error::QueryCanceled))
        ));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn it_errors_before_polling_inner_when_already_aborted() {
        let (registration, trigger) = QueryAbortRegistration::new();
        trigger.abort();

        let mut stream = Box::pin(registration.wrap(stream::pending::<Result<i32>>()));

        assert!(matches!(
            stream.next().await,
            Some(Err(error::Error::QueryCanceled))
        ));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn it_completes_normally_when_trigger_is_dropped_unfired() {
        let (registration, trigger) = QueryAbortRegistration::new();
        drop(trigger);

        let stream = registration.wrap(stream::iter(vec![Ok(1), Ok(2)]));
        let items = stream.collect::<Vec<_>>().await;

        assert!(matches!(items[..], [Ok(1), Ok(2)]));
    }
}
