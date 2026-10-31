use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
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
use geoengine_datatypes::{
    primitives::CacheTtlSeconds, raster::TilingSpecification, util::test::TestDefault,
};
use pin_project::pin_project;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

/// Defines the size in bytes of a vector data chunk
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct ChunkByteSize(usize);

/// The parallelism available to the sub-queries of a query.
///
/// Each adapter runs one sub-query without a permit, its guaranteed one. Every further one
/// needs a [`SubQueryPermit`] from this counter, which never waits: if none is left, the adapter
/// starts no more until it yields a tile. The counter is shared by all adapters of the query,
/// so nested adapters add up instead of multiplying their fanout. A minimum count would not
/// replace the guaranteed one: the outer adapter can take all permits before its inner ones ask.
pub struct SubQueryParallelism {
    available: Arc<AtomicUsize>,
}

/// One sub-query beyond an adapter's guaranteed one; gives it back to the counter when dropped.
pub struct SubQueryPermit(Arc<AtomicUsize>);

impl Drop for SubQueryPermit {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

impl SubQueryParallelism {
    /// Up to `available_per_query` sub-queries of the query run at once, counting the
    /// guaranteed one of each running adapter chain.
    pub fn new(available_per_query: usize) -> Self {
        Self {
            // the guaranteed sub-query takes nothing, so the counter holds the rest
            available: Arc::new(AtomicUsize::new(available_per_query.saturating_sub(1))),
        }
    }

    /// A permit, or `None` when the query's parallelism is in use.
    pub fn try_acquire(&self) -> Option<SubQueryPermit> {
        // Relaxed: the counter guards no other data, it only bounds the number of sub-queries
        self.available
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .ok()?;
        Some(SubQueryPermit(self.available.clone()))
    }
}

/// Parallel, so tests exercise concurrent sub-queries; servers read
/// `[query_context] available_subquery_parallelism_per_query` (default 1).
impl TestDefault for SubQueryParallelism {
    fn test_default() -> Self {
        Self::new(4)
    }
}

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
    fn default_cache_ttl(&self) -> CacheTtlSeconds;
    fn subquery_parallelism(&self) -> &SubQueryParallelism;

    fn quota_tracking(&self) -> Option<&QuotaTracking>;

    fn quota_checker(&self) -> Option<&QuotaChecker>;

    fn cache(&self) -> Option<Arc<SharedCache>>;

    fn new_raster_cache(&self) -> Option<Arc<crate::cache::new_raster_cache::NewRasterCacheEnum>> {
        None
    }

    /// The runtime that runs query tasks, see [`crate::util::spawn_query`].
    fn processing_runtime(&self) -> tokio::runtime::Handle;

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
            Poll::Ready(Some(Err(error::Error::QueryCanceled))) => {
                // the stream ends after a cancellation so that no further items are requested
                *this.ended = true;
                Poll::Ready(Some(Err(error::Error::QueryCanceled)))
            }
            other => other,
        }
    }
}

/// This type allows aborting all streams that were wrapped using the corresponding
/// `QueryAbortRegistration`.
///
/// Dropping the trigger cancels the query, just like the `Trigger` of the formerly used
/// `stream-cancel` crate did. This way, a trigger that is dropped by mistake on some
/// error path cannot leak a still-running query. Use [`QueryAbortTrigger::abort`] to
/// make the cancellation explicit.
pub struct QueryAbortTrigger {
    token: CancellationToken,
}

impl QueryAbortTrigger {
    pub fn abort(self) {
        self.token.cancel();
    }
}

impl Drop for QueryAbortTrigger {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

pub struct MockQueryContext {
    pub default_cache_ttl: CacheTtlSeconds,
    pub chunk_byte_size: ChunkByteSize,
    pub tiling_specification: TilingSpecification,
    pub thread_pool: Arc<ThreadPool>,
    pub subquery_parallelism: SubQueryParallelism,

    pub cache: Option<Arc<SharedCache>>,
    pub quota_tracking: Option<QuotaTracking>,
    pub quota_checker: Option<QuotaChecker>,
    pub new_raster_cache: Option<Arc<crate::cache::new_raster_cache::NewRasterCacheEnum>>,

    pub abort_registration: QueryAbortRegistration,
    pub abort_trigger: Option<QueryAbortTrigger>,

    /// see [`QueryContext::processing_runtime`]; the constructors use the current runtime
    pub processing_runtime: tokio::runtime::Handle,

    gdal_process_pool: Arc<GdalProcessPool>,
}

impl MockQueryContext {
    pub(super) fn new(
        chunk_byte_size: ChunkByteSize,
        tiling_specification: TilingSpecification,
        gdal_process_pool: Arc<GdalProcessPool>,
        default_cache_ttl: CacheTtlSeconds,
    ) -> Self {
        let (abort_registration, abort_trigger) = QueryAbortRegistration::new();
        Self {
            chunk_byte_size,
            tiling_specification,
            default_cache_ttl,
            thread_pool: create_rayon_thread_pool(0),
            subquery_parallelism: SubQueryParallelism::test_default(),
            cache: None,
            quota_checker: None,
            quota_tracking: None,
            new_raster_cache: Some(Arc::new(
                crate::cache::new_raster_cache::NewRasterCacheEnum::new_fifo(1_073_741_824),
            )),
            abort_registration,
            abort_trigger: Some(abort_trigger),
            processing_runtime: tokio::runtime::Handle::current(),
            gdal_process_pool,
        }
    }

    pub(super) fn new_with_query_extensions(
        chunk_byte_size: ChunkByteSize,
        tiling_specification: TilingSpecification,
        gdal_process_pool: Arc<GdalProcessPool>,
        default_cache_ttl: CacheTtlSeconds,
        cache: Option<Arc<SharedCache>>,
        quota_tracking: Option<QuotaTracking>,
        quota_checker: Option<QuotaChecker>,
    ) -> Self {
        let (abort_registration, abort_trigger) = QueryAbortRegistration::new();
        Self {
            chunk_byte_size,
            tiling_specification,
            default_cache_ttl,
            thread_pool: create_rayon_thread_pool(0),
            subquery_parallelism: SubQueryParallelism::test_default(),
            cache,
            quota_checker,
            quota_tracking,
            new_raster_cache: Some(Arc::new(
                crate::cache::new_raster_cache::NewRasterCacheEnum::new_fifo(1_073_741_824),
            )),
            abort_registration,
            abort_trigger: Some(abort_trigger),
            processing_runtime: tokio::runtime::Handle::current(),
            gdal_process_pool,
        }
    }

    pub(super) fn with_chunk_size_and_thread_count(
        chunk_byte_size: ChunkByteSize,
        tiling_specification: TilingSpecification,
        num_threads: usize,
        gdal_process_pool: Arc<GdalProcessPool>,
        default_cache_ttl: CacheTtlSeconds,
    ) -> Self {
        let (abort_registration, abort_trigger) = QueryAbortRegistration::new();
        Self {
            chunk_byte_size,
            tiling_specification,
            default_cache_ttl,
            thread_pool: create_rayon_thread_pool(num_threads),
            subquery_parallelism: SubQueryParallelism::test_default(),
            gdal_process_pool,
            cache: None,
            quota_checker: None,
            quota_tracking: None,
            new_raster_cache: Some(Arc::new(
                crate::cache::new_raster_cache::NewRasterCacheEnum::new_fifo(1_073_741_824),
            )),
            abort_registration,
            abort_trigger: Some(abort_trigger),
            processing_runtime: tokio::runtime::Handle::current(),
        }
    }
}

impl QueryContext for MockQueryContext {
    fn chunk_byte_size(&self) -> ChunkByteSize {
        self.chunk_byte_size
    }

    fn processing_runtime(&self) -> tokio::runtime::Handle {
        self.processing_runtime.clone()
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.thread_pool
    }

    fn default_cache_ttl(&self) -> CacheTtlSeconds {
        self.default_cache_ttl
    }

    fn subquery_parallelism(&self) -> &SubQueryParallelism {
        &self.subquery_parallelism
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

    fn new_raster_cache(&self) -> Option<Arc<crate::cache::new_raster_cache::NewRasterCacheEnum>> {
        self.new_raster_cache.clone()
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
    async fn it_cancels_when_trigger_is_dropped() {
        let (registration, trigger) = QueryAbortRegistration::new();
        drop(trigger);

        let mut stream = Box::pin(registration.wrap(stream::pending::<Result<i32>>()));

        assert!(matches!(
            stream.next().await,
            Some(Err(error::Error::QueryCanceled))
        ));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn it_ends_when_inner_yields_query_canceled() {
        let (registration, _trigger) = QueryAbortRegistration::new();

        let inner = stream::iter(vec![Ok(1), Err(error::Error::QueryCanceled), Ok(2)]);
        let mut stream = Box::pin(registration.wrap(inner));

        assert!(matches!(stream.next().await, Some(Ok(1))));
        assert!(matches!(
            stream.next().await,
            Some(Err(error::Error::QueryCanceled))
        ));
        assert!(stream.next().await.is_none());
    }

    #[test]
    fn it_hands_out_up_to_the_available_parallelism() {
        assert!(SubQueryParallelism::new(0).try_acquire().is_none());
        assert!(SubQueryParallelism::new(1).try_acquire().is_none());

        let parallelism = SubQueryParallelism::new(3);
        // the guaranteed one plus 2 permits = 3
        let held = [parallelism.try_acquire(), parallelism.try_acquire()];
        assert!(held.iter().all(Option::is_some));
        assert!(parallelism.try_acquire().is_none());

        drop(held);
        assert!(parallelism.try_acquire().is_some());
    }
}
