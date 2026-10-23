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
use geoengine_datatypes::{
    primitives::CacheTtlSeconds, raster::TilingSpecification, util::test::TestDefault,
};
use pin_project::pin_project;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

/// Defines the size in bytes of a vector data chunk
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct ChunkByteSize(usize);

/// The parallelism available to the sub-queries of a query.
///
/// An adapter asks for a [`SubQueryHandout`] before each sub-query:
/// - its first running sub-query is always handed out ([`SubQueryHandout::Guaranteed`]) and
///   takes nothing from any pool, which guarantees progress and avoids deadlocks;
/// - every further one ([`SubQueryHandout::Parallel`]) needs a free permit from the query's
///   pool AND from the server's pool, and is refused instead of waiting.
///
/// The query's pool is shared by all adapters of the query, so nested adapters add up
/// instead of multiplying their fanout. The server's pool is shared by all queries, so an
/// idle server gives a single query its full parallelism and a busy one runs queries
/// sequentially: parallelism within a query adds no throughput once the GDAL pool and the
/// thread pool are saturated.
pub struct SubQueryParallelism {
    per_query: Arc<Semaphore>,
    per_server: Arc<Semaphore>,
}

/// Permission to run one more sub-query, held until its tile is yielded.
pub enum SubQueryHandout {
    /// The first running sub-query of an adapter, always handed out.
    Guaranteed,
    /// A sub-query running in parallel to the guaranteed one.
    Parallel(ParallelSubQueryPermit),
}

/// Returns its permits to the query's and the server's pool when dropped.
pub struct ParallelSubQueryPermit {
    _per_query: OwnedSemaphorePermit,
    _per_server: OwnedSemaphorePermit,
}

impl SubQueryParallelism {
    pub const DEFAULT_AVAILABLE_PER_QUERY: usize = 4;

    /// Up to `available_per_query` sub-queries of the query run at once, counting the
    /// guaranteed one, however deep adapters nest; fewer while `per_server` has no permits.
    pub fn new(available_per_query: usize, per_server: Arc<Semaphore>) -> Self {
        Self {
            // the guaranteed sub-query takes no permit, so the pool holds the rest
            per_query: Arc::new(Semaphore::new(available_per_query.saturating_sub(1))),
            per_server,
        }
    }

    /// Like [`Self::new`], with a server pool no other query uses.
    pub fn fixed(available_per_query: usize) -> Self {
        Self::new(
            available_per_query,
            Arc::new(Semaphore::new(available_per_query.saturating_sub(1))),
        )
    }

    /// The handout for an adapter's next sub-query while it has `running` sub-queries in
    /// flight; `None` when all parallelism is in use.
    ///
    /// Never waits: waiting on a permit deadlocks when outer sub-queries hold all of them.
    pub fn handout(&self, running: usize) -> Option<SubQueryHandout> {
        if running == 0 {
            return Some(SubQueryHandout::Guaranteed);
        }
        let per_query = self.per_query.clone().try_acquire_owned().ok()?;
        let per_server = self.per_server.clone().try_acquire_owned().ok()?;
        Some(SubQueryHandout::Parallel(ParallelSubQueryPermit {
            _per_query: per_query,
            _per_server: per_server,
        }))
    }
}

impl Default for SubQueryParallelism {
    fn default() -> Self {
        Self::fixed(Self::DEFAULT_AVAILABLE_PER_QUERY)
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
            subquery_parallelism: SubQueryParallelism::default(),
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
            subquery_parallelism: SubQueryParallelism::default(),
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
            subquery_parallelism: SubQueryParallelism::default(),
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

    fn is_parallel(handout: Option<&SubQueryHandout>) -> bool {
        matches!(handout, Some(SubQueryHandout::Parallel(_)))
    }

    #[test]
    fn it_always_hands_out_the_guaranteed_subquery() {
        for parallelism in [
            SubQueryParallelism::fixed(0),
            SubQueryParallelism::fixed(1),
            SubQueryParallelism::new(4, Arc::new(Semaphore::new(0))),
        ] {
            assert!(matches!(
                parallelism.handout(0),
                Some(SubQueryHandout::Guaranteed)
            ));
            assert!(parallelism.handout(1).is_none());
        }
    }

    #[test]
    fn it_hands_out_up_to_the_available_parallelism() {
        let parallelism = SubQueryParallelism::fixed(3);
        let held = [parallelism.handout(1), parallelism.handout(2)];
        assert!(held.iter().all(|h| is_parallel(h.as_ref())));
        // the guaranteed one plus 2 parallel ones = 3
        assert!(parallelism.handout(3).is_none());

        drop(held);
        assert!(is_parallel(parallelism.handout(1).as_ref()));
    }

    #[test]
    fn it_shares_the_server_pool_between_queries() {
        let per_server = Arc::new(Semaphore::new(2));
        let busy = SubQueryParallelism::new(4, per_server.clone());
        let other = SubQueryParallelism::new(4, per_server.clone());

        let held = [busy.handout(1), busy.handout(2)];
        assert!(other.handout(1).is_none(), "server pool is used up");
        // the per-query permit taken before the server refused is returned
        assert_eq!(other.per_query.available_permits(), 3);
        assert!(matches!(
            other.handout(0),
            Some(SubQueryHandout::Guaranteed)
        ));

        drop(held);
        assert!(is_parallel(other.handout(1).as_ref()));
    }
}
