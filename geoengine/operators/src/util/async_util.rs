use futures::{Future, FutureExt, future::BoxFuture};
use rayon::ThreadPool;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{Instrument, debug};
use tracing::{Level, span};

use crate::{engine::QueryAbortTrigger, error, util::Result};

/// A wrapper around `tokio::task::spawn_blocking` that wraps the
/// function into the parent `Span` from `tracing`.
#[inline]
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let current_span = span!(Level::DEBUG, "spawn_blocking").or_current();

    tokio::task::spawn_blocking(move || {
        let _entered_span = current_span.enter();
        let _work = tracing::debug_span!("blocking_work").entered();

        f()
    })
}

/// A wrapper around `tokio::task::spawn_blocking` that wraps the
/// function into the parent `Span` from `tracing`.
/// Additionally, it installs a Rayon thread pool.
#[inline]
pub fn spawn_blocking_with_thread_pool<F, R>(thread_pool: Arc<ThreadPool>, f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let current_span = span!(Level::DEBUG, "spawn_blocking_with_thread_pool").or_current();

    tokio::task::spawn_blocking(move || {
        let _entered_span = current_span.enter();
        let work_parent = current_span.clone();
        thread_pool.install(move || {
            let _entered_span = work_parent.enter();
            let _work = tracing::debug_span!("blocking_work").entered();

            f()
        })
    })
}

/// A wrapper around `tokio::task::spawn` that wraps the
/// function into the parent `Span` from `tracing`.
#[inline]
pub fn spawn<T>(future: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    tokio::spawn(future.instrument(tracing::Span::current()))
}

/// execute a future that consumes a query and abort it using the trigger if the abort future completes
pub async fn abortable_query_execution<F: Future<Output = Result<T>> + Send, T>(
    execution: F,
    abort_future: BoxFuture<'_, ()>,
    abort_trigger: QueryAbortTrigger,
) -> F::Output {
    let execution: BoxFuture<F::Output> = Box::pin(execution);

    let (result, _, _) = futures::future::select_all([
        execution,
        Box::pin(abort_future.map(|()| Err(error::Error::QueryCanceled))),
    ])
    .await;

    if matches!(result, Err(error::Error::QueryCanceled)) {
        abort_trigger.abort();
        debug!("Query canceled");
        return Err(error::Error::QueryCanceled);
    }

    result
}
