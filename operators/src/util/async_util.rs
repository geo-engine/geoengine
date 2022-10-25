use futures::{future::BoxFuture, Future, FutureExt};
use log::debug;
use rayon::ThreadPool;
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::{engine::QueryAbortTrigger, error, util::Result};

/// A wrapper around `tokio::task::spawn_blocking` that wraps the
/// function into the parent `Span` from `tracing`.
#[inline]
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let current_span = tracing::Span::current();

    tokio::task::spawn_blocking(move || {
        let _entered_span = current_span.enter();

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
    let current_span = tracing::Span::current();

    tokio::task::spawn_blocking(move || {
        thread_pool.install(move || {
            let _entered_span = current_span.enter();

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
    let current_span = tracing::Span::current();

    tokio::task::spawn(async move {
        // TODO: check if we need to move a span into here
        let _entered_span = current_span.enter();

        future.await
    })
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
        Box::pin(abort_future.map(|_| Err(error::Error::QueryCanceled))),
    ])
    .await;

    if matches!(result, Err(error::Error::QueryCanceled)) {
        abort_trigger.abort();
        debug!("Query canceled");
        return Err(error::Error::QueryCanceled);
    }

    result
}
