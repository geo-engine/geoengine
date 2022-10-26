use futures::Future;
use rayon::ThreadPool;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{span, Level};

/// A wrapper around `tokio::task::spawn_blocking` that wraps the
/// function into the parent `Span` from `tracing`.
#[inline]
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let current_span = span!(Level::TRACE, "spawn_blocking");

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
    let current_span = span!(Level::TRACE, "spawn_blocking_with_thread_pool");

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
    let current_span = span!(Level::TRACE, "spawn");

    tokio::task::spawn(async move {
        // TODO: check if we need to move a span into here
        let _entered_span = current_span.enter();

        future.await
    })
}
