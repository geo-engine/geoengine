use futures::stream::BoxStream;
use futures::{Future, FutureExt, StreamExt, future::BoxFuture};
use geoengine_datatypes::primitives::{PlotQueryRectangle, QueryRectangle};
use rayon::ThreadPool;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio_util::task::AbortOnDropHandle;
use tracing::debug;
use tracing::{Instrument, Level, span};

use crate::{
    engine::{
        PlotQueryProcessor, QueryAbortTrigger, QueryContext, QueryProcessor, QueryProcessorExt,
    },
    error,
    util::Result,
};

/// How many finished items a query task may run ahead of its consumer: enough to keep both
/// busy, few enough to bound what a slow consumer holds in memory.
const QUERY_TASK_CHANNEL_CAPACITY: usize = 2;

/// Runs a query as a task on the query context's processing runtime (else the current one) and
/// streams its items through a bounded channel, so the caller's runtime, e.g. an HTTP worker,
/// only receives finished items. Dropping the stream aborts the task.
pub fn spawn_query<Q>(
    processor: Q,
    query: QueryRectangle<Q::SpatialBounds, Q::Selection>,
    query_ctx: Box<dyn QueryContext>,
) -> BoxStream<'static, Result<Q::Output>>
where
    Q: QueryProcessor + 'static,
    Q::Output: Send + 'static,
{
    let runtime = query_ctx
        .processing_runtime()
        .cloned()
        .unwrap_or_else(Handle::current);
    let (sender, mut receiver) = tokio::sync::mpsc::channel(QUERY_TASK_CHANNEL_CAPACITY);

    let task = AbortOnDropHandle::new(
        runtime.spawn(
            async move {
                let mut stream = match processor.query_into_owned_stream(query, query_ctx).await {
                    Ok(stream) => stream,
                    Err(error) => {
                        sender.send(Err(error)).await.ok();
                        return;
                    }
                };
                while let Some(item) = stream.next().await {
                    if sender.send(item).await.is_err() {
                        return; // the consumer is gone
                    }
                }
            }
            .in_current_span(),
        ),
    );

    let items = futures::stream::poll_fn(move |cx| receiver.poll_recv(cx));
    // a panicked task closes the channel early, so its error ends the stream instead
    let task_error = futures::stream::once(task)
        .filter_map(|outcome| async move { outcome.err().map(|error| Err(error.into())) });

    items.chain(task_error).boxed()
}

/// Like [`spawn_query`] for a plot, whose result is a single value.
pub async fn spawn_plot_query<T: Send + 'static>(
    processor: Box<dyn PlotQueryProcessor<OutputFormat = T>>,
    query: PlotQueryRectangle,
    query_ctx: Box<dyn QueryContext>,
) -> Result<T> {
    let runtime = query_ctx
        .processing_runtime()
        .cloned()
        .unwrap_or_else(Handle::current);
    AbortOnDropHandle::new(runtime.spawn(
        async move { processor.plot_query(query, query_ctx.as_ref()).await }.in_current_span(),
    ))
    .await?
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContext, RasterResultDescriptor, TimeDescriptor};
    use async_trait::async_trait;
    use geoengine_datatypes::primitives::{BandSelection, Coordinate2D, TimeInterval};
    use geoengine_datatypes::raster::{GeoTransform, GridBoundingBox2D, RasterDataType};
    use geoengine_datatypes::util::test::TestDefault;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Produces 0, 1, 2, … forever and counts what it produced.
    struct Counting {
        descriptor: RasterResultDescriptor,
        produced: Arc<AtomicUsize>,
        fail_at: Option<usize>,
        panic_at: Option<usize>,
    }

    impl Counting {
        fn new(fail_at: Option<usize>, panic_at: Option<usize>) -> Self {
            Self {
                descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
                    RasterDataType::U8,
                    1,
                    GridBoundingBox2D::new([0, 0], [1, 1]).unwrap(),
                    GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.).unwrap(),
                    TimeDescriptor::new_irregular(None),
                ),
                produced: Arc::new(AtomicUsize::new(0)),
                fail_at,
                panic_at,
            }
        }
    }

    #[async_trait]
    impl QueryProcessor for Counting {
        type Output = usize;
        type SpatialBounds = GridBoundingBox2D;
        type Selection = BandSelection;
        type ResultDescription = RasterResultDescriptor;

        async fn _query<'a>(
            &'a self,
            _query: QueryRectangle<GridBoundingBox2D, BandSelection>,
            _ctx: &'a dyn QueryContext,
        ) -> Result<BoxStream<'a, Result<usize>>> {
            Ok(futures::stream::iter(0..)
                .map(|i| {
                    self.produced.fetch_add(1, Ordering::SeqCst);
                    assert_ne!(Some(i), self.panic_at, "panic requested by the test");
                    if Some(i) == self.fail_at {
                        Err(error::Error::QueryProcessor)
                    } else {
                        Ok(i)
                    }
                })
                .boxed())
        }

        fn result_descriptor(&self) -> &RasterResultDescriptor {
            &self.descriptor
        }
    }

    fn query() -> QueryRectangle<GridBoundingBox2D, BandSelection> {
        QueryRectangle::new(
            GridBoundingBox2D::new([0, 0], [1, 1]).unwrap(),
            TimeInterval::default(),
            BandSelection::first(),
        )
    }

    fn query_ctx() -> Box<dyn QueryContext> {
        Box::new(MockExecutionContext::test_default().mock_query_context_test_default())
    }

    #[tokio::test]
    async fn it_streams_items_in_order_and_forwards_errors() {
        let items: Vec<_> = spawn_query(Counting::new(Some(2), None), query(), query_ctx())
            .take(4)
            .collect()
            .await;

        assert!(matches!(items.first(), Some(Ok(0))));
        assert!(matches!(items.get(1), Some(Ok(1))));
        assert!(matches!(
            items.get(2),
            Some(Err(error::Error::QueryProcessor))
        ));
        assert!(matches!(items.get(3), Some(Ok(3))));
    }

    #[tokio::test]
    async fn it_stops_the_task_when_the_stream_is_dropped() {
        let processor = Counting::new(None, None);
        let produced = processor.produced.clone();
        let mut items = spawn_query(processor, query(), query_ctx());

        assert!(matches!(items.next().await, Some(Ok(0))));
        drop(items);

        tokio::time::sleep(Duration::from_millis(20)).await;
        let after_drop = produced.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            produced.load(Ordering::SeqCst),
            after_drop,
            "task kept running"
        );
        // the consumed item, the channel's capacity and one item waiting to be sent
        assert!(after_drop <= 2 + QUERY_TASK_CHANNEL_CAPACITY);
    }

    #[tokio::test]
    async fn it_ends_with_an_error_when_the_task_panics() {
        let items: Vec<_> = spawn_query(Counting::new(None, Some(1)), query(), query_ctx())
            .collect()
            .await;

        assert_eq!(items.len(), 2);
        assert!(matches!(items.first(), Some(Ok(0))));
        assert!(matches!(
            items.get(1),
            Some(Err(error::Error::TokioJoin { .. }))
        ));
    }
}
