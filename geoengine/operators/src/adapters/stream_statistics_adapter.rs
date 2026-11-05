use crate::{engine::ProcessingGraphOperatorPath, meta::quota::QuotaTracking};
use futures::{Stream, ready};
use geoengine_datatypes::util::ByteSize;
use pin_project::{pin_project, pinned_drop};
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tracing::{Level, Span};

#[pin_project(PinnedDrop, project = StreamStatisticsAdapterProjection)]
pub struct StreamStatisticsAdapter<S> {
    #[pin]
    stream: S,
    poll_next_count: u64,
    element_count: u64,
    span: Span,
    quota: QuotaTracking,
    path: ProcessingGraphOperatorPath,
    operator_name: &'static str,
    /// If the wrapped stream is a source, pass the value of the `data` parameter to the quota tracking
    data: Option<String>,
    /// Compute metrics, only collected if this module logs at debug level
    metrics: Option<StreamMetrics>,
}

/// Like `DataFusion`'s `BaselineMetrics`, logged when the stream ends or is dropped.
/// The times of an operator overlap with those of its inputs and are neither exclusive nor CPU time.
#[derive(Default)]
struct StreamMetrics {
    /// The start of `wall_time_us`, which includes waiting for the consumer of the stream
    first_poll: Option<Instant>,
    /// Time spent in `poll_next`, including the operator's inputs, but not work spawned to other threads
    poll_time: Duration,
    output_bytes: u64,
    /// The stream produced an error
    failed: bool,
}

impl StreamMetrics {
    /// `completion` is `failed` after an error, otherwise `completed` at the end of the stream and `dropped` before it
    fn log(
        &self,
        ended: bool,
        operator_name: &str,
        path: &ProcessingGraphOperatorPath,
        element_count: u64,
    ) {
        let completion = match (self.failed, ended) {
            (true, _) => "failed",
            (false, true) => "completed",
            (false, false) => "dropped",
        };
        tracing::debug!(
            event = %"stream_metrics",
            operator = operator_name,
            path = %path,
            completion,
            elements = element_count,
            output_bytes = self.output_bytes,
            poll_time_us = self.poll_time.as_micros() as u64,
            wall_time_us = self.first_poll.map_or(0, |t| t.elapsed().as_micros() as u64),
        );
    }
}

impl<S> StreamStatisticsAdapter<S> {
    pub fn new(
        stream: S,
        span: Span,
        quota: QuotaTracking,
        path: ProcessingGraphOperatorPath,
        operator_name: &'static str,
        data: Option<String>,
    ) -> StreamStatisticsAdapter<S> {
        StreamStatisticsAdapter {
            stream,
            poll_next_count: 0,
            element_count: 0,
            span,
            quota,
            path,
            operator_name,
            data,
            metrics: tracing::enabled!(Level::DEBUG).then(StreamMetrics::default),
        }
    }

    pub fn poll_next_count(&self) -> u64 {
        self.poll_next_count
    }

    pub fn element_count(&self) -> u64 {
        self.element_count
    }

    pub fn not_ready_count(&self) -> u64 {
        self.poll_next_count - self.element_count
    }
}

impl<S, T, E> Stream for StreamStatisticsAdapter<S>
where
    S: Stream<Item = Result<T, E>>,
    T: ByteSize,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        *this.poll_next_count += 1;

        let _enter = this.span.enter();

        tracing::trace!(
            event = %"poll_next",
            poll_next_count = *this.poll_next_count,
        );

        let poll_start = this.metrics.as_ref().map(|_| Instant::now());
        let v = this.stream.as_mut().poll_next(cx);
        if let (Some(metrics), Some(poll_start)) = (this.metrics.as_mut(), poll_start) {
            metrics.first_poll.get_or_insert(poll_start);
            metrics.poll_time += poll_start.elapsed();
            match &v {
                Poll::Ready(Some(Ok(element))) => {
                    metrics.output_bytes += element.byte_size() as u64;
                }
                Poll::Ready(Some(Err(_))) => metrics.failed = true,
                _ => {}
            }
        }

        let v = ready!(v);
        match v {
            Some(_) => {
                *this.element_count += 1;
                tracing::trace!(
                    event = %"poll_next",
                    poll_next_count = *this.poll_next_count,
                    element_count = *this.element_count,
                    empty = false,
                );

                (*this.quota).work_unit_done(
                    this.operator_name,
                    this.path.clone(),
                    this.data.clone(),
                );
            }
            None => {
                tracing::trace!(
                    event = %"poll_next",
                    poll_next_count = *this.poll_next_count,
                    element_count = *this.element_count,
                    empty = true,
                );

                if let Some(metrics) = this.metrics.take() {
                    metrics.log(true, this.operator_name, this.path, *this.element_count);
                }
            }
        }
        Poll::Ready(v)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

#[pinned_drop]
impl<S> PinnedDrop for StreamStatisticsAdapter<S> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        // a stream that was never polled did no work, so there is nothing to report
        if let Some(metrics) = this.metrics.take().filter(|m| m.first_poll.is_some()) {
            let _enter = this.span.enter();
            metrics.log(false, this.operator_name, this.path, *this.element_count);
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::meta::quota::{ComputationUnit, QuotaMessage};
    use futures::StreamExt;
    use std::{
        io::Write,
        sync::{Arc, Mutex},
    };
    use tokio::sync::mpsc::unbounded_channel;
    use tracing::{Level, span};
    use uuid::Uuid;

    #[derive(Clone, Default)]
    struct Log(Arc<Mutex<Vec<u8>>>);

    impl Write for Log {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn it_logs_stream_metrics_once_with_the_completion() {
        let log = Log::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let (tx, _rx) = unbounded_channel::<QuotaMessage>();
        let quota = QuotaTracking::new(tx, Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let adapter = |items: Vec<Result<i32, ()>>| {
            StreamStatisticsAdapter::new(
                futures::stream::iter(items),
                span!(Level::DEBUG, "test"),
                quota.clone(),
                ProcessingGraphOperatorPath::initialize_root(),
                "test",
                None,
            )
        };

        let mut completed = adapter(vec![Ok(1)]);
        while completed.next().await.is_some() {}
        drop(completed);

        let mut failed_to_the_end = adapter(vec![Err(())]);
        while failed_to_the_end.next().await.is_some() {}
        drop(failed_to_the_end);

        let mut failed_and_dropped = adapter(vec![Err(()), Ok(1)]);
        failed_and_dropped.next().await;
        drop(failed_and_dropped);

        let mut dropped = adapter(vec![Ok(1), Ok(2)]);
        dropped.next().await;
        drop(dropped);

        drop(adapter(vec![Ok(1)])); // never polled

        let log = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
        let count = |text: &str| log.matches(text).count();
        assert_eq!(count("stream_metrics"), 4, "{log}");
        assert_eq!(count("completion=\"completed\""), 1, "{log}");
        assert_eq!(count("completion=\"failed\""), 2, "{log}");
        assert_eq!(count("completion=\"dropped\""), 1, "{log}");
    }

    #[tokio::test]
    async fn simple() {
        let v = vec![Ok::<_, ()>(1), Ok(2), Ok(3)];
        let v_stream = futures::stream::iter(v);
        let (tx, mut rx) = unbounded_channel::<QuotaMessage>();
        let user = Uuid::new_v4();
        let processing_graph = Uuid::new_v4();
        let computation = Uuid::new_v4();
        let quota = QuotaTracking::new(tx, user, processing_graph, computation);
        let mut v_stat_stream = StreamStatisticsAdapter::new(
            v_stream,
            span!(Level::TRACE, "test"),
            quota,
            ProcessingGraphOperatorPath::initialize_root(),
            "test",
            None,
        );

        let one = v_stat_stream.next().await;
        assert_eq!(one, Some(Ok(1)));
        assert_eq!(v_stat_stream.element_count(), 1);
        assert_eq!(v_stat_stream.poll_next_count(), 1);
        assert_eq!(v_stat_stream.not_ready_count(), 0);

        let two = v_stat_stream.next().await;
        assert_eq!(two, Some(Ok(2)));
        assert_eq!(v_stat_stream.element_count(), 2);
        assert_eq!(v_stat_stream.poll_next_count(), 2);
        assert_eq!(v_stat_stream.not_ready_count(), 0);

        let three = v_stat_stream.next().await;
        assert_eq!(three, Some(Ok(3)));
        assert_eq!(v_stat_stream.element_count(), 3);
        assert_eq!(v_stat_stream.poll_next_count(), 3);
        assert_eq!(v_stat_stream.not_ready_count(), 0);

        assert_eq!(
            rx.recv().await.unwrap(),
            ComputationUnit {
                user,
                processing_graph,
                computation,
                operator_name: "test",
                operator_path: ProcessingGraphOperatorPath::initialize_root(),
                data: None,
            }
            .into()
        );
    }
}
