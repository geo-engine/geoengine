use crate::{engine::ProcessingGraphOperatorPath, meta::quota::QuotaTracking};
use futures::{Stream, ready};
use geoengine_datatypes::util::ByteSize;
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tracing::{Level, Span};

#[pin_project(project = StreamStatisticsAdapterProjection)]
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

/// Like `DataFusion`'s `BaselineMetrics`, logged at the end of the stream
#[derive(Default)]
struct StreamMetrics {
    first_poll: Option<Instant>,
    /// Time spent in `poll_next`, including the operator's inputs, but not work spawned to other threads
    poll_time: Duration,
    output_bytes: u64,
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
            if let Poll::Ready(Some(Ok(element))) = &v {
                metrics.output_bytes += element.byte_size() as u64;
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
                    tracing::debug!(
                        event = %"stream_metrics",
                        operator = *this.operator_name,
                        path = %this.path,
                        elements = *this.element_count,
                        output_bytes = metrics.output_bytes,
                        poll_time_us = metrics.poll_time.as_micros() as u64,
                        wall_time_us = metrics.first_poll.map_or(0, |t| t.elapsed().as_micros() as u64),
                    );
                }
            }
        }
        Poll::Ready(v)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::meta::quota::{ComputationUnit, QuotaMessage};
    use futures::StreamExt;
    use tokio::sync::mpsc::unbounded_channel;
    use tracing::{Level, span};
    use uuid::Uuid;

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
