use futures::{ready, Stream};
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tracing::Span;

#[pin_project(project = StreamStatisticsAdapterProjection)]
pub struct StreamStatisticsAdapter<S> {
    #[pin]
    stream: S,
    poll_next_count: u64,
    element_count: u64,
    span: Span,
}

impl<S> StreamStatisticsAdapter<S> {
    pub fn new(stream: S, span: Span) -> StreamStatisticsAdapter<S> {
        StreamStatisticsAdapter {
            stream,
            poll_next_count: 0,
            element_count: 0,
            span,
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

impl<S> Stream for StreamStatisticsAdapter<S>
where
    S: Stream,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        *this.poll_next_count += 1;

        let _enter = this.span.enter();

        tracing::trace!(event = "poll_next", poll_next_count = *this.poll_next_count);

        let v = ready!(this.stream.as_mut().poll_next(cx));
        match v {
            Some(_) => {
                *this.element_count += 1;
                tracing::debug!(
                    event = "poll_next",
                    poll_next_count = *this.poll_next_count,
                    element_count = *this.element_count,
                    empty = false
                );
            }
            None => {
                tracing::debug!(
                    event = "poll_next",
                    poll_next_count = *this.poll_next_count,
                    element_count = *this.element_count,
                    empty = true
                );
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
    use futures::StreamExt;
    use tracing::{span, Level};

    use super::*;

    #[tokio::test]
    async fn simple() {
        let v = vec![1, 2, 3];
        let v_stream = futures::stream::iter(v);
        let mut v_stat_stream = StreamStatisticsAdapter::new(v_stream, span!(Level::TRACE, "test"));

        let one = v_stat_stream.next().await;
        assert_eq!(one, Some(1));
        assert_eq!(v_stat_stream.element_count(), 1);
        assert_eq!(v_stat_stream.poll_next_count(), 1);
        assert_eq!(v_stat_stream.not_ready_count(), 0);

        let two = v_stat_stream.next().await;
        assert_eq!(two, Some(2));
        assert_eq!(v_stat_stream.element_count(), 2);
        assert_eq!(v_stat_stream.poll_next_count(), 2);
        assert_eq!(v_stat_stream.not_ready_count(), 0);

        let three = v_stat_stream.next().await;
        assert_eq!(three, Some(3));
        assert_eq!(v_stat_stream.element_count(), 3);
        assert_eq!(v_stat_stream.poll_next_count(), 3);
        assert_eq!(v_stat_stream.not_ready_count(), 0);
    }
}
