use std::pin::Pin;

use futures::stream::{Fuse, FusedStream};
use futures::task::{Context, Poll};
use futures::{Stream, StreamExt};
use pin_project::pin_project;

/// An adapter for merging the outputs of multiple streams.
///
/// The merged stream produces items from one to all of the underlying
/// streams as they become available. Errors, however, are not merged: you
/// get at most one error at a time.
#[derive(Debug)]
#[pin_project(project = StreamArrayZipProjection)]
pub struct StreamArrayZip<St: Stream, const N: usize> {
    #[pin]
    streams: [Fuse<St>; N],
    values: [Option<St::Item>; N],
}

/// An adapter for merging the outputs of multiple streams.
///
/// The merged stream produces items from one to all of the underlying
/// streams as they become available. Errors, however, are not merged: you
/// get at most one error at a time.
#[derive(Debug)]
#[pin_project(project = StreamVectorZipProjection)]
pub struct StreamVectorZip<St: Stream> {
    #[pin]
    streams: Vec<Fuse<St>>,
    values: Vec<Option<St::Item>>,
}

impl<St: Stream, const N: usize> StreamArrayZip<St, N> {
    /// Creates a new stream zip.
    ///
    /// # Panics
    /// Panics if `streams` is empty.
    pub fn new(streams: [St; N]) -> Self {
        assert!(!streams.is_empty());

        Self {
            streams: streams.map(StreamExt::fuse),
            values: Self::array_of_none(),
        }
    }

    /// Since `St::Item` is not copy, we cannot use `[None; N]`
    #[inline]
    fn array_of_none() -> [Option<St::Item>; N] {
        [(); N].map(|_| None)
    }
}

impl<St: Stream> StreamVectorZip<St> {
    /// Creates a new stream zip.
    ///
    /// # Panics
    /// Panics if `streams` is empty.
    pub fn new(streams: Vec<St>) -> Self {
        assert!(!streams.is_empty());

        Self {
            values: Self::vec_of_none(streams.len()),
            streams: streams.into_iter().map(StreamExt::fuse).collect(),
        }
    }

    /// Since `St::Item` is not copy, we cannot use `vec![None; N]`
    #[inline]
    fn vec_of_none(len: usize) -> Vec<Option<St::Item>> {
        (0..len).map(|_| None).collect()
    }
}

impl<St, const N: usize> Stream for StreamArrayZip<St, N>
where
    St: Stream + Unpin,
{
    type Item = [St::Item; N];

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        for (stream, value) in this.streams.as_mut().iter_mut().zip(this.values.as_mut()) {
            if value.is_none() {
                match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(item)) => *value = Some(item),
                    Poll::Ready(None) | Poll::Pending => {}
                }
            }
        }

        if this.values.iter().all(Option::is_some) {
            let values: [Option<St::Item>; N] =
                std::mem::replace(this.values, Self::array_of_none());
            let tuple: [St::Item; N] = values.map(Option::unwrap);

            Poll::Ready(Some(tuple))
        } else if this.streams.iter().any(FusedStream::is_terminated) {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let mut streams_lower = usize::MAX;
        let mut streams_upper = None::<usize>;

        for (stream, value) in self.streams.iter().zip(&self.values) {
            let value_len = if value.is_some() { 1 } else { 0 };

            let (lower, upper) = stream.size_hint();

            streams_lower = streams_lower.min(lower.saturating_add(value_len));

            streams_upper = match (streams_upper, upper) {
                (Some(x), Some(y)) => Some(x.min(y.saturating_add(value_len))),
                (Some(x), None) => Some(x),
                (None, Some(y)) => y.checked_add(value_len),
                (None, None) => None,
            };
        }

        (streams_lower, streams_upper)
    }
}

impl<St> Stream for StreamVectorZip<St>
where
    St: Stream + Unpin,
{
    type Item = Vec<St::Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        for (stream, value) in this.streams.as_mut().iter_mut().zip(this.values.iter_mut()) {
            if value.is_none() {
                match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(item)) => *value = Some(item),
                    Poll::Ready(None) | Poll::Pending => {}
                }
            }
        }

        if this.values.iter().all(Option::is_some) {
            let tuple: Vec<St::Item> = this
                .values
                .iter_mut()
                .map(|o| o.take().expect("checked in if-condition"))
                .collect();

            Poll::Ready(Some(tuple))
        } else if this.streams.iter().any(FusedStream::is_terminated) {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let mut streams_lower = usize::MAX;
        let mut streams_upper = None::<usize>;

        for (stream, value) in self.streams.iter().zip(&self.values) {
            let value_len = if value.is_some() { 1 } else { 0 };

            let (lower, upper) = stream.size_hint();

            streams_lower = streams_lower.min(lower.saturating_add(value_len));

            streams_upper = match (streams_upper, upper) {
                (Some(x), Some(y)) => Some(x.min(y.saturating_add(value_len))),
                (Some(x), None) => Some(x),
                (None, Some(y)) => y.checked_add(value_len),
                (None, None) => None,
            };
        }

        (streams_lower, streams_upper)
    }
}

impl<St, const N: usize> FusedStream for StreamArrayZip<St, N>
where
    St: Stream + Unpin,
{
    fn is_terminated(&self) -> bool {
        self.streams.iter().all(FusedStream::is_terminated)
    }
}

impl<St> FusedStream for StreamVectorZip<St>
where
    St: Stream + Unpin,
{
    fn is_terminated(&self) -> bool {
        self.streams.iter().all(FusedStream::is_terminated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_stream::stream;
    use futures::stream::BoxStream;
    use futures::StreamExt;

    #[tokio::test]
    async fn concurrent_stream() {
        let st1 = stream! {
            for i in 1..=3 {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                yield i;
            }
        };

        let st2 = stream! {
            for i in 1..=3 {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                yield i * 10;
            }
        };

        let st1: BoxStream<'static, u32> = Box::pin(st1);
        let st2: BoxStream<'static, u32> = Box::pin(st2);

        let st_all = StreamArrayZip::new([st1, st2]);

        let start = std::time::Instant::now();

        let values: Vec<[u32; 2]> = st_all.collect().await;

        assert!(start.elapsed() < std::time::Duration::from_millis(500));

        assert_eq!(values, [[1, 10], [2, 20], [3, 30]]);
    }

    #[tokio::test]
    async fn compare_with_zip() {
        let st1 = futures::stream::iter(vec![1, 2, 3]);
        let st2 = futures::stream::iter(vec![1, 2, 3, 4]);

        // init
        let st_array_zip = StreamArrayZip::new([st1.clone().fuse(), st2.clone().fuse()]);
        let st_vector_zip = StreamVectorZip::new(vec![st1.clone().fuse(), st2.clone().fuse()]);
        let st_zip = st1.zip(st2);

        // size hints
        assert_eq!(st_array_zip.size_hint(), st_zip.size_hint());
        assert_eq!(st_vector_zip.size_hint(), st_zip.size_hint());

        // output
        let o1: Vec<[i32; 2]> = st_array_zip.collect().await;
        let o2: Vec<[i32; 2]> = st_zip.map(|(v1, v2)| [v1, v2]).collect().await;
        let o3: Vec<[i32; 2]> = st_vector_zip.map(|vs| [vs[0], vs[1]]).collect().await;
        assert_eq!(o1, o2);
        assert_eq!(o1, o3);
    }
}
