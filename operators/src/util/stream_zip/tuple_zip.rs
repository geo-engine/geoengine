use std::pin::Pin;

use futures::stream::{Fuse, FusedStream};
use futures::task::{Context, Poll};
use futures::{Stream, StreamExt};
use pin_project::pin_project;

/// An adapter for merging the outputs of multiple streams.
///
/// The merged stream produces items from the underlying streams as they become available.
/// Errors, however, are not merged: you get at most one error at a time.
#[derive(Debug)]
#[pin_project(project = StreamTupleZipProjection)]
pub struct StreamTupleZip<Streams: StreamTuple> {
    #[pin]
    streams: Streams::FusedStreams,
    values: Streams::ValueOptions,
}

impl<Streams: StreamTuple> StreamTupleZip<Streams> {
    pub fn new(streams: Streams) -> Self {
        Self {
            streams: StreamTuple::fused_streams(streams),
            values: Streams::empty_values(),
        }
    }
}

pub trait StreamTuple {
    type FusedStreams;
    type ValueOptions;
    type Values;

    fn fused_streams(self) -> Self::FusedStreams;

    fn empty_values() -> Self::ValueOptions;

    fn all_streams_terminated(streams: &Self::FusedStreams) -> bool;

    fn any_stream_terminated(streams: &Self::FusedStreams) -> bool;

    fn check_streams(
        streams: Pin<&mut Self::FusedStreams>,
        values: &mut Self::ValueOptions,
        cx: &mut Context<'_>,
    );

    fn all_values_some(values: &Self::ValueOptions) -> bool;

    fn take_all_values_unchecked(values: &mut Self::ValueOptions) -> Self::Values;

    fn size_hints<F>(streams: &Self::FusedStreams, values: &Self::ValueOptions, f: F)
    where
        F: FnMut(usize, (usize, Option<usize>));
}

impl<Streams: StreamTuple> Stream for StreamTupleZip<Streams> {
    type Item = Streams::Values;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        Streams::check_streams(this.streams.as_mut(), this.values, cx);

        if Streams::all_values_some(this.values) {
            let tuple = Streams::take_all_values_unchecked(this.values);

            Poll::Ready(Some(tuple))
        } else if Streams::any_stream_terminated(&this.streams) {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let mut streams_lower = usize::MAX;
        let mut streams_upper = None::<usize>;

        Streams::size_hints(&self.streams, &self.values, |value_len, (lower, upper)| {
            streams_lower = streams_lower.min(lower.saturating_add(value_len));

            streams_upper = match (streams_upper, upper) {
                (Some(x), Some(y)) => Some(x.min(y.saturating_add(value_len))),
                (Some(x), None) => Some(x),
                (None, Some(y)) => y.checked_add(value_len),
                (None, None) => None,
            };
        });

        (streams_lower, streams_upper)
    }
}

impl<Streams: StreamTuple> FusedStream for StreamTupleZip<Streams> {
    fn is_terminated(&self) -> bool {
        Streams::all_streams_terminated(&self.streams)
    }
}

macro_rules! impl_stream_tuple_zip {
    ($N:tt => $($I:tt),+ ) => {
        paste::paste! {
            impl< $([<S $I>]: Stream + Unpin),* > StreamTuple for ( $([<S $I>]),* )
            {
                type FusedStreams = ( $(Fuse< [<S $I>] >),* );
                type ValueOptions = ( $(Option< [<S $I>] :: Item >),* );
                type Values = ( $( [<S $I>] :: Item),* );

                #[inline]
                fn fused_streams(self) -> Self::FusedStreams {
                    ( $( self.$I.fuse() ),* )
                }

                #[inline]
                fn empty_values() -> Self::ValueOptions {
                    ( $( Option::< [<S $I>] :: Item >::None ),* )
                }

                #[inline]
                fn all_streams_terminated(streams: &Self::FusedStreams) -> bool {
                    $( streams.$I.is_terminated() )&&*
                }

                #[inline]
                fn any_stream_terminated(streams: &Self::FusedStreams) -> bool {
                    $( streams.$I.is_terminated() )||*
                }

                #[inline]
                fn check_streams(
                    mut streams: Pin<&mut Self::FusedStreams>,
                    values: &mut Self::ValueOptions,
                    cx: &mut Context<'_>,
                ) {
                    $(
                        if values.$I.is_none() {
                            match streams.as_mut().$I.poll_next_unpin(cx) {
                                Poll::Ready(Some(item)) => values.$I = Some(item),
                                Poll::Ready(None) | Poll::Pending => {}
                            }
                        }
                    )*
                }

                #[inline]
                fn all_values_some(values: &Self::ValueOptions) -> bool {
                    $( values.$I.is_some() )&&*
                }

                #[inline]
                fn take_all_values_unchecked(values: &mut Self::ValueOptions) -> Self::Values {
                    ( $( values.$I.take().unwrap() ),* )
                }

                #[inline]
                fn size_hints<F>(streams: &Self::FusedStreams, values: &Self::ValueOptions, mut f: F)
                where
                    F: FnMut(usize, (usize, Option<usize>)),
                {
                    $(
                        f(
                            if values.$I.is_some() { 1 } else { 0 },
                            streams.$I.size_hint(),
                        );
                    )*
                }
            }
        }
    };
}

// TODO: generate list of variables (1, 2, 3, 4…) from macro call
impl_stream_tuple_zip!(2 => 0, 1);
impl_stream_tuple_zip!(3 => 0, 1, 2);
impl_stream_tuple_zip!(4 => 0, 1, 2, 3);
impl_stream_tuple_zip!(5 => 0, 1, 2, 3, 4);
impl_stream_tuple_zip!(6 => 0, 1, 2, 3, 4, 5);
impl_stream_tuple_zip!(7 => 0, 1, 2, 3, 4, 5, 6);
impl_stream_tuple_zip!(8 => 0, 1, 2, 3, 4, 5, 6, 7);

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn compare_with_zip() {
        let st1 = futures::stream::iter(vec![1, 2, 3]);
        let st2 = futures::stream::iter(vec![1, 2, 3, 4]);

        // init
        let st_tuple_zip = StreamTupleZip::new((st1.clone(), st2.clone()));
        let st_zip = st1.zip(st2);

        // size hints
        assert_eq!(st_tuple_zip.size_hint(), st_zip.size_hint());

        // output
        let o1: Vec<(i32, i32)> = st_tuple_zip.collect().await;
        let o2: Vec<(i32, i32)> = st_zip.collect().await;
        assert_eq!(o1, o2);
    }
}
