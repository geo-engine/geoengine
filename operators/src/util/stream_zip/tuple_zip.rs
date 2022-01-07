use std::pin::Pin;

use futures::stream::{Fuse, FusedStream};
use futures::task::{Context, Poll};
use futures::{Stream, StreamExt};
use pin_project::pin_project;

macro_rules! impl_stream_tuple_zip {
    ($N:tt => $($I:tt),+ ) => {
        paste::paste! {
            /// An adapter for merging the outputs of multiple streams.
            ///
            /// The merged stream produces items from the underlying streams as they become available.
            /// Errors, however, are not merged: you get at most one error at a time.
            #[derive(Debug)]
            #[pin_project(project = [<StreamTuple $N ZipProjection>])]
            pub struct [<StreamTuple $N Zip>]<$([<I $I>]: Stream),*> {
                #[pin]
                streams: ($(Fuse< [<I $I>] >),*),
                values: ($(Option< [<I $I>] :: Item >),*),
            }
        }

        paste::paste! {
            impl<$([<I $I>]: Stream),*> [<StreamTuple $N Zip>]<$([<I $I>]),*> {
                pub fn new(streams: ($([<I $I>]),*)) -> Self {
                    Self {
                        streams: ($(streams.$I.fuse()),*),
                        values: ($(Option::< [<I $I>] :: Item >::None),*),
                    }
                }
            }
        }

        paste::paste! {
            impl<$([<I $I>]: Stream + Unpin),*> Stream for [<StreamTuple $N Zip>]<$([<I $I>]),*>
            {
                type Item = ($( [<I $I>] :: Item ),*);

                fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
                    let mut this = self.project();

                    $(
                        if this.values.$I.is_none() {
                            match Pin::new(&mut this.streams.as_mut().$I).poll_next(cx) {
                                Poll::Ready(Some(item)) => this.values.$I = Some(item),
                                Poll::Ready(None) | Poll::Pending => {}
                            }
                        }
                    )*

                    if $(this.values.$I.is_some())&&* {
                        let tuple = ($(this.values.$I.take().unwrap()),*);

                        Poll::Ready(Some(tuple))
                    } else if this.streams.0.is_terminated() || this.streams.1.is_terminated() {
                        Poll::Ready(None)
                    } else {
                        Poll::Pending
                    }
                }

                fn size_hint(&self) -> (usize, Option<usize>) {
                    let mut streams_lower = usize::MAX;
                    let mut streams_upper = None::<usize>;

                    $({
                        let stream = &self.streams.$I;
                        let value = &self.values.$I;

                        let value_len = if value.is_some() { 1 } else { 0 };

                        let (lower, upper) = stream.size_hint();

                        streams_lower = streams_lower.min(lower.saturating_add(value_len));

                        streams_upper = match (streams_upper, upper) {
                            (Some(x), Some(y)) => Some(x.min(y.saturating_add(value_len))),
                            (Some(x), None) => Some(x),
                            (None, Some(y)) => y.checked_add(value_len),
                            (None, None) => None,
                        };
                    })*

                    (streams_lower, streams_upper)
                }
            }
        }

        paste::paste! {

            impl<$([<I $I>]: Stream + Unpin),*> FusedStream for [<StreamTuple $N Zip>]<$([<I $I>]),*>
            {
                fn is_terminated(&self) -> bool {
                    $(self.streams.$I.is_terminated())&&*
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
        let st_tuple_zip = StreamTuple2Zip::new((st1.clone(), st2.clone()));
        let st_zip = st1.zip(st2);

        // size hints
        assert_eq!(st_tuple_zip.size_hint(), st_zip.size_hint());

        // output
        let o1: Vec<(i32, i32)> = st_tuple_zip.collect().await;
        let o2: Vec<(i32, i32)> = st_zip.collect().await;
        assert_eq!(o1, o2);
    }
}
