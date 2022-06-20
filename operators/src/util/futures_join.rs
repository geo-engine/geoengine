use futures::{
    future::{maybe_done, FusedFuture, MaybeDone},
    Future,
};
use pin_project::pin_project;
use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

/// An array of futures that run concurrently and are joined together.
#[pin_project(project = ArrayJoinProjection)]
pub struct ArrayJoin<Fut: Future, const N: usize> {
    futures: [MaybeDone<Fut>; N],
}

impl<Fut: Future, const N: usize> fmt::Debug for ArrayJoin<Fut, N>
where
    Fut: Future + fmt::Debug,
    Fut::Output: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArrayJoin")
            .field("futures", &self.futures)
            .finish()
    }
}

impl<Fut: Future, const N: usize> ArrayJoin<Fut, N> {
    pub fn new(futures: [Fut; N]) -> Self {
        Self {
            futures: futures.map(maybe_done),
        }
    }
}

impl<Fut: Future, const N: usize> Future for ArrayJoin<Fut, N> {
    type Output = [Fut::Output; N];

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut all_done = true;
        let this = self.project();

        let futures = this.futures.as_mut();

        for future in futures.iter_mut() {
            let future = unsafe { Pin::new_unchecked(future) };
            all_done &= future.poll(cx).is_ready();
        }

        if all_done {
            let results: Vec<Fut::Output> = futures
                .iter_mut()
                .map(|future| {
                    let future = unsafe { Pin::new_unchecked(future) };
                    future.take_output().unwrap()
                })
                .collect();

            if let Ok(array) = results.try_into() {
                Poll::Ready(array)
            } else {
                // custom `unwrap` because `Result<Fut::Output, _>` is not `Debug`
                unreachable!("ArrayJoin: vec and array size differ")
            }
        } else {
            Poll::Pending
        }
    }
}

impl<Fut: Future, const N: usize> FusedFuture for ArrayJoin<Fut, N> {
    fn is_terminated(&self) -> bool {
        self.futures.iter().all(FusedFuture::is_terminated)
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;

    use super::*;

    #[tokio::test]
    async fn test_array_futures() {
        let futures = [
            async { 1 }.boxed(),
            async { 2 }.boxed(),
            async { 3 }.boxed(),
        ];

        let join = ArrayJoin::new(futures);

        assert_eq!(join.await, [1, 2, 3]);
    }
}
