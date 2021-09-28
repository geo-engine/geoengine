use std::time::Duration;

use futures::Future;

/// A method wrapper for calling a method that may fail spuriously until it succeeds.
/// The method is called at most `max_retries + 1` times.
/// If it still fails after `max_retries` times, the error is returned.
///
/// Uses exponential backoff by taking the `initial_delay_ms` and multiplying each time an `exponential_backoff_factor` on it.
///
/// # Panics
/// Panics if `max_retries` is 0.
///
pub async fn retry<F, T, E, Fut>(
    mut max_retries: usize,
    initial_delay_ms: u64,
    exponential_backoff_factor: f64,
    mut f: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut result = (f)().await;

    let mut sleep_delay = initial_delay_ms as f64;

    while result.is_err() && max_retries > 0 {
        tokio::time::sleep(Duration::from_millis(sleep_delay as u64)).await;

        result = (f)().await;

        max_retries -= 1;
        sleep_delay *= exponential_backoff_factor;
    }

    result
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::Poll;

    use futures::future::{err, ok, poll_fn};

    use super::*;

    #[tokio::test]
    async fn test_immediate_success() {
        let result: Result<(), ()> = retry(3, 0, 1., async || ok(()).await).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_retry_success_after_tries() {
        let i = Arc::new(AtomicUsize::new(0));

        let result = retry(3, 0, 1., async || {
            let i = i.clone();
            poll_fn(move |_ctx| {
                Poll::Ready(match i.fetch_add(1, Ordering::Relaxed) {
                    0..=2 => Err(()),
                    _ => Ok(()),
                })
            })
            .await
        })
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_failure() {
        let result: Result<(), ()> = retry(3, 0, 1., async || err(()).await).await;

        assert!(result.is_err());
    }
}
