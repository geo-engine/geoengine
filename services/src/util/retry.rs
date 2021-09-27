use std::time::Duration;

use futures::Future;

/// A method wrapper for calling a method that may fail spuriously until it succeeds.
/// The method is called at most `max_retries` times.
/// If it still fails after `max_retries` times, the error is returned.
///
/// Uses exponential backoff by taking the `initial_delay_ms` and multiplying each time an `exponential_backoff_factor` on it.
///
/// # Panics
/// Panics if `max_retries` is 0.
///
pub async fn retry<F, T, E, Fut>(
    max_retries: usize,
    initial_delay_ms: u64,
    exponential_backoff_factor: f64,
    mut f: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    T: std::fmt::Debug, // TODO: remove
    E: std::fmt::Debug, // TODO: remove
{
    assert!(max_retries > 0, "Must use at least one retry");

    let mut result = (f)().await;

    let mut retries = max_retries - 1; // first one used previously
    let mut sleep_delay = initial_delay_ms as f64;

    while result.is_err() && retries > 0 {
        tokio::time::sleep(Duration::from_millis(sleep_delay as u64)).await;

        result = (f)().await;

        retries -= 1;
        sleep_delay *= exponential_backoff_factor;
    }

    result
}
