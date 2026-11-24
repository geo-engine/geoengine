use std::time::{Duration, Instant};

use futures::Future;

/// A method wrapper for calling a method that may fail spuriously until it succeeds.
/// The method is called at most `max_retries + 1` times.
/// If it still fails after `max_retries` times, the error is returned.
///
/// Uses exponential backoff by taking the `initial_delay_ms` and multiplying each time an `exponential_backoff_factor` on it.
pub async fn retry<F, T, E, Fut>(
    mut max_retries: usize,
    initial_delay_ms: u64,
    exponential_backoff_factor: f64,
    max_delay_ms: Option<u64>,
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

        if let Some(max_delay_ms) = max_delay_ms {
            sleep_delay = sleep_delay.min(max_delay_ms as f64);
        }
    }

    result
}

/// The limits of [`retry_sync`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SyncRetryLimits {
    /// The number of retries after the first attempt.
    pub max_retries: usize,
    /// The pause before the first retry.
    pub initial_delay: Duration,
    /// Each pause is the previous one times this factor.
    pub backoff_factor: f64,
    /// The longest pause between two attempts.
    pub max_delay: Option<Duration>,
    /// No retry starts if its pause would end after this time, counted from the first attempt.
    pub max_duration: Option<Duration>,
}

/// What [`retry_sync`] does after a failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// The next attempt starts after `delay`.
    RetryAfter { attempt: usize, delay: Duration },
    /// The error is returned.
    GiveUp {
        attempt: usize,
        reason: GiveUpReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GiveUpReason {
    /// The error does not go away by retrying.
    Permanent,
    /// All `max_retries` retries failed.
    RetriesExhausted,
    /// The next pause would end after `max_duration`.
    DurationExhausted,
}

/// Calls `f` until it succeeds, it fails with an error for which `is_permanent` is true, or the
/// `limits` are reached. Pauses between attempts grow exponentially and block the thread.
///
/// `on_failure` is called after every failed attempt with what happens next, e.g. for logging.
pub fn retry_sync<F, T, E, P, L>(
    limits: &SyncRetryLimits,
    mut f: F,
    is_permanent: P,
    mut on_failure: L,
) -> std::result::Result<T, E>
where
    F: FnMut() -> std::result::Result<T, E>,
    P: Fn(&E) -> bool,
    L: FnMut(&E, RetryDecision),
{
    let start = Instant::now();
    let mut delay = limits.initial_delay;
    let mut attempt = 1;

    loop {
        let error = match f() {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };

        let give_up = if is_permanent(&error) {
            Some(GiveUpReason::Permanent)
        } else if attempt > limits.max_retries {
            Some(GiveUpReason::RetriesExhausted)
        } else if limits
            .max_duration
            .is_some_and(|max_duration| start.elapsed().saturating_add(delay) > max_duration)
        {
            Some(GiveUpReason::DurationExhausted)
        } else {
            None
        };

        if let Some(reason) = give_up {
            on_failure(&error, RetryDecision::GiveUp { attempt, reason });
            return Err(error);
        }

        on_failure(&error, RetryDecision::RetryAfter { attempt, delay });
        std::thread::sleep(delay);

        attempt += 1;
        delay = Duration::try_from_secs_f64(delay.as_secs_f64() * limits.backoff_factor)
            .unwrap_or(Duration::MAX);
        if let Some(max_delay) = limits.max_delay {
            delay = delay.min(max_delay);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;

    use futures::future::{err, ok, poll_fn};

    use super::*;

    #[tokio::test]
    async fn test_immediate_success() {
        let result: Result<(), ()> = retry(3, 0, 1., None, || ok(())).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_retry_success_after_tries() {
        let i = Arc::new(AtomicUsize::new(0));

        let result = retry(3, 0, 1., None, || {
            let i = i.clone();
            poll_fn(move |_ctx| {
                Poll::Ready(match i.fetch_add(1, Ordering::Relaxed) {
                    0..=2 => Err(()),
                    _ => Ok(()),
                })
            })
        })
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_failure() {
        let result: Result<(), ()> = retry(3, 0, 1., None, || err(())).await;

        assert!(result.is_err());
    }

    fn sync_limits(max_retries: usize) -> SyncRetryLimits {
        SyncRetryLimits {
            max_retries,
            initial_delay: Duration::ZERO,
            backoff_factor: 2.,
            max_delay: None,
            max_duration: None,
        }
    }

    #[derive(Debug, PartialEq)]
    enum TestError {
        Transient,
        Permanent,
    }

    #[test]
    fn retry_sync_retries_until_success() {
        let mut calls = 0;
        let mut decisions = Vec::new();

        let result = retry_sync(
            &sync_limits(5),
            || {
                calls += 1;
                if calls < 3 {
                    Err(TestError::Transient)
                } else {
                    Ok(calls)
                }
            },
            |e| *e == TestError::Permanent,
            |_, decision| decisions.push(decision),
        );

        assert_eq!(result, Ok(3));
        assert_eq!(
            decisions,
            vec![
                RetryDecision::RetryAfter {
                    attempt: 1,
                    delay: Duration::ZERO
                },
                RetryDecision::RetryAfter {
                    attempt: 2,
                    delay: Duration::ZERO
                },
            ]
        );
    }

    #[test]
    fn retry_sync_stops_after_max_retries() {
        let mut calls = 0;
        let mut decisions = Vec::new();

        let result: Result<(), _> = retry_sync(
            &sync_limits(2),
            || {
                calls += 1;
                Err(TestError::Transient)
            },
            |e| *e == TestError::Permanent,
            |_, decision| decisions.push(decision),
        );

        assert_eq!(result, Err(TestError::Transient));
        assert_eq!(calls, 3);
        assert_eq!(
            decisions.last(),
            Some(&RetryDecision::GiveUp {
                attempt: 3,
                reason: GiveUpReason::RetriesExhausted
            })
        );
    }

    #[test]
    fn retry_sync_does_not_retry_a_permanent_first_error() {
        let mut calls = 0;
        let mut decisions = Vec::new();

        let result: Result<(), _> = retry_sync(
            &SyncRetryLimits {
                initial_delay: Duration::from_secs(3600),
                ..sync_limits(99)
            },
            || {
                calls += 1;
                Err(TestError::Permanent)
            },
            |e| *e == TestError::Permanent,
            |_, decision| decisions.push(decision),
        );

        assert_eq!(result, Err(TestError::Permanent));
        assert_eq!(calls, 1);
        assert_eq!(
            decisions,
            vec![RetryDecision::GiveUp {
                attempt: 1,
                reason: GiveUpReason::Permanent
            }]
        );
    }

    #[test]
    fn retry_sync_stops_at_a_later_permanent_error() {
        let mut calls = 0;

        let result: Result<(), _> = retry_sync(
            &sync_limits(99),
            || {
                calls += 1;
                if calls < 2 {
                    Err(TestError::Transient)
                } else {
                    Err(TestError::Permanent)
                }
            },
            |e| *e == TestError::Permanent,
            |_, _| {},
        );

        assert_eq!(result, Err(TestError::Permanent));
        assert_eq!(calls, 2);
    }

    #[test]
    fn retry_sync_caps_the_delay() {
        let mut decisions = Vec::new();

        let result: Result<(), _> = retry_sync(
            &SyncRetryLimits {
                max_retries: 4,
                initial_delay: Duration::from_millis(1),
                backoff_factor: 10.,
                max_delay: Some(Duration::from_millis(5)),
                max_duration: None,
            },
            || Err(TestError::Transient),
            |e| *e == TestError::Permanent,
            |_, decision| decisions.push(decision),
        );

        assert!(result.is_err());
        let delays: Vec<_> = decisions
            .iter()
            .filter_map(|decision| match decision {
                RetryDecision::RetryAfter { delay, .. } => Some(*delay),
                RetryDecision::GiveUp { .. } => None,
            })
            .collect();
        assert_eq!(
            delays,
            vec![
                Duration::from_millis(1),
                Duration::from_millis(5),
                Duration::from_millis(5),
                Duration::from_millis(5),
            ]
        );
    }

    #[test]
    fn retry_sync_stops_when_the_next_pause_exceeds_the_duration() {
        let mut calls = 0;
        let mut decisions = Vec::new();
        let start = Instant::now();

        let result: Result<(), _> = retry_sync(
            &SyncRetryLimits {
                max_retries: 99,
                initial_delay: Duration::from_millis(20),
                backoff_factor: 2.,
                max_delay: None,
                max_duration: Some(Duration::from_millis(100)),
            },
            || {
                calls += 1;
                Err(TestError::Transient)
            },
            |e| *e == TestError::Permanent,
            |_, decision| decisions.push(decision),
        );

        assert!(result.is_err());
        // pauses of 20 and 40 ms fit into 100 ms, the next one of 80 ms does not
        assert_eq!(calls, 3);
        assert_eq!(
            decisions.last(),
            Some(&RetryDecision::GiveUp {
                attempt: 3,
                reason: GiveUpReason::DurationExhausted
            })
        );
        assert!(start.elapsed() < Duration::from_millis(100));
    }
}
