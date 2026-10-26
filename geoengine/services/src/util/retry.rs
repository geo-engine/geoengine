//! Generic HTTP retry helper with configurable stop/retry conditions.
//!
//! [`retry_http`] retries an async operation with exponential backoff until it
//! succeeds, the configured attempt limit is reached, or the error matches a
//! *terminal* condition. Terminal conditions can be declared by HTTP status
//! code and/or by substring in the error's display message, either as "stop
//! retrying on these" or as "only retry on these" (whitelist).

use std::time::Duration;

use tracing::{error, warn};

/// Default number of attempts before giving up.
pub const DEFAULT_MAX_RETRIES: u32 = 10;
/// Default initial backoff delay in milliseconds (doubles after each attempt).
pub const DEFAULT_INITIAL_DELAY_MS: u64 = 1000;

/// Controls when [`retry_http`] keeps retrying and when it gives up.
///
/// Two complementary mechanisms are available, each applicable to HTTP status
/// codes and to substrings of the error's `Display` message:
///
/// - `stop_on_*`: errors matching these are terminal and returned immediately.
/// - `retry_on_*`: if non-empty, a whitelist — only matching errors are
///   retried, everything else is terminal.
///
/// With no conditions set, every error is retried (plain exponential backoff).
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub initial_delay_ms: u64,
    /// HTTP status codes on which to stop retrying immediately.
    pub stop_on_status: Vec<u16>,
    /// HTTP status codes to retry (whitelist; empty = retry all non-terminal errors).
    pub retry_on_status: Vec<u16>,
    /// Error-message substrings on which to stop retrying immediately.
    pub stop_on_message_contains: Vec<String>,
    /// Error-message substrings to retry (whitelist; empty = retry all non-terminal errors).
    pub retry_on_message_contains: Vec<String>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            initial_delay_ms: DEFAULT_INITIAL_DELAY_MS,
            stop_on_status: Vec::new(),
            retry_on_status: Vec::new(),
            stop_on_message_contains: Vec::new(),
            retry_on_message_contains: Vec::new(),
        }
    }
}

impl RetryPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    #[must_use]
    pub fn initial_delay_ms(mut self, initial_delay_ms: u64) -> Self {
        self.initial_delay_ms = initial_delay_ms;
        self
    }

    /// Abort retrying on the given HTTP status codes (e.g. `[400, 404]`).
    #[must_use]
    pub fn stop_on_status(mut self, codes: &[u16]) -> Self {
        self.stop_on_status.extend_from_slice(codes);
        self
    }

    /// Only retry on the given HTTP status codes; everything else is terminal.
    #[must_use]
    pub fn retry_on_status(mut self, codes: &[u16]) -> Self {
        self.retry_on_status.extend_from_slice(codes);
        self
    }

    /// Abort retrying when the error message contains any of the given substrings.
    #[must_use]
    pub fn stop_on_message(mut self, substrings: &[&str]) -> Self {
        self.stop_on_message_contains
            .extend(substrings.iter().map(ToString::to_string));
        self
    }

    /// Only retry when the error message contains any of the given substrings.
    #[must_use]
    pub fn retry_on_message(mut self, substrings: &[&str]) -> Self {
        self.retry_on_message_contains
            .extend(substrings.iter().map(ToString::to_string));
        self
    }
}

/// Retry an operation with exponential backoff, honouring `policy`.
///
/// `extract_status` maps an error to an optional HTTP status code so the policy
/// can match `stop_on_status`/`retry_on_status`; the error's `Display` output is
/// used for the message-substring conditions.
pub async fn retry_http<F, Fut, T, E>(
    mut operation: F,
    operation_name: &str,
    policy: &RetryPolicy,
    extract_status: impl Fn(&E) -> Option<u16>,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    retry_http_with_action(
        || {
            let attempt = operation();
            async move { (attempt.await, RetryAction::Backoff) }
        },
        operation_name,
        policy,
        extract_status,
    )
    .await
}

/// How to handle a failed attempt, subject to the policy's terminal conditions.
#[derive(Clone, Copy)]
pub(crate) enum RetryAction {
    /// Use the policy's exponential backoff, advancing its failure counter.
    Backoff,
    /// Let the caller's scheduler handle the delay without advancing backoff.
    Scheduled,
    /// Return the failure immediately without retrying.
    Abort,
}

/// Retry with one shared attempt budget and an action chosen by each attempt.
/// Terminal checks, logging and backoff are identical to [`retry_http`]. Attempts
/// whose delay is managed externally still consume the same retry budget.
pub(crate) async fn retry_http_with_action<F, Fut, T, E>(
    mut operation: F,
    operation_name: &str,
    policy: &RetryPolicy,
    extract_status: impl Fn(&E) -> Option<u16>,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = (Result<T, E>, RetryAction)>,
    E: std::fmt::Display,
{
    let max_attempts = policy.max_retries.max(1);
    let mut attempt = 0;
    let mut backoff_failures = 0;
    loop {
        let (result, action) = operation().await;
        match result {
            Ok(result) => return Ok(result),
            Err(err) => {
                if matches!(action, RetryAction::Abort)
                    || is_terminal(&err, policy, &extract_status)
                {
                    return Err(err);
                }

                attempt += 1;
                if attempt >= max_attempts {
                    error!("{operation_name} failed after {max_attempts} attempts: {err}");
                    return Err(err);
                }

                if let RetryAction::Backoff = action {
                    let delay = Duration::from_millis(
                        policy.initial_delay_ms * 2_u64.pow(backoff_failures),
                    );
                    backoff_failures += 1;
                    warn!(
                        "{operation_name} failed (attempt {attempt}/{max_attempts}): {err}. Retrying in {delay:?}..."
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
}

/// Decide whether `err` should abort the retry loop under `policy`.
fn is_terminal<E: std::fmt::Display>(
    err: &E,
    policy: &RetryPolicy,
    extract_status: &impl Fn(&E) -> Option<u16>,
) -> bool {
    let status = extract_status(err);
    let message = err.to_string();

    // Explicit stop conditions always win.
    if let Some(status) = status
        && policy.stop_on_status.contains(&status)
    {
        return true;
    }
    if policy
        .stop_on_message_contains
        .iter()
        .any(|needle| message.contains(needle.as_str()))
    {
        return true;
    }

    // Whitelist conditions: if configured, retry only on matches.
    let status_allows_retry = if policy.retry_on_status.is_empty() {
        true
    } else {
        status.is_some_and(|s| policy.retry_on_status.contains(&s))
    };
    let message_allows_retry = if policy.retry_on_message_contains.is_empty() {
        true
    } else {
        policy
            .retry_on_message_contains
            .iter()
            .any(|needle| message.contains(needle.as_str()))
    };

    !(status_allows_retry && message_allows_retry)
}

#[cfg(test)]
mod tests {
    use super::{RetryAction, RetryPolicy, retry_http, retry_http_with_action};
    use std::fmt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    #[derive(Debug, Clone)]
    struct TestError {
        status: Option<u16>,
        message: String,
    }

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            if let Some(status) = self.status {
                write!(f, "HTTP {status}: {}", self.message)
            } else {
                write!(f, "{}", self.message)
            }
        }
    }

    impl std::error::Error for TestError {}

    #[tokio::test]
    async fn it_returns_aborted_failures_without_retrying() {
        let mut attempts = 0;
        let result = retry_http_with_action(
            || {
                attempts += 1;
                async { (Err::<(), _>("coordinator stopped"), RetryAction::Abort) }
            },
            "aborted request",
            &RetryPolicy::new(),
            |_| None,
        )
        .await;
        assert_eq!(result.unwrap_err(), "coordinator stopped");
        assert_eq!(attempts, 1);
    }

    #[tokio::test]
    async fn it_skips_backoff_without_exceeding_the_attempt_budget() {
        for max_retries in [0, 1, 3] {
            let mut attempts = 0;
            let result = tokio::time::timeout(
                Duration::from_millis(100),
                retry_http_with_action(
                    || {
                        attempts += 1;
                        async { (Err::<(), _>("rate limited"), RetryAction::Scheduled) }
                    },
                    "scheduled retry",
                    &RetryPolicy::new()
                        .max_retries(max_retries)
                        .initial_delay_ms(10_000),
                    |_| None,
                ),
            )
            .await
            .expect("externally scheduled retries must skip policy backoff");
            assert_eq!(result.unwrap_err(), "rate limited");
            assert_eq!(attempts, max_retries.max(1));
        }
    }

    #[tokio::test]
    async fn it_only_advances_backoff_for_attempts_using_policy_delays() {
        let mut starts = Vec::new();
        tokio::time::timeout(
            Duration::from_millis(700),
            retry_http_with_action(
                || {
                    let attempt = starts.len();
                    starts.push(Instant::now());
                    async move {
                        match attempt {
                            0 | 2 => (Err("rate limited"), RetryAction::Scheduled),
                            1 | 3 => (Err("server error"), RetryAction::Backoff),
                            _ => (Ok(()), RetryAction::Scheduled),
                        }
                    }
                },
                "mixed failures",
                &RetryPolicy::new().max_retries(5).initial_delay_ms(100),
                |_| None,
            ),
        )
        .await
        .expect("scheduled retries must not increase exponential backoff")
        .unwrap();
        assert_eq!(starts.len(), 5);
        assert!(starts[2].duration_since(starts[1]) >= Duration::from_millis(100));
        assert!(starts[4].duration_since(starts[3]) >= Duration::from_millis(200));
    }

    #[tokio::test]
    async fn retry_http_retries_until_success() {
        let attempts = Arc::new(AtomicUsize::new(0));

        let result: Result<String, TestError> = retry_http(
            {
                let attempts = attempts.clone();
                move || {
                    let current = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    async move {
                        if current < 3 {
                            Err(TestError {
                                status: None,
                                message: "temporary network issue".to_string(),
                            })
                        } else {
                            Ok("ok".to_string())
                        }
                    }
                }
            },
            "transient fetch",
            &RetryPolicy::new().max_retries(5).initial_delay_ms(1),
            |e| e.status,
        )
        .await;

        assert_eq!(result.unwrap(), "ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_http_stops_on_status_code() {
        let attempts = Arc::new(AtomicUsize::new(0));

        let result: Result<String, TestError> = retry_http(
            {
                let attempts = attempts.clone();
                move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async {
                        Err(TestError {
                            status: Some(500),
                            message: "server error".to_string(),
                        })
                    }
                }
            },
            "server fetch",
            &RetryPolicy::new()
                .max_retries(5)
                .initial_delay_ms(1)
                .stop_on_status(&[500]),
            |e| e.status,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_http_stops_on_message_contains() {
        let attempts = Arc::new(AtomicUsize::new(0));

        let result: Result<String, TestError> = retry_http(
            {
                let attempts = attempts.clone();
                move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async {
                        Err(TestError {
                            status: Some(400),
                            message: "bad request".to_string(),
                        })
                    }
                }
            },
            "message fetch",
            &RetryPolicy::new()
                .max_retries(5)
                .initial_delay_ms(1)
                .stop_on_message(&["bad request"]),
            |e| e.status,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
