//! Rate limiting of a data source, as a GDAL worker observes it while it reads.
//!
//! Object stores answer too many requests with HTTP 429, S3 also with 503 (`SlowDown`). GDAL
//! retries such requests itself (`GDAL_HTTP_MAX_RETRY`) and the worker retries failed reads on
//! top (`WorkerRetryConfig`), so a read can wait for minutes. The worker reports each retry as a
//! [`ThrottleEvent`] before the result of the read, so that callers can tell users why a tile
//! takes long. Reads are never aborted for it.

use serde::{Deserialize, Serialize};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

/// A request of a read was rate limited by its data source and is retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThrottleEvent {
    /// The HTTP status the data source answered with, 429 or 503.
    pub http_status: u16,
    /// The pause before the next attempt.
    pub retry_in: Duration,
}

/// The HTTP statuses that mean that a data source rate limits requests.
pub fn is_throttling_status(status: u16) -> bool {
    matches!(status, 429 | 503)
}

/// GDAL retries an HTTP request, for any status it retries (by default 429, 500, 502, 503 and
/// 504), see [`parse_gdal_http_retry_warning`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpRetry {
    /// The HTTP status of the failed attempt.
    pub http_status: u16,
    /// The pause before the next attempt.
    pub retry_in: Duration,
}

impl From<HttpRetry> for ThrottleEvent {
    fn from(retry: HttpRetry) -> Self {
        Self {
            http_status: retry.http_status,
            retry_in: retry.retry_in,
        }
    }
}

/// Parses the warning GDAL emits before it retries an HTTP request, in one of its two forms
/// (GDAL 3.12, `cpl_http.cpp` and `cpl_vsil_curl.cpp`):
///
/// - `HTTP error code: 429 - <url>. Retrying again in 0.5 secs`
/// - `HTTP error code for <url> range <range>: 429. Retrying again in 0.5 secs`
///
/// Returns `None` for other messages. Whether a retry means rate limiting is up to the caller,
/// see [`is_throttling_status`].
pub fn parse_gdal_http_retry_warning(msg: &str) -> Option<HttpRetry> {
    let (head, delay) = msg.rsplit_once(". Retrying again in ")?;
    let retry_in =
        Duration::try_from_secs_f64(delay.strip_suffix(" secs")?.trim().parse().ok()?).ok()?;

    let status = if let Some(rest) = head.strip_prefix("HTTP error code: ") {
        rest.split_once(" - ").map_or(rest, |(status, _url)| status)
    } else {
        let rest = head.strip_prefix("HTTP error code for ")?;
        rest.rsplit_once(": ")?.1
    };
    let http_status = status.trim().parse().ok()?;

    Some(HttpRetry {
        http_status,
        retry_in,
    })
}

/// The throttling HTTP status of the message of a failed GDAL call, if it says that the data
/// source rate limits requests.
pub fn throttling_status_of_error_message(msg: &str) -> Option<u16> {
    const TOO_MANY_REQUESTS: [&str; 5] = [
        "HTTP response code: 429",
        "HTTP error code: 429",
        "HTTP error code : 429",
        "Too Many Requests",
        "TooManyRequests",
    ];
    // S3 answers `SlowDown` with a 503
    const UNAVAILABLE: [&str; 4] = [
        "HTTP response code: 503",
        "HTTP error code: 503",
        "HTTP error code : 503",
        "SlowDown",
    ];

    if TOO_MANY_REQUESTS
        .iter()
        .any(|pattern| msg.contains(pattern))
    {
        Some(429)
    } else if UNAVAILABLE.iter().any(|pattern| msg.contains(pattern)) {
        Some(503)
    } else {
        None
    }
}

type Reporter = Box<dyn Fn(ThrottleEvent) + Send + Sync>;

/// Where a worker process sends its [`ThrottleEvent`]s, set once when it starts. GDAL emits
/// warnings from its decoder threads too, so the reporter is `Sync` and called without a lock.
static REPORTER: OnceLock<Reporter> = OnceLock::new();

/// Sets where [`report_throttled`] sends events in this process. Only the first call has an
/// effect.
pub fn set_throttle_reporter(reporter: impl Fn(ThrottleEvent) + Send + Sync + 'static) {
    let _ = REPORTER.set(Box::new(reporter));
}

/// Reports that a request of the current read is rate limited. Does nothing without a reporter,
/// e.g. in tests that read in the server process.
pub fn report_throttled(event: ThrottleEvent) {
    if let Some(reporter) = REPORTER.get() {
        reporter(event);
    }
}

/// Hears about the reads of a query that their data source rate limits, e.g. to tell the user
/// why tiles take long. Set per query, see `GdalProcessPoolAccess::read_status_listener`.
///
/// For each rate limited read, `read_throttled` is called for every [`ThrottleEvent`] (`first`
/// for the first one), then `throttled_read_ended` exactly once: when the read is done, failed,
/// or the caller stopped waiting for it.
pub trait ReadStatusListener: Send + Sync {
    fn read_throttled(&self, event: ThrottleEvent, first: bool);
    fn throttled_read_ended(&self);
}

/// A [`ReadStatusListener`] for contexts without a user to tell, e.g. while operators are
/// initialized or in tests.
pub struct NoopReadStatusListener;

impl ReadStatusListener for NoopReadStatusListener {
    fn read_throttled(&self, _event: ThrottleEvent, _first: bool) {}

    fn throttled_read_ended(&self) {}
}

/// The shared [`NoopReadStatusListener`], so that contexts do not allocate one per query.
pub fn noop_read_status_listener() -> Arc<dyn ReadStatusListener> {
    static NOOP: LazyLock<Arc<dyn ReadStatusListener>> =
        LazyLock::new(|| Arc::new(NoopReadStatusListener));
    NOOP.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_both_forms_of_gdal_http_retry_warnings() {
        assert_eq!(
            parse_gdal_http_retry_warning(
                "HTTP error code: 429 - https://bucket.s3.example.com/a.tif. Retrying again in 0.5 secs"
            ),
            Some(HttpRetry {
                http_status: 429,
                retry_in: Duration::from_millis(500),
            })
        );
        assert_eq!(
            parse_gdal_http_retry_warning(
                "HTTP error code for https://host/a.tif range 0-16383: 503. Retrying again in 2.3 secs"
            ),
            Some(HttpRetry {
                http_status: 503,
                retry_in: Duration::from_secs_f64(2.3),
            })
        );
    }

    #[test]
    fn it_parses_http_retries_of_any_status() {
        assert_eq!(
            parse_gdal_http_retry_warning(
                "HTTP error code: 502 - https://host/a.tif. Retrying again in 0.5 secs"
            ),
            Some(HttpRetry {
                http_status: 502,
                retry_in: Duration::from_millis(500),
            })
        );
    }

    #[test]
    fn it_ignores_other_gdal_warnings() {
        assert_eq!(
            parse_gdal_http_retry_warning("HTTP error code: 429 - https://host/a.tif"),
            None
        );
        assert_eq!(
            parse_gdal_http_retry_warning("TIFFReadDirectory: Unknown field with tag 42112"),
            None
        );
    }

    #[test]
    fn it_counts_only_429_and_503_as_throttling() {
        assert!(is_throttling_status(429));
        assert!(is_throttling_status(503));
        assert!(!is_throttling_status(500));
        assert!(!is_throttling_status(502));
        assert!(!is_throttling_status(504));
    }

    #[test]
    fn it_detects_rate_limits_in_error_messages() {
        assert_eq!(
            throttling_status_of_error_message("HTTP response code: 429"),
            Some(429)
        );
        assert_eq!(
            throttling_status_of_error_message("SlowDown: Please reduce your request rate."),
            Some(503)
        );
        assert_eq!(
            throttling_status_of_error_message("HTTP response code: 503"),
            Some(503)
        );
        assert_eq!(
            throttling_status_of_error_message("/vsis3/bucket/429.tif: No such file or directory"),
            None
        );
    }
}
