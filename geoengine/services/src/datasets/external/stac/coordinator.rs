//! Coordinates outgoing STAC HTTP requests to respect shared server rate limits.
//! Providers with the same configured STAC origin and credentials share a FIFO
//! queue. Each HTTP attempt waits for its turn and reserves capacity in every
//! known rate-limit window before dispatch. Queue and quota updates briefly lock
//! scheduler state; those locks are released before HTTP calls or retry backoff.
//!
//! [`Identity`] selects a process-wide [`StacRequestCoordinator`], shared across
//! providers and collections with the same configuration. Its [`State`] holds
//! the FIFO queue, request history, learned rate-limit windows and server feedback.
//! Admission follows queue order, while admitted HTTP attempts may overlap.
//!
//! One [`StacRequestCoordinator::execute`] call may make several HTTP attempts.
//! Each attempt gets a [`RequestGuard`] that tracks its queue membership and
//! completion. Response headers become [`ResponseFeedback`], which is applied
//! when the guard finishes. Failures and cancellation keep the recorded
//! request start charged against the rolling windows.
//!
//! Retries for transient failures wait for exponential backoff outside the queue,
//! then join its tail again. A 429 sets a shared cooldown; retries also back off
//! unless the server supplies quota windows or usable retry timing. Admission
//! waits for the cooldown and all quota constraints.
use super::auth::StacAuthentication;
use crate::util::retry::{RetryPolicy, is_terminal};
use chrono::{DateTime, Utc};
use reqwest::header::{HeaderMap, RETRY_AFTER};
use serde::de::DeserializeOwned;
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::Notify;
use url::Url;

/// Default cooldown/reset delay and duration of the initial ten-request window.
const FALLBACK_DELAY: Duration = Duration::from_secs(1);
/// Keep recent starts and idle coordinators long enough to learn further policies
/// and preserve quota accounting when providers are reconstructed.
const MIN_RETENTION: Duration = Duration::from_mins(5);

/// Registry key identifying providers that share one quota bucket.
///
/// The origin is the configured STAC scheme, host and effective port. Configured
/// OAuth credentials distinguish authenticated buckets; absent credentials select
/// the anonymous bucket. Provider ids, collection paths and renewed access tokens
/// do not affect sharing. The custom `Debug` implementation omits credential fields.
#[derive(Clone, PartialEq, Eq, Hash)]
struct Identity {
    scheme: String,
    host: String,
    port: u16,
    token_endpoint: Option<String>,
    client_id: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StacCoordinatorIdentity")
            .field("scheme", &self.scheme)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("authenticated", &self.password.is_some())
            .finish_non_exhaustive()
    }
}

/// Remaining server capacity and the monotonic deadline at which it expires.
///
/// This supplements local request history with server knowledge, including quota
/// consumed by other clients. It can belong to a known [`Window`] or apply to the
/// whole coordinator when the headers do not identify a window unambiguously.
/// Merging observations only reduces remaining capacity until reset, so responses
/// arriving out of order cannot replenish the budget.
#[derive(Debug)]
struct RemoteQuota {
    /// Server-reported capacity, conservatively reduced for local reservations.
    remaining: usize,
    /// Absolute deadline derived from the server's relative reset duration.
    reset: Instant,
}

impl RemoteQuota {
    /// Deduct other unfinished attempts that the server may not have counted yet,
    /// then retain the smaller budget and later reset from both observations.
    fn merge(current: &mut Option<Self>, mut observed: Self, outstanding: usize) {
        observed.remaining = observed.remaining.saturating_sub(outstanding);
        if let Some(current) = current {
            current.remaining = current.remaining.min(observed.remaining);
            current.reset = current.reset.max(observed.reset);
        } else {
            *current = Some(observed);
        }
    }

    fn blocked_until(&self) -> Option<Instant> {
        (self.remaining == 0).then_some(self.reset)
    }
}

/// One rate-limit policy: at most `limit` starts within a rolling `duration`.
///
/// Admission must satisfy both this local limit and any associated server quota.
/// Policies are identified by duration and all use [`State::starts`] as their
/// history. The same type represents policies parsed from headers and those
/// retained by the scheduler.
#[derive(Debug)]
struct Window {
    /// Maximum number of admissions within the rolling interval.
    limit: usize,
    /// Length of the rolling interval used to count recent request starts.
    duration: Duration,
    /// Additional remaining/reset constraint reported for this policy, if known.
    remote: Option<RemoteQuota>,
}

impl Window {
    fn new(limit: usize, duration: Duration) -> Self {
        Self {
            limit,
            duration,
            remote: None,
        }
    }

    /// Return the later deadline imposed by local starts and an exhausted server
    /// quota. `None` means this window permits another request.
    fn blocked_until(&self, starts: &VecDeque<Instant>, now: Instant) -> Option<Instant> {
        // The limit-th most recent start determines when capacity returns. This
        // is O(1), even with a long shared history or a newly lowered limit.
        starts
            .len()
            .checked_sub(self.limit)
            .and_then(|index| starts.get(index))
            .and_then(|at| at.checked_add(self.duration))
            .filter(|deadline| *deadline > now)
            .into_iter()
            .chain(self.remote.as_ref().and_then(RemoteQuota::blocked_until))
            .max()
    }
}

/// Result of one admission check under the scheduler mutex.
///
/// This tells the async caller what to await after releasing the mutex. Waits
/// always lead to another check because notifications and deadlines can race
/// with cancellations or new server feedback.
#[derive(Debug)]
enum Admission {
    /// The attempt left the queue and reserved capacity in the current state.
    Admitted,
    /// Another attempt is ahead in the FIFO queue; wait for notification.
    WaitForChange,
    /// The attempt is at the front but a quota or cooldown blocks it. Wait for
    /// this deadline or a notification that the scheduling state changed.
    WaitUntil(Instant),
}

/// Mutable scheduling data for one [`Identity`], protected by a mutex held briefly
/// for each update.
///
/// Queue changes, admission reservations and response feedback are applied
/// atomically here. Async waiting, HTTP dispatch and JSON decoding happen outside
/// this state. The history of request starts enforces rates independently of how many
/// attempts are currently waiting for response headers.
#[derive(Debug)]
struct State {
    /// One notification handle per queued attempt, also used as its unique identity.
    queue: VecDeque<Arc<Notify>>,
    /// Admitted attempts still waiting for response headers or a transport error.
    /// Used to adjust server feedback; this is not a concurrency limit.
    in_flight: usize,
    /// Learned policies, deduplicated by duration. Empty selects ten starts/second.
    windows: Vec<Window>,
    /// Chronological admission timestamps shared by every window. Completion,
    /// failure and cancellation do not refund these reservations.
    starts: VecDeque<Instant>,
    /// Server remaining/reset feedback without an unambiguous window association.
    unscoped_quota: Option<RemoteQuota>,
    /// Shared admission barrier learned from 429 responses.
    cooldown_until: Option<Instant>,
    /// Last admission or release of an admitted attempt, used for idle retention.
    last_activity: Instant,
}

impl State {
    /// Wake only the queue head, which is the only waiter eligible for admission.
    fn wake_front(&self) {
        if let Some(waiter) = self.queue.front() {
            waiter.notify_one();
        }
    }

    /// Preserve starts for the longest learned policy, with a minimum history
    /// for policies that may first appear in later responses.
    fn history_duration(&self) -> Duration {
        self.windows
            .iter()
            .map(|window| window.duration)
            .max()
            .unwrap_or_default()
            .max(MIN_RETENTION)
    }

    /// Deadline after which an unused coordinator's history, quotas and cooldown
    /// no longer require its registry entry to be retained.
    fn retain_until(&self) -> Instant {
        self.last_activity
            .checked_add(self.history_duration())
            .into_iter()
            .chain(self.cooldown_until)
            .chain(self.unscoped_quota.as_ref().map(|quota| quota.reset))
            .chain(
                self.windows
                    .iter()
                    .filter_map(|window| window.remote.as_ref().map(|quota| quota.reset)),
            )
            .max()
            .unwrap_or(self.last_activity)
    }

    /// Admit the queue head only when every known window and server constraint
    /// permits it. Record the start and decrement remaining quotas before dispatch.
    fn admit(&mut self, waiter: &Arc<Notify>, now: Instant) -> Admission {
        if !self
            .queue
            .front()
            .is_some_and(|front| Arc::ptr_eq(front, waiter))
        {
            return Admission::WaitForChange;
        }
        self.prune(now);
        // Before learning policies, use the fallback immediately. There is no
        // special bootstrap request or wait for its headers.
        let local = if self.windows.is_empty() {
            Window::new(10, FALLBACK_DELAY).blocked_until(&self.starts, now)
        } else {
            self.windows
                .iter()
                .filter_map(|window| window.blocked_until(&self.starts, now))
                .max()
        };
        let deadline = local
            .into_iter()
            .chain(
                self.unscoped_quota
                    .as_ref()
                    .and_then(RemoteQuota::blocked_until),
            )
            .chain(self.cooldown_until)
            .max();
        if let Some(deadline) = deadline {
            return Admission::WaitUntil(deadline);
        }
        self.queue.pop_front();
        self.in_flight += 1;
        self.starts.push_back(now);
        self.last_activity = now;
        for quota in self
            .windows
            .iter_mut()
            .filter_map(|window| window.remote.as_mut())
            .chain(self.unscoped_quota.as_mut())
        {
            quota.remaining = quota.remaining.saturating_sub(1);
        }
        self.wake_front();
        Admission::Admitted
    }

    /// Expire old starts, server quotas and cooldowns while keeping learned policies.
    fn prune(&mut self, now: Instant) {
        let history_duration = self.history_duration();
        while self
            .starts
            .front()
            .is_some_and(|at| now.duration_since(*at) >= history_duration)
        {
            self.starts.pop_front();
        }
        for quota in self
            .windows
            .iter_mut()
            .map(|window| &mut window.remote)
            .chain(std::iter::once(&mut self.unscoped_quota))
        {
            if quota.as_ref().is_some_and(|quota| quota.reset <= now) {
                *quota = None;
            }
        }
        if self.cooldown_until.is_some_and(|until| until <= now) {
            self.cooldown_until = None;
        }
    }

    /// Merge response feedback while preserving policies absent from this response.
    /// The responding attempt must already be released so `in_flight` counts only
    /// the other attempts to reserve against the reported remaining capacity.
    fn observe(&mut self, feedback: ResponseFeedback, now: Instant) {
        self.prune(now);
        for mut observed in feedback.windows {
            let remote = observed.remote.take();
            let window = if let Some(index) = self
                .windows
                .iter()
                .position(|window| window.duration == observed.duration)
            {
                self.windows[index].limit = observed.limit;
                &mut self.windows[index]
            } else {
                self.windows.push(observed);
                self.windows
                    .last_mut()
                    .expect("the observed window was just inserted")
            };
            if let Some(remote) = remote {
                RemoteQuota::merge(&mut window.remote, remote, self.in_flight);
            }
        }
        if let Some(quota) = feedback.unscoped_quota {
            RemoteQuota::merge(&mut self.unscoped_quota, quota, self.in_flight);
        }
        if let Some(deadline) = feedback.cooldown_until {
            self.cooldown_until = Some(
                self.cooldown_until
                    .map_or(deadline, |current| current.max(deadline)),
            );
        }
    }

    /// Remove a queued attempt or decrement the count of unfinished HTTP attempts.
    /// An admitted attempt's timestamp remains charged against local rate limits.
    fn release(&mut self, phase: &RequestPhase) {
        match phase {
            RequestPhase::Queued(waiter) => {
                if let Some(index) = self
                    .queue
                    .iter()
                    .position(|entry| Arc::ptr_eq(entry, waiter))
                {
                    self.queue.remove(index);
                }
            }
            RequestPhase::InFlight => {
                self.in_flight = self.in_flight.saturating_sub(1);
                self.last_activity = Instant::now();
            }
            RequestPhase::Finished => return,
        }
        self.wake_front();
    }
}

/// Shared admission and retry scheduler for one configured origin and auth identity.
///
/// The registry supplies shared ownership across providers, collections and client
/// clones. Each HTTP attempt briefly locks `state` to reserve capacity, then runs
/// independently until its headers or transport error can update the scheduler.
#[derive(Debug)]
pub(super) struct StacRequestCoordinator {
    state: Mutex<State>,
}

/// Process-wide coordinator ownership. Lookups reclaim unused entries only after
/// their request history and server deadlines expire, preserving quotas across
/// provider reconstruction. When both locks are needed, lock registry before state.
static REGISTRY: OnceLock<Mutex<HashMap<Identity, Arc<StacRequestCoordinator>>>> = OnceLock::new();

impl StacRequestCoordinator {
    /// Find or create the coordinator for this configured origin and authentication.
    /// Provider ids, collection ids and refreshed access tokens do not split quotas.
    /// This lookup also reclaims expired registry entries that have no other owners.
    pub(super) fn shared(
        origin: &Url,
        authentication: Option<&super::StacProviderAuthentication>,
    ) -> Arc<Self> {
        let identity = Identity {
            scheme: origin.scheme().to_ascii_lowercase(),
            host: origin.host_str().unwrap_or_default().to_ascii_lowercase(),
            port: origin.port_or_known_default().unwrap_or(0),
            token_endpoint: authentication.map(|a| normalize_endpoint(&a.endpoint)),
            client_id: authentication.map(|a| a.client_id.clone()),
            username: authentication.map(|a| a.username.clone()),
            password: authentication.map(|a| a.password.clone()),
        };
        let mut registry = REGISTRY
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        // Only lookup locks both structures, always registry -> state. Dispatch
        // and cancellation never acquire the registry lock.
        registry.retain(|_, coordinator| {
            Arc::strong_count(coordinator) > 1 || coordinator.lock_state().retain_until() > now
        });
        registry
            .entry(identity)
            .or_insert_with(|| {
                Arc::new(Self {
                    state: Mutex::new(State {
                        queue: VecDeque::new(),
                        in_flight: 0,
                        windows: Vec::new(),
                        starts: VecDeque::new(),
                        unscoped_quota: None,
                        cooldown_until: None,
                        last_activity: now,
                    }),
                })
            })
            .clone()
    }

    fn lock_state(&self) -> MutexGuard<'_, State> {
        // Cancellation cleanup must not panic again while unwinding a panic.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Executes and decodes a STAC request, owning admission, header feedback and
    /// retries. Tokens are read after admission, and each retry enters the queue.
    /// The policy's attempt limit includes rate-limit, HTTP, transport and decode failures.
    ///
    /// Transient retries back off before joining the queue again. A 429 retry
    /// also backs off unless a learned quota window or usable server retry timing
    /// lets admission control the retry delay.
    /// No request guard or scheduler mutex is held during exponential backoff or
    /// JSON decoding.
    pub(super) async fn execute<T: DeserializeOwned>(
        self: &Arc<Self>,
        client: &reqwest::Client,
        request: reqwest::Request,
        authentication: Option<&StacAuthentication>,
        policy: &RetryPolicy,
    ) -> reqwest::Result<T> {
        let max_attempts = policy.max_retries.max(1);
        let mut backoff_failures = 0;
        let operation_name = format!("Fetch STAC items from {}", request.url());
        for attempt in 1..=max_attempts {
            let response = self.execute_attempt(client, &request, authentication).await;
            // The attempt has already applied any advertised windows to state.
            let server_controls_retry = response.as_ref().is_ok_and(|response| {
                response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
                    && (rate_limit_delay(response.headers()).is_some()
                        || !self.lock_state().windows.is_empty())
            });
            let result = async { response?.error_for_status()?.json::<T>().await }.await;
            let error = match result {
                Ok(value) => return Ok(value),
                Err(error) => error,
            };
            if is_terminal(&error, policy, &|error| {
                error.status().map(|status| status.as_u16())
            }) {
                return Err(error);
            }
            if attempt == max_attempts {
                tracing::error!("{operation_name} failed after {max_attempts} attempts: {error}");
                return Err(error);
            }
            // Every failure consumes the same attempt budget. Without server
            // quota information, 429s use the same backoff as other failures.
            if !server_controls_retry {
                let delay =
                    Duration::from_millis(policy.initial_delay_ms * 2_u64.pow(backoff_failures));
                backoff_failures += 1;
                tracing::warn!(
                    "{operation_name} failed (attempt {attempt}/{max_attempts}): {error}. Retrying in {delay:?}..."
                );
                tokio::time::sleep(delay).await;
            }
        }
        unreachable!("bounded STAC retry loop always returns")
    }

    /// Queue and dispatch one attempt using the current access token. Finish its
    /// guard as soon as headers or a transport error arrive; body decoding belongs
    /// to `execute` and does not keep the attempt in `State::in_flight`.
    async fn execute_attempt(
        self: &Arc<Self>,
        client: &reqwest::Client,
        request: &reqwest::Request,
        authentication: Option<&StacAuthentication>,
    ) -> reqwest::Result<reqwest::Response> {
        let request_guard = self.acquire().await;
        let request_copy = request
            .try_clone()
            .expect("STAC API searches are replayable GET requests");
        let mut request_builder = reqwest::RequestBuilder::from_parts(client.clone(), request_copy);
        if let Some(authentication) = authentication {
            let token = authentication.access_token().await;
            request_builder = request_builder.bearer_auth(token);
        }
        let response = request_builder.send().await;
        request_guard.finish(
            response
                .as_ref()
                .ok()
                .map(|response| (response.headers(), response.status().as_u16())),
        );
        response
    }

    /// Join the FIFO tail and wait until this attempt can reserve capacity.
    /// Only the head waits on a quota timer; other waiters await notifications.
    /// Dropping this future removes its queued attempt through the request guard.
    async fn acquire(self: &Arc<Self>) -> RequestGuard {
        let waiter = Arc::new(Notify::new());
        let mut request = RequestGuard {
            coordinator: self.clone(),
            phase: RequestPhase::Queued(waiter.clone()),
        };
        // Enqueue and the first admission check share one lock acquisition.
        let mut admission = {
            let mut state = self.lock_state();
            state.queue.push_back(waiter.clone());
            state.admit(&waiter, Instant::now())
        };
        loop {
            match admission {
                Admission::Admitted => {
                    request.phase = RequestPhase::InFlight;
                    return request;
                }
                Admission::WaitForChange => waiter.notified().await,
                Admission::WaitUntil(deadline) => tokio::select! {
                    () = waiter.notified() => {},
                    () = tokio::time::sleep_until(deadline.into()) => {},
                },
            }
            admission = self.lock_state().admit(&waiter, Instant::now());
        }
    }

    #[cfg(test)]
    fn notify(&self) {
        self.lock_state().wake_front();
    }
}

/// Cleanup obligation of a single attempt throughout its lifetime.
///
/// [`Admission`] describes one scheduling decision; this enum records which
/// resources [`RequestGuard`] must release when the attempt finishes or is dropped.
enum RequestPhase {
    /// Registered in the FIFO queue; the handle identifies the entry to remove.
    Queued(Arc<Notify>),
    /// Capacity is reserved and the attempt contributes to `State::in_flight`.
    InFlight,
    /// Bookkeeping has been released; dropping the guard requires no cleanup.
    Finished,
}

/// Owns cleanup for one attempt from queue insertion until response headers or a
/// transport error arrive.
///
/// This guard holds a coordinator reference and lifecycle state, not a mutex lock.
/// Normal completion calls [`Self::finish`] to release bookkeeping and apply
/// response feedback atomically. `Drop` handles cancellation or unwinding by
/// removing the waiter or decrementing [`State::in_flight`]. Neither path
/// refunds the admitted start recorded in the rolling history.
struct RequestGuard {
    coordinator: Arc<StacRequestCoordinator>,
    phase: RequestPhase,
}

impl RequestGuard {
    /// Finish an attempt and publish its headers in the same state update as its
    /// release. `None` represents a transport failure with no response headers.
    /// Header parsing happens before the scheduler mutex is acquired.
    fn finish(mut self, response: Option<(&HeaderMap, u16)>) {
        let feedback = response
            .map(|(headers, status)| ResponseFeedback::parse(headers, status, Instant::now()));
        let mut state = self.coordinator.lock_state();
        state.release(&self.phase);
        self.phase = RequestPhase::Finished;
        if let Some(feedback) = feedback {
            state.observe(feedback, Instant::now());
        }
    }

    #[cfg(test)]
    fn observe(self, headers: &HeaderMap, status: u16) {
        self.finish(Some((headers, status)));
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        if !matches!(self.phase, RequestPhase::Finished) {
            // A short synchronous lock is necessary to atomically remove the
            // cancelled waiter/reservation. No caller holds this lock across an
            // await, and poison recovery keeps cleanup from double-panicking.
            self.coordinator.lock_state().release(&self.phase);
        }
    }
}

/// Canonicalize valid OAuth endpoint URLs for registry matching, preserving the
/// original text when parsing fails.
fn normalize_endpoint(endpoint: &str) -> String {
    Url::parse(endpoint).map_or_else(|_| endpoint.to_owned(), |url| url.to_string())
}

/// Scheduling constraints extracted from one HTTP response before locking state.
///
/// Known policies carry their associated remaining/reset quota inside [`Window`].
/// Ambiguous quota feedback is kept separately, and a 429 contributes a shared
/// cooldown. [`State::observe`] merges these observations with existing constraints
/// and accounts for other unfinished attempts.
struct ResponseFeedback {
    /// Advertised policies, deduplicated by duration, with any identifiable quotas.
    windows: Vec<Window>,
    /// Remaining/reset feedback that could not be assigned to one advertised policy.
    unscoped_quota: Option<RemoteQuota>,
    /// Monotonic deadline from a 429's Retry-After/reset headers or fallback delay.
    cooldown_until: Option<Instant>,
}

impl ResponseFeedback {
    /// Parse rate-limit policies and relative reset seconds. Repeated header fields
    /// retain their positions so ambiguous remaining counts stay unscoped. For a
    /// 429, use the longest usable Retry-After/reset delay or the fixed fallback.
    fn parse(headers: &HeaderMap, status: u16, now: Instant) -> Self {
        let limits: Vec<_> = headers.get_all("x-ratelimit-limit").iter().collect();
        let remaining: Vec<_> = headers.get_all("x-ratelimit-remaining").iter().collect();
        let resets: Vec<_> = headers.get_all("x-ratelimit-reset").iter().collect();
        let mut feedback = Self {
            windows: Vec::new(),
            unscoped_quota: None,
            cooldown_until: None,
        };
        for index in 0..limits.len().max(remaining.len()) {
            let value = limits.get(index).and_then(|value| value.to_str().ok());
            let policies = value.map(parse_limit_field).unwrap_or_default();
            // A bare leading count identifies the binding policy only when it
            // matches one duration. Preserve malformed field positions, and do
            // not guess associations when repeated field counts differ.
            let active = value.and_then(|value| {
                value
                    .split(',')
                    .next()?
                    .split(';')
                    .next()?
                    .trim()
                    .parse::<usize>()
                    .ok()
            });
            let mut matching = policies
                .iter()
                .filter(|window| Some(window.limit) == active)
                .map(|window| window.duration);
            let scope = matching
                .next()
                .filter(|first| matching.all(|duration| duration == *first))
                .filter(|_| limits.len() == remaining.len());
            for policy in policies {
                if let Some(previous) = feedback
                    .windows
                    .iter_mut()
                    .find(|window| window.duration == policy.duration)
                {
                    previous.limit = previous.limit.min(policy.limit);
                } else {
                    feedback.windows.push(policy);
                }
            }
            let Some(available) = remaining
                .get(index)
                .and_then(|value| value.to_str().ok()?.trim().parse().ok())
            else {
                continue;
            };
            let reset = if resets.len() == remaining.len() {
                resets[index].to_str().ok().and_then(parse_seconds)
            } else {
                header_values(headers, "x-ratelimit-reset")
                    .filter_map(parse_seconds)
                    .max()
            }
            .unwrap_or(FALLBACK_DELAY);
            let quota = RemoteQuota {
                remaining: available,
                reset: now.checked_add(reset).unwrap_or(now + FALLBACK_DELAY),
            };
            let destination = if let Some(window) = scope.and_then(|duration| {
                feedback
                    .windows
                    .iter_mut()
                    .find(|window| window.duration == duration)
            }) {
                &mut window.remote
            } else {
                &mut feedback.unscoped_quota
            };
            RemoteQuota::merge(destination, quota, 0);
        }
        if status == 429 {
            let delay = rate_limit_delay(headers).unwrap_or(FALLBACK_DELAY);
            feedback.cooldown_until = Some(now.checked_add(delay).unwrap_or(now + FALLBACK_DELAY));
        }
        feedback
    }
}

/// Extract positive `limit;w=seconds` policies from a comma-separated limit field.
/// The bare leading count identifies a binding policy but has no window duration.
fn parse_limit_field(value: &str) -> Vec<Window> {
    value
        .split(',')
        .filter_map(|item| {
            let mut fields = item.trim().split(';');
            let limit = fields
                .next()?
                .trim()
                .parse::<usize>()
                .ok()
                .filter(|limit| *limit > 0)?;
            let duration =
                fields.find_map(|field| field.trim().strip_prefix("w=").and_then(parse_seconds))?;
            (!duration.is_zero()).then(|| Window::new(limit, duration))
        })
        .collect()
}

fn header_values<'a>(headers: &'a HeaderMap, name: &str) -> impl Iterator<Item = &'a str> {
    headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
}

/// Accept integer seconds only when they can form a representable monotonic deadline.
fn parse_seconds(value: &str) -> Option<Duration> {
    let duration = Duration::from_secs(value.trim().parse().ok()?);
    Instant::now().checked_add(duration).map(|_| duration)
}

/// Longest usable server retry delay, before applying any local fallback.
fn rate_limit_delay(headers: &HeaderMap) -> Option<Duration> {
    retry_after(headers)
        .into_iter()
        .chain(header_values(headers, "x-ratelimit-reset").filter_map(parse_seconds))
        .max()
        .filter(|delay| !delay.is_zero())
}

/// Interpret Retry-After as relative seconds or an HTTP date. Wall-clock dates
/// become a duration here; the scheduler subsequently stores a monotonic deadline.
fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    if let Some(duration) = parse_seconds(value) {
        return Some(duration);
    }
    let date = DateTime::parse_from_rfc2822(value)
        .ok()?
        .with_timezone(&Utc);
    (date - DateTime::<Utc>::from(SystemTime::now()))
        .to_std()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_limits(headers: &HeaderMap) -> Vec<Window> {
        ResponseFeedback::parse(headers, 200, Instant::now()).windows
    }

    #[tokio::test]
    async fn it_execute_json_owns_http_and_decode_retries() {
        use httptest::{Expectation, Server, matchers::request, responders};
        use serde_json::{Value, json};

        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/items"))
                .times(4)
                .respond_with(responders::cycle![
                    responders::status_code(429),
                    responders::status_code(500),
                    responders::status_code(200).body("invalid json"),
                    responders::json_encoded(json!({"complete": true})),
                ]),
        );
        let coordinator =
            StacRequestCoordinator::shared(&Url::parse(&server.url_str("/")).unwrap(), None);
        let client = reqwest::Client::new();
        let request = client.get(server.url_str("/items")).build().unwrap();
        let policy = RetryPolicy::new().max_retries(4).initial_delay_ms(1);
        let result: Value = coordinator
            .execute(&client, request, None, &policy)
            .await
            .expect("the coordinator retries HTTP and decode failures internally");
        assert_eq!(result, json!({"complete": true}));
        let state = coordinator.state.lock().unwrap();
        assert_eq!(state.in_flight, 0);
    }

    #[tokio::test]
    async fn it_execute_returns_terminal_http_errors_and_releases_admission() {
        use httptest::{Expectation, Server, matchers::request, responders};

        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/missing"))
                .times(1)
                .respond_with(responders::status_code(404)),
        );
        let coordinator =
            StacRequestCoordinator::shared(&Url::parse(&server.url_str("/")).unwrap(), None);
        let client = reqwest::Client::new();
        let request = client.get(server.url_str("/missing")).build().unwrap();
        let policy = RetryPolicy::new().stop_on_status(&[400, 404]);
        let error = coordinator
            .execute::<serde_json::Value>(&client, request, None, &policy)
            .await
            .expect_err("terminal responses are returned as errors");
        assert_eq!(error.status(), Some(reqwest::StatusCode::NOT_FOUND));
        assert_eq!(coordinator.state.lock().unwrap().in_flight, 0);
        coordinator.acquire().await.observe(&HeaderMap::new(), 200);
    }

    #[test]
    fn it_parses_all_advertised_windows_and_relative_reset() {
        let mut headers = HeaderMap::new();
        headers.insert("x-ratelimit-limit", "10, 10;w=1, 60;w=60".parse().unwrap());
        headers.insert("x-ratelimit-reset", "1".parse().unwrap());
        let windows = parse_limits(&headers);
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].limit, 10);
        assert_eq!(windows[0].duration, Duration::from_secs(1));
        assert_eq!(windows[1].limit, 60);
        assert_eq!(
            header_values(&headers, "x-ratelimit-reset")
                .next()
                .and_then(parse_seconds),
            Some(Duration::from_secs(1))
        );
    }

    #[test]
    fn it_retry_after_accepts_seconds_and_http_dates() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, "3".parse().unwrap());
        assert_eq!(retry_after(&headers), Some(Duration::from_secs(3)));
        headers.insert(
            RETRY_AFTER,
            "Mon, 21 Oct 2030 07:28:00 GMT".parse().unwrap(),
        );
        assert!(retry_after(&headers).is_some());
    }

    #[test]
    fn it_requires_usable_server_retry_timing() {
        let mut headers = HeaderMap::new();
        assert_eq!(rate_limit_delay(&headers), None);
        for value in ["invalid", "0", "-1", "18446744073709551615"] {
            headers.insert(RETRY_AFTER, value.parse().unwrap());
            headers.insert("x-ratelimit-reset", value.parse().unwrap());
            assert_eq!(rate_limit_delay(&headers), None);
        }
        headers.insert(RETRY_AFTER, "2".parse().unwrap());
        headers.insert("x-ratelimit-reset", "3".parse().unwrap());
        assert_eq!(rate_limit_delay(&headers), Some(Duration::from_secs(3)));
    }

    #[test]
    fn it_coordinators_share_by_normalized_origin_and_full_auth_identity() {
        let credentials = super::super::StacProviderAuthentication {
            endpoint: "https://auth.example/token/".to_owned(),
            client_id: "client".to_owned(),
            username: "user".to_owned(),
            password: "secret-value-must-not-be-debugged".to_owned(),
        };
        let first = StacRequestCoordinator::shared(
            &Url::parse("https://STAC.example:443/api").unwrap(),
            Some(&credentials),
        );
        let same_identity = StacRequestCoordinator::shared(
            &Url::parse("https://stac.example/other").unwrap(),
            Some(&credentials),
        );
        let anonymous =
            StacRequestCoordinator::shared(&Url::parse("https://stac.example").unwrap(), None);

        assert!(Arc::ptr_eq(&first, &same_identity));
        assert!(!Arc::ptr_eq(&first, &anonymous));
        assert!(!format!("{first:?}").contains("secret-value-must-not-be-debugged"));
    }

    async fn wait_for_queue(coordinator: &StacRequestCoordinator, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if coordinator.state.lock().unwrap().queue.len() == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("request should reach coordinator queue");
    }

    fn isolated_coordinator(label: &str) -> Arc<StacRequestCoordinator> {
        StacRequestCoordinator::shared(
            &Url::parse(&format!("https://{label}.stac-coordinator.test")).unwrap(),
            None,
        )
    }

    #[tokio::test]
    async fn it_queued_attempts_are_admitted_in_fifo_order_after_window_opens() {
        let coordinator = isolated_coordinator("fifo");
        {
            let mut state = coordinator.state.lock().unwrap();
            state.windows.push(Window::new(1, Duration::from_mins(1)));
            state.starts.push_back(Instant::now());
        }

        let order = Arc::new(Mutex::new(Vec::new()));
        let first_order = order.clone();
        let first_coordinator = coordinator.clone();
        let mut first = tokio::spawn(async move {
            let _permit = first_coordinator.acquire().await;
            first_order.lock().unwrap().push(1);
        });
        wait_for_queue(&coordinator, 1).await;
        let second_order = order.clone();
        let second_coordinator = coordinator.clone();
        let mut second = tokio::spawn(async move {
            let _permit = second_coordinator.acquire().await;
            second_order.lock().unwrap().push(2);
        });
        wait_for_queue(&coordinator, 2).await;
        assert!(!first.is_finished());

        coordinator.state.lock().unwrap().starts.clear();
        coordinator.notify();
        tokio::time::timeout(Duration::from_millis(200), &mut first)
            .await
            .expect("first queued request should be admitted")
            .unwrap();

        // The first admission fills the one request window again. Open it
        // explicitly to let the second waiter proceed without a one minute test.
        coordinator.state.lock().unwrap().starts.clear();
        coordinator.notify();
        tokio::time::timeout(Duration::from_millis(200), &mut second)
            .await
            .expect("second queued request should be admitted")
            .unwrap();
        assert_eq!(*order.lock().unwrap(), [1, 2]);
    }

    #[tokio::test]
    async fn it_cancelling_a_queued_attempt_does_not_block_later_requests() {
        let coordinator = isolated_coordinator("cancel");
        let first = coordinator.acquire().await;
        coordinator.lock_state().cooldown_until = Some(Instant::now() + Duration::from_mins(1));
        let queued_coordinator = coordinator.clone();
        let queued = tokio::spawn(async move { queued_coordinator.acquire().await });
        wait_for_queue(&coordinator, 1).await;
        queued.abort();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if coordinator.state.lock().unwrap().queue.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled queue entry should be removed");

        first.observe(&HeaderMap::new(), 200);
        coordinator.lock_state().cooldown_until = None;
        let later = tokio::time::timeout(Duration::from_millis(200), coordinator.acquire())
            .await
            .expect("later request should not be blocked by cancellation");
        later.observe(&HeaderMap::new(), 200);
    }

    #[tokio::test]
    async fn it_the_long_rate_limit_window_remains_enforced_after_the_short_window_opens() {
        let coordinator = isolated_coordinator("long-window");
        {
            let mut state = coordinator.state.lock().unwrap();
            let now = Instant::now();
            state.windows.push(Window::new(10, Duration::from_secs(1)));
            state.windows.push(Window::new(60, Duration::from_mins(1)));
            state.starts =
                std::iter::repeat_n(now.checked_sub(Duration::from_secs(30)).unwrap(), 60)
                    .collect();
        }

        let mut waiting = Box::pin(coordinator.acquire());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut waiting)
                .await
                .is_err()
        );
        coordinator.state.lock().unwrap().starts.clear();
        coordinator.notify();
        let permit = tokio::time::timeout(Duration::from_millis(200), &mut waiting)
            .await
            .expect("opening the long window should admit the queued request");
        permit.observe(&HeaderMap::new(), 200);
    }

    #[tokio::test]
    async fn it_outstanding_dispatches_reduce_observed_remaining_and_stale_headers_do_not_refill_it()
     {
        let coordinator = isolated_coordinator("remaining");
        let first = coordinator.acquire().await;
        let mut bootstrap_headers = HeaderMap::new();
        bootstrap_headers.insert("x-ratelimit-limit", "10;w=60".parse().unwrap());
        bootstrap_headers.insert("x-ratelimit-remaining", "9".parse().unwrap());
        bootstrap_headers.insert("x-ratelimit-reset", "60".parse().unwrap());
        first.observe(&bootstrap_headers, 200);

        let older = coordinator.acquire().await;
        let newer = coordinator.acquire().await;
        let mut newer_headers = HeaderMap::new();
        newer_headers.insert("x-ratelimit-limit", "10;w=60".parse().unwrap());
        newer_headers.insert("x-ratelimit-remaining", "8".parse().unwrap());
        newer_headers.insert("x-ratelimit-reset", "60".parse().unwrap());
        newer.observe(&newer_headers, 200);
        assert_eq!(
            coordinator.state.lock().unwrap().windows[0]
                .remote
                .as_ref()
                .map(|quota| quota.remaining),
            Some(7)
        );

        let mut stale_headers = HeaderMap::new();
        stale_headers.insert("x-ratelimit-limit", "10;w=60".parse().unwrap());
        stale_headers.insert("x-ratelimit-remaining", "9".parse().unwrap());
        stale_headers.insert("x-ratelimit-reset", "60".parse().unwrap());
        older.observe(&stale_headers, 200);
        assert_eq!(
            coordinator.state.lock().unwrap().windows[0]
                .remote
                .as_ref()
                .map(|quota| quota.remaining),
            Some(7)
        );
    }

    #[tokio::test]
    async fn it_keeps_attempt_history_after_transport_failure() {
        let coordinator = isolated_coordinator("bootstrap-failure");
        drop(coordinator.acquire().await);
        let response = coordinator.acquire().await;
        let mut headers = HeaderMap::new();
        headers.insert("x-ratelimit-limit", "2;w=60".parse().unwrap());
        response.observe(&headers, 200);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), coordinator.acquire())
                .await
                .is_err(),
            "both attempts reserve window capacity"
        );
    }

    #[tokio::test]
    async fn it_shares_429_cooldown_and_honors_the_longer_reset() {
        let coordinator = isolated_coordinator("shared-cooldown");
        let response = coordinator.acquire().await;
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, "1".parse().unwrap());
        headers.insert("x-ratelimit-reset", "2".parse().unwrap());
        let observed = Instant::now();
        response.observe(&headers, 429);
        let same = isolated_coordinator("shared-cooldown");
        assert!(Arc::ptr_eq(&same, &coordinator));
        assert!(
            coordinator.state.lock().unwrap().cooldown_until.unwrap()
                >= observed + Duration::from_secs(2)
        );
        let mut waiting = Box::pin(same.acquire());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut waiting)
                .await
                .is_err()
        );
        coordinator.state.lock().unwrap().cooldown_until = Some(Instant::now());
        coordinator.notify();
        tokio::time::timeout(Duration::from_millis(200), &mut waiting)
            .await
            .expect("all callers resume after the shared cooldown");
    }

    #[tokio::test]
    async fn it_exhausted_remaining_without_reset_uses_a_bounded_cooldown() {
        let coordinator = isolated_coordinator("missing-reset");
        let first = coordinator.acquire().await;
        let mut headers = HeaderMap::new();
        headers.insert("x-ratelimit-limit", "2;w=60".parse().unwrap());
        headers.insert("x-ratelimit-remaining", "1".parse().unwrap());
        first.observe(&headers, 200);

        let second = coordinator.acquire().await;
        assert_eq!(
            coordinator.state.lock().unwrap().windows[0]
                .remote
                .as_ref()
                .map(|quota| quota.remaining),
            Some(0)
        );
        assert!(
            coordinator.state.lock().unwrap().windows[0]
                .remote
                .as_ref()
                .is_some_and(|quota| quota.reset > Instant::now())
        );
        drop(second);
    }

    fn quota_headers(policies: &[(&str, &str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (limit, remaining, reset) in policies {
            headers.append("x-ratelimit-limit", limit.parse().unwrap());
            headers.append("x-ratelimit-remaining", remaining.parse().unwrap());
            headers.append("x-ratelimit-reset", reset.parse().unwrap());
        }
        headers
    }

    #[tokio::test]
    async fn it_preserves_both_repeated_and_alternating_server_policies() {
        let coordinator = isolated_coordinator("multiple-policies");
        let minute = ("1800, 1800;w=60, 1800;w=60", "1799", "50");
        let second = ("20, 20;w=1, 20;w=1", "19", "1");
        coordinator
            .acquire()
            .await
            .observe(&quota_headers(&[minute, second]), 200);
        {
            let state = coordinator.state.lock().unwrap();
            assert_eq!(state.windows.len(), 2, "duplicates are one policy");
            for (duration, limit, remaining) in [(60, 1800, 1799), (1, 20, 19)] {
                let window = state
                    .windows
                    .iter()
                    .find(|window| window.duration == Duration::from_secs(duration))
                    .unwrap();
                assert_eq!(window.limit, limit);
                assert_eq!(state.starts.len(), 1);
                assert_eq!(window.remote.as_ref().unwrap().remaining, remaining);
            }
        }
        coordinator
            .acquire()
            .await
            .observe(&quota_headers(&[second]), 200);
        coordinator
            .acquire()
            .await
            .observe(&quota_headers(&[minute]), 200);
        let state = coordinator.state.lock().unwrap();
        assert_eq!(
            state.windows.len(),
            2,
            "reporting the binding policy preserves the other policy"
        );
        assert_eq!(state.starts.len(), 3);
    }

    #[tokio::test]
    async fn it_admits_twenty_parallel_requests_and_blocks_the_twenty_first() {
        let coordinator = isolated_coordinator("parallel-twenty");
        coordinator
            .acquire()
            .await
            .observe(&quota_headers(&[("20, 20;w=1, 20;w=1", "19", "1")]), 200);
        // Move into a fresh window without a real one-second test delay.
        {
            let mut state = coordinator.state.lock().unwrap();
            state.starts.clear();
            state.windows[0].remote = None;
        }
        let mut requests = tokio::task::JoinSet::new();
        for _ in 0..20 {
            let coordinator = coordinator.clone();
            requests.spawn(async move { coordinator.acquire().await });
        }
        let mut permits = Vec::new();
        tokio::time::timeout(Duration::from_millis(500), async {
            while let Some(permit) = requests.join_next().await {
                permits.push(permit.unwrap());
            }
        })
        .await
        .expect("all twenty attempts must overlap without waiting for responses");
        assert_eq!(coordinator.state.lock().unwrap().in_flight, 20);
        let mut next = Box::pin(coordinator.acquire());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut next)
                .await
                .is_err()
        );
        drop(permits);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut next)
                .await
                .is_err(),
            "completion does not refund rate-limit reservations"
        );
        coordinator.state.lock().unwrap().starts.clear();
        coordinator.notify();
        tokio::time::timeout(Duration::from_millis(200), next)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn it_older_responses_can_reduce_but_never_refill_capacity() {
        let coordinator = isolated_coordinator("older-exhaustion");
        let headers = quota_headers(&[("20;w=60", "19", "60")]);
        coordinator.acquire().await.observe(&headers, 200);
        let older = coordinator.acquire().await;
        let newer = coordinator.acquire().await;
        newer.observe(&quota_headers(&[("20;w=60", "18", "60")]), 200);
        // Other clients can consume the server quota before the older request
        // is processed; dispatch order does not determine server ordering.
        older.observe(&quota_headers(&[("20;w=60", "0", "60")]), 200);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), coordinator.acquire())
                .await
                .is_err()
        );
        assert_eq!(
            coordinator.state.lock().unwrap().windows[0]
                .remote
                .as_ref()
                .unwrap()
                .remaining,
            0
        );
    }

    #[tokio::test]
    async fn it_retains_long_reset_deadlines_across_provider_reconstruction() {
        let origin = Url::parse("https://retained-reset.stac-coordinator.test").unwrap();
        let coordinator = StacRequestCoordinator::shared(&origin, None);
        let mut headers = HeaderMap::new();
        headers.insert("x-ratelimit-remaining", "0".parse().unwrap());
        headers.insert("x-ratelimit-reset", "3600".parse().unwrap());
        coordinator.acquire().await.observe(&headers, 200);
        {
            let mut state = coordinator.state.lock().unwrap();
            state.last_activity = Instant::now().checked_sub(Duration::from_mins(10)).unwrap();
            assert!(state.retain_until() >= state.unscoped_quota.as_ref().unwrap().reset);
        }
        let previous = Arc::downgrade(&coordinator);
        drop(coordinator);
        let reconstructed = StacRequestCoordinator::shared(&origin, None);
        assert!(previous.ptr_eq(&Arc::downgrade(&reconstructed)));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), reconstructed.acquire())
                .await
                .is_err()
        );
        // Once both history and feedback expire the registry can reclaim it.
        {
            let mut state = reconstructed.state.lock().unwrap();
            state.unscoped_quota.as_mut().unwrap().reset = Instant::now();
        }
        drop(reconstructed);
        let replacement = StacRequestCoordinator::shared(&origin, None);
        assert!(!previous.ptr_eq(&Arc::downgrade(&replacement)));
    }

    #[tokio::test]
    async fn it_waits_for_the_binding_constraint_across_arbitrary_windows() {
        let coordinator = isolated_coordinator("binding-window");
        coordinator.acquire().await.observe(
            &quota_headers(&[
                ("2;w=1", "1", "1"),
                ("7;w=37", "6", "37"),
                ("90;w=3600", "89", "3600"),
            ]),
            200,
        );
        let mut state = coordinator.state.lock().unwrap();
        let now = Instant::now();
        state.windows[0].remote = Some(RemoteQuota {
            remaining: 0,
            reset: now + Duration::from_secs(1),
        });
        state.windows[1].remote = Some(RemoteQuota {
            remaining: 0,
            reset: now + Duration::from_secs(37),
        });
        let waiter = Arc::new(Notify::new());
        state.queue.push_back(waiter.clone());
        assert!(
            matches!(state.admit(&waiter, now), Admission::WaitUntil(deadline) if deadline == now + Duration::from_secs(37))
        );
        // At 37s both exhausted policies reset; the hourly policy still has
        // capacity, so it must not serialize dispatch or delay it for an hour.
        assert!(matches!(
            state.admit(&waiter, now + Duration::from_secs(37)),
            Admission::Admitted
        ));
    }

    #[test]
    fn it_deduplicates_windows_and_keeps_the_stricter_duplicate() {
        let headers = quota_headers(&[
            ("20, 20;w=1, 20;w=1", "19", "1"),
            ("1800, 1800;w=60, 1800;w=60", "1799", "50"),
            ("15;w=1, 100000;w=172800", "14", "1"),
        ]);
        let windows = parse_limits(&headers);
        assert_eq!(windows.len(), 3);
        assert_eq!(windows[0].limit, 15);
        assert_eq!(
            windows[2].limit, 100_000,
            "do not impose an artificial request cap"
        );
        assert_eq!(windows[2].duration, Duration::from_hours(48));
        let mut malformed = HeaderMap::new();
        malformed.insert(
            "x-ratelimit-limit",
            "0;w=1, 2;w=0, invalid, 10;w=18446744073709551615"
                .parse()
                .unwrap(),
        );
        assert!(parse_limits(&malformed).is_empty());
    }

    #[tokio::test]
    async fn it_replaces_the_fallback_with_the_advertised_capacity() {
        let coordinator = isolated_coordinator("fallback-upgrade");
        coordinator.acquire().await.observe(&HeaderMap::new(), 200);
        coordinator
            .acquire()
            .await
            .observe(&quota_headers(&[("20;w=1", "18", "1")]), 200);
        let state = coordinator.state.lock().unwrap();
        assert_eq!(state.windows.len(), 1);
        assert_eq!(state.windows[0].limit, 20);
        assert_eq!(state.starts.len(), 2);
    }

    #[test]
    fn it_does_not_assign_ambiguous_feedback_to_the_wrong_window() {
        let mut headers = quota_headers(&[("20;w=1", "19", "1"), ("1800;w=60", "1799", "50")]);
        headers.insert("x-ratelimit-remaining", "0".parse().unwrap());
        let now = Instant::now();
        let feedback = ResponseFeedback::parse(&headers, 200, now);
        let quota = feedback.unscoped_quota.unwrap();
        assert_eq!(quota.remaining, 0);
        assert_eq!(quota.reset, now + Duration::from_secs(50));
        assert!(
            feedback
                .windows
                .iter()
                .all(|window| window.remote.is_none())
        );
        headers.remove("x-ratelimit-limit");
        headers.append(
            "x-ratelimit-limit",
            reqwest::header::HeaderValue::from_bytes(&[255]).unwrap(),
        );
        headers.append("x-ratelimit-limit", "1800;w=60".parse().unwrap());
        headers.remove("x-ratelimit-remaining");
        headers.append("x-ratelimit-remaining", "0".parse().unwrap());
        headers.append("x-ratelimit-remaining", "1799".parse().unwrap());
        let feedback = ResponseFeedback::parse(&headers, 200, now);
        assert_eq!(feedback.unscoped_quota.unwrap().remaining, 0);
        assert_eq!(feedback.windows.len(), 1);
        assert_eq!(feedback.windows[0].duration, Duration::from_mins(1));
        assert_eq!(feedback.windows[0].remote.as_ref().unwrap().remaining, 1799);
    }

    #[tokio::test]
    async fn it_uses_the_callers_attempt_limit_for_rate_limit_retries() {
        use httptest::{Expectation, Server, matchers::request, responders};
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/limited"))
                .times(2)
                .respond_with(responders::status_code(429)),
        );
        let client = reqwest::Client::new();
        let coordinator =
            StacRequestCoordinator::shared(&Url::parse(&server.url_str("/")).unwrap(), None);
        let error = coordinator
            .execute::<serde_json::Value>(
                &client,
                client.get(server.url_str("/limited")).build().unwrap(),
                None,
                &RetryPolicy::new().max_retries(2),
            )
            .await
            .unwrap_err();
        assert_eq!(error.status(), Some(reqwest::StatusCode::TOO_MANY_REQUESTS));
    }

    #[tokio::test]
    async fn it_backs_off_exponentially_for_headerless_rate_limits() {
        use httptest::{Expectation, Server, matchers::request, responders};

        let server = Server::run();
        let starts = Arc::new(Mutex::new(Vec::new()));
        let response_starts = starts.clone();
        server.expect(
            Expectation::matching(request::method_path("GET", "/limited"))
                .times(4)
                .respond_with(move || {
                    let mut starts = response_starts.lock().unwrap();
                    starts.push(Instant::now());
                    responders::status_code(if starts.len() < 4 { 429 } else { 200 }).body("null")
                }),
        );
        let client = reqwest::Client::new();
        let coordinator =
            StacRequestCoordinator::shared(&Url::parse(&server.url_str("/")).unwrap(), None);
        let policy = RetryPolicy::new().max_retries(4).initial_delay_ms(600);
        tokio::time::timeout(
            Duration::from_secs(10),
            coordinator.execute::<serde_json::Value>(
                &client,
                client.get(server.url_str("/limited")).build().unwrap(),
                None,
                &policy,
            ),
        )
        .await
        .expect("headerless rate limits must recover within the attempt budget")
        .unwrap();
        let starts = starts.lock().unwrap();
        for (attempt, pair) in starts.windows(2).enumerate() {
            assert!(
                pair[1].duration_since(pair[0])
                    >= Duration::from_millis(600 * 2_u64.pow(attempt as u32)),
                "each headerless 429 must increase the retry delay"
            );
        }
    }

    #[tokio::test]
    async fn it_uses_server_rate_limits_without_adding_exponential_backoff() {
        use httptest::{Expectation, Server, matchers::request, responders};

        for (header, value) in [
            ("retry-after", "1"),
            ("x-ratelimit-reset", "1"),
            ("x-ratelimit-limit", "100;w=1"),
        ] {
            let server = Server::run();
            server.expect(
                Expectation::matching(request::method_path("GET", "/limited"))
                    .times(2)
                    .respond_with(responders::cycle![
                        responders::status_code(429).append_header(header, value),
                        responders::status_code(200).body("null"),
                    ]),
            );
            let client = reqwest::Client::new();
            let coordinator =
                StacRequestCoordinator::shared(&Url::parse(&server.url_str("/")).unwrap(), None);
            let policy = RetryPolicy::new().max_retries(2).initial_delay_ms(10_000);
            tokio::time::timeout(
                Duration::from_secs(3),
                coordinator.execute::<serde_json::Value>(
                    &client,
                    client.get(server.url_str("/limited")).build().unwrap(),
                    None,
                    &policy,
                ),
            )
            .await
            .expect("server-controlled retries must not add the ten-second policy backoff")
            .unwrap();
        }
    }

    #[tokio::test]
    async fn it_shares_the_attempt_budget_between_rate_limits_and_other_failures() {
        use httptest::{Expectation, Server, matchers::request, responders};

        for statuses in [[429, 500], [500, 429], [429, 200]] {
            let server = Server::run();
            server.expect(
                Expectation::matching(request::method_path("GET", "/items"))
                    .times(2)
                    .respond_with(responders::cycle![
                        responders::status_code(statuses[0]),
                        // A 200 with an empty body exercises JSON decode errors.
                        responders::status_code(statuses[1]),
                    ]),
            );
            let client = reqwest::Client::new();
            let coordinator =
                StacRequestCoordinator::shared(&Url::parse(&server.url_str("/")).unwrap(), None);
            let error = coordinator
                .execute::<serde_json::Value>(
                    &client,
                    client.get(server.url_str("/items")).build().unwrap(),
                    None,
                    &RetryPolicy::new().max_retries(2).initial_delay_ms(1),
                )
                .await
                .unwrap_err();
            if statuses[1] == 200 {
                assert!(error.is_decode());
            } else {
                assert_eq!(error.status().unwrap().as_u16(), statuses[1]);
            }
        }
    }

    #[tokio::test]
    async fn it_uses_fallback_capacity_without_waiting_for_headers() {
        let coordinator = isolated_coordinator("cold-concurrency");
        let mut requests = Vec::new();
        for _ in 0..10 {
            requests.push(
                tokio::time::timeout(Duration::from_millis(200), coordinator.acquire())
                    .await
                    .expect("cold requests must not serialize on the first response"),
            );
        }
        assert_eq!(coordinator.lock_state().in_flight, 10);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), coordinator.acquire())
                .await
                .is_err()
        );
        for request in requests {
            request.finish(None);
        }
        assert_eq!(coordinator.lock_state().in_flight, 0);
        assert_eq!(
            coordinator.lock_state().starts.len(),
            10,
            "completion does not refund starts"
        );
    }

    #[tokio::test]
    async fn it_learns_a_longer_window_from_the_shared_history() {
        let coordinator = isolated_coordinator("shared-history");
        coordinator
            .acquire()
            .await
            .observe(&quota_headers(&[("20;w=1", "19", "1")]), 200);
        {
            let mut state = coordinator.lock_state();
            state.starts[0] = Instant::now().checked_sub(Duration::from_secs(2)).unwrap();
        }
        coordinator
            .acquire()
            .await
            .observe(&quota_headers(&[("2;w=60", "0", "60")]), 200);
        assert_eq!(
            coordinator.lock_state().starts.len(),
            2,
            "short-window expiration preserves history for newly learned policies"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(10), coordinator.acquire())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn it_cleans_up_cancelled_requests_even_after_mutex_poisoning() {
        let coordinator = isolated_coordinator("poison-cleanup");
        let request = coordinator.acquire().await;
        let poisoner = coordinator.clone();
        assert!(
            std::thread::spawn(move || {
                let _state = poisoner.state.lock().unwrap();
                panic!("simulate an unrelated panic while holding scheduler state");
            })
            .join()
            .is_err()
        );
        drop(request);
        assert_eq!(coordinator.lock_state().in_flight, 0);
        coordinator.acquire().await.finish(None);
        assert_eq!(coordinator.lock_state().in_flight, 0);
    }
}
