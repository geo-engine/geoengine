//! Coordinates outgoing STAC HTTP requests to respect shared server rate limits.
//! Providers with the same configured STAC origin and credentials share a FIFO
//! queue. A channel actor owns that queue and all quota state, admits requests in
//! order, and runs admitted HTTP attempts concurrently.
//!
//! [`Identity`] selects shared [`ActorChannels`] inside an owned
//! [`StacRequestRegistry`], shared across providers and collections that use that
//! registry. The actor owns the FIFO queue and [`State`], which holds request
//! history, learned rate-limit windows and server feedback. Admitted HTTP
//! attempts may overlap.
//!
//! One [`StacClient::execute_json`] call may make several HTTP attempts.
//! Each attempt is an owned channel job with a one-shot reply. The actor applies
//! response feedback before replying. Failures and cancellation keep an admitted
//! request start charged against the rolling windows.
//!
//! Retries for transient failures wait for exponential backoff outside the queue,
//! then join its tail again. A 429 sets a shared cooldown; retries also back off
//! unless the server supplies quota windows or usable retry timing. Admission
//! waits for the cooldown and all quota constraints.
use super::{StacProviderAuthentication, auth::StacAuthentication};
use crate::util::retry::{RetryAction, RetryPolicy, retry_http_with_action};
use chrono::{DateTime, Utc};
use dashmap::{DashMap, mapref::entry::Entry};
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use reqwest::header::{HeaderMap, RETRY_AFTER};
use serde::de::DeserializeOwned;
use snafu::Snafu;
use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::{mpsc, oneshot};
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
    /// Number of requests remaining, conservatively reduced for local reservations.
    remaining_requests: usize,
    /// Absolute deadline derived from the server's relative reset duration.
    reset: Instant,
}

impl RemoteQuota {
    /// Deduct other local reservations that the server may not have counted yet,
    /// then retain the smaller budget and later reset from both observations.
    fn merge(current: &mut Option<Self>, mut observed: Self, reservations: usize) {
        observed.remaining_requests = observed.remaining_requests.saturating_sub(reservations);
        if let Some(current) = current {
            current.remaining_requests =
                current.remaining_requests.min(observed.remaining_requests);
            current.reset = current.reset.max(observed.reset);
        } else {
            *current = Some(observed);
        }
    }

    fn blocked_until(&self) -> Option<Instant> {
        (self.remaining_requests == 0).then_some(self.reset)
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

/// Mutable quota data owned exclusively by the coordinator actor.
///
/// The history of request starts enforces rates independently of how many attempts
/// are currently waiting for response headers.
#[derive(Debug)]
struct State {
    /// Number of admitted HTTP attempts still waiting for response headers or a transport error.
    /// Used to adjust server feedback; this is not a concurrency limit.
    in_flight_requests: usize,
    /// Monotonic completion count, including failures and cancellation. Attempts
    /// snapshot this at admission so delayed feedback also charges other
    /// requests that finished while they were running.
    completed_requests: usize,
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

    /// Return a blocking deadline, or reserve capacity and return `None`.
    fn reserve(&mut self, now: Instant) -> Option<Instant> {
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
            return Some(deadline);
        }
        self.in_flight_requests += 1;
        self.starts.push_back(now);
        self.last_activity = now;
        for quota in self
            .windows
            .iter_mut()
            .filter_map(|window| window.remote.as_mut())
            .chain(self.unscoped_quota.as_mut())
        {
            quota.remaining_requests = quota.remaining_requests.saturating_sub(1);
        }
        None
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
    /// The responding attempt must already be released. Charge both other
    /// in-flight attempts and those that completed since its admission, excluding
    /// the responding attempt itself, against the reported remaining capacity.
    fn observe(&mut self, feedback: ResponseFeedback, now: Instant, completed_before: usize) {
        self.prune(now);
        let reservations = self.in_flight_requests
            + self
                .completed_requests
                .saturating_sub(completed_before)
                .saturating_sub(1);
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
                RemoteQuota::merge(&mut window.remote, remote, reservations);
            }
        }
        if let Some(quota) = feedback.unscoped_quota {
            RemoteQuota::merge(&mut self.unscoped_quota, quota, reservations);
        }
        if let Some(deadline) = feedback.cooldown_until {
            self.cooldown_until = Some(
                self.cooldown_until
                    .map_or(deadline, |current| current.max(deadline)),
            );
        }
    }

    /// Release one in-flight attempt. Its request start remains charged.
    fn release(&mut self) {
        self.in_flight_requests = self.in_flight_requests.saturating_sub(1);
        self.completed_requests += 1;
        self.last_activity = Instant::now();
    }
}

/// One owned HTTP attempt and its private completion channel.
#[derive(Debug)]
struct AttemptJob {
    client: reqwest::Client,
    request: reqwest::Request,
    authentication: Option<StacAuthentication>,
    reply: oneshot::Sender<AttemptReply>,
}

#[derive(Debug)]
struct AttemptReply {
    response: reqwest::Result<reqwest::Response>,
    server_controls_retry: bool,
}

/// Shared request and cancellation channels plus their registry identity.
#[derive(Debug)]
pub(super) struct ActorChannels {
    jobs: mpsc::UnboundedSender<AttemptJob>,
    cancellations: mpsc::UnboundedSender<()>,
    identity: Identity,
    registry: Weak<CoordinatorMap>,
}

/// Close the reply before notifying the actor, so a cancellation wakeup cannot
/// race ahead of the receiver being dropped and leave a queued job behind.
struct PendingAttempt {
    reply: oneshot::Receiver<AttemptReply>,
    cancellation: Option<mpsc::UnboundedSender<()>>,
}

impl Drop for PendingAttempt {
    fn drop(&mut self) {
        if let Some(sender) = &self.cancellation {
            self.reply.close();
            let _ = sender.send(());
        }
    }
}

type AttemptOutcome = (
    oneshot::Sender<AttemptReply>,
    Option<reqwest::Result<reqwest::Response>>,
);
type RunningAttempt = Pin<Box<dyn Future<Output = AttemptOutcome> + Send>>;
type CoordinatorMap = DashMap<Identity, Arc<ActorChannels>>;
type ActorReceivers = (
    mpsc::UnboundedReceiver<AttemptJob>,
    mpsc::UnboundedReceiver<()>,
);

/// Application- or operation-owned coordinator registry.
#[derive(Clone, Default)]
pub struct StacRequestRegistry {
    coordinators: Arc<CoordinatorMap>,
}

impl fmt::Debug for StacRequestRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StacRequestRegistry")
            .finish_non_exhaustive()
    }
}

impl StacRequestRegistry {
    /// Create an empty request registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Find or create the coordinator for this origin and authentication.
    /// Provider ids, collection ids and refreshed access tokens do not split quotas.
    pub(super) fn shared(
        &self,
        origin: &Url,
        authentication: Option<&StacProviderAuthentication>,
    ) -> Arc<ActorChannels> {
        let identity = identity_for(origin, authentication);
        self.shared_with_dispatch(identity, |job| Box::pin(run_attempt(job)) as RunningAttempt)
    }

    fn shared_with_dispatch<F>(&self, identity: Identity, dispatch: F) -> Arc<ActorChannels>
    where
        F: Fn(AttemptJob) -> RunningAttempt + Send + 'static,
    {
        // A closed actor is replaced only for this identity. Other entries
        // retire themselves after their retained quota history expires.
        let (channels, receivers) = match self.coordinators.entry(identity.clone()) {
            Entry::Occupied(mut entry) if entry.get().jobs.is_closed() => {
                let (channels, receivers) =
                    Self::new_coordinator(identity, Arc::downgrade(&self.coordinators));
                entry.insert(channels.clone());
                (channels, Some(receivers))
            }
            Entry::Occupied(entry) => (entry.get().clone(), None),
            Entry::Vacant(entry) => {
                let (channels, receivers) =
                    Self::new_coordinator(identity, Arc::downgrade(&self.coordinators));
                entry.insert(channels.clone());
                (channels, Some(receivers))
            }
        };

        if let Some((jobs, cancellations)) = receivers {
            tokio::spawn(run_actor(
                jobs,
                cancellations,
                Arc::downgrade(&channels),
                dispatch,
            ));
        }
        channels
    }

    fn new_coordinator(
        identity: Identity,
        registry: Weak<CoordinatorMap>,
    ) -> (Arc<ActorChannels>, ActorReceivers) {
        let (jobs, job_receiver) = mpsc::unbounded_channel();
        let (cancellations, cancellation_receiver) = mpsc::unbounded_channel();
        (
            Arc::new(ActorChannels {
                jobs,
                cancellations,
                identity,
                registry,
            }),
            (job_receiver, cancellation_receiver),
        )
    }
}

fn identity_for(
    origin: &Url,
    authentication: Option<&super::StacProviderAuthentication>,
) -> Identity {
    Identity {
        scheme: origin.scheme().to_ascii_lowercase(),
        host: origin.host_str().unwrap_or_default().to_ascii_lowercase(),
        port: origin.port_or_known_default().unwrap_or(0),
        token_endpoint: authentication.map(|a| normalize_endpoint(&a.endpoint)),
        client_id: authentication.map(|a| a.client_id.clone()),
        username: authentication.map(|a| a.username.clone()),
        password: authentication.map(|a| a.password.clone()),
    }
}

/// Shared channel endpoint for one configured origin and authentication identity.
///
/// The actor owns FIFO admission and quota state. Provider and client clones only
/// submit owned jobs; they never hold a scheduler lock while waiting or sending.
impl ActorChannels {
    #[cfg(test)]
    pub(super) fn is_closed(&self) -> bool {
        self.jobs.is_closed()
    }

    /// Request retirement in registry lookup order. The actor can stop when its
    /// entry was removed or is already absent; external owners prevent removal.
    fn try_retire(self_weak: &Weak<Self>) -> bool {
        let Some(channels) = self_weak.upgrade() else {
            return true;
        };
        let registry = channels.registry.clone();
        let identity = channels.identity.clone();
        drop(channels);
        let Some(registry) = registry.upgrade() else {
            return true;
        };
        match registry.entry(identity) {
            Entry::Occupied(entry)
                if std::ptr::eq(Arc::as_ptr(entry.get()), self_weak.as_ptr()) =>
            {
                if Arc::strong_count(entry.get()) == 1 {
                    drop(entry.remove());
                    true
                } else {
                    false
                }
            }
            Entry::Occupied(_) | Entry::Vacant(_) => true,
        }
    }
}

/// STAC HTTP transport with a shared request channel.
/// Clones share the connection pool, token renewal state and request registry.
#[derive(Clone)]
pub(crate) struct StacClient {
    client: reqwest::Client,
    origin: Url,
    pub(super) request_registry: StacRequestRegistry,
    coordinator_authentication: Option<StacProviderAuthentication>,
    pub(super) authentication: Option<StacAuthentication>,
}

impl fmt::Debug for StacClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StacClient")
            .field("origin", &self.origin)
            .field(
                "coordinator_authentication",
                &self.coordinator_authentication,
            )
            .field("authenticated", &self.authentication.is_some())
            .finish_non_exhaustive()
    }
}

/// Failures of the STAC transport or its request coordinator.
#[derive(Debug, Snafu)]
pub(crate) enum StacClientError {
    #[snafu(transparent)]
    Http { source: reqwest::Error },
    #[snafu(display("STAC request coordinator stopped before completing the request"))]
    CoordinatorStopped,
}

impl StacClientError {
    /// HTTP status reported by the server, if this failure has one.
    pub(crate) fn status(&self) -> Option<reqwest::StatusCode> {
        match self {
            Self::Http { source } => source.status(),
            Self::CoordinatorStopped => None,
        }
    }
}

impl StacClient {
    /// Creates a client without starting an actor or requiring a Tokio runtime.
    pub(crate) fn new(
        client: reqwest::Client,
        stac_api_url: &str,
        request_registry: StacRequestRegistry,
    ) -> crate::error::Result<Self> {
        let origin = Url::parse(stac_api_url)?;
        Ok(Self {
            client,
            origin,
            request_registry,
            coordinator_authentication: None,
            authentication: None,
        })
    }

    /// Initializes authentication and the configured quota identity for this client.
    pub(crate) async fn with_authentication(
        mut self,
        config: Option<StacProviderAuthentication>,
    ) -> crate::error::Result<Self> {
        self.coordinator_authentication = config.clone();
        self.authentication = match config {
            Some(config) => {
                Some(StacAuthentication::initialize(self.client.clone(), config).await?)
            }
            None => None,
        };
        Ok(self)
    }

    /// Builds a GET request using this provider's HTTP transport.
    pub(crate) fn get(&self, url: impl reqwest::IntoUrl) -> reqwest::RequestBuilder {
        self.client.get(url)
    }

    /// Executes a request through the client's shared channel and authentication.
    /// Pagination URLs are followed as supplied by the STAC server, with
    /// authentication restricted to the configured origin. All failures
    /// share the policy's attempt budget. Retries rejoin FIFO admission after
    /// backoff, unless server quota information controls the retry delay.
    pub(crate) async fn execute_json<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
        policy: &RetryPolicy,
    ) -> Result<T, StacClientError> {
        let request = request.build()?;
        let channels = self
            .request_registry
            .shared(&self.origin, self.coordinator_authentication.as_ref());
        let operation_name = format!("Fetch STAC items from {}", request.url());
        retry_http_with_action(
            || async {
                let reply = match self.execute_attempt(&channels, &request).await {
                    Ok(reply) => reply,
                    Err(error) => return (Err(error), RetryAction::Abort),
                };
                let action = if reply.server_controls_retry {
                    RetryAction::Scheduled
                } else {
                    RetryAction::Backoff
                };
                let result = async { reply.response?.error_for_status()?.json::<T>().await }.await;
                (result.map_err(StacClientError::from), action)
            },
            &operation_name,
            policy,
            |error| error.status().map(|status| status.as_u16()),
        )
        .await
    }

    /// Submit one owned attempt and wait for quota feedback before decoding.
    async fn execute_attempt(
        &self,
        channels: &ActorChannels,
        request: &reqwest::Request,
    ) -> Result<AttemptReply, StacClientError> {
        let request_copy = request
            .try_clone()
            .expect("STAC API searches are replayable GET requests");
        let (reply, receiver) = oneshot::channel();
        let mut pending = PendingAttempt {
            reply: receiver,
            cancellation: Some(channels.cancellations.clone()),
        };
        let job = AttemptJob {
            client: self.client.clone(),
            request: request_copy,
            authentication: self
                .authentication
                .as_ref()
                .filter(|_| self.origin.origin() == request.url().origin())
                .cloned(),
            reply,
        };
        channels
            .jobs
            .send(job)
            .map_err(|_| StacClientError::CoordinatorStopped)?;
        let reply = (&mut pending.reply)
            .await
            .map_err(|_| StacClientError::CoordinatorStopped)?;
        pending.cancellation = None;
        Ok(reply)
    }
}

async fn run_attempt(job: AttemptJob) -> AttemptOutcome {
    let AttemptJob {
        client,
        request,
        authentication,
        mut reply,
    } = job;
    let send = async {
        let mut request = reqwest::RequestBuilder::from_parts(client, request);
        if let Some(authentication) = authentication {
            request = request.bearer_auth(authentication.access_token().await);
        }
        request.send().await
    };
    tokio::select! {
        () = reply.closed() => (reply, None),
        response = send => (reply, Some(response)),
    }
}

async fn run_actor<F>(
    mut receiver: mpsc::UnboundedReceiver<AttemptJob>,
    mut cancellations: mpsc::UnboundedReceiver<()>,
    coordinator: Weak<ActorChannels>,
    dispatch: F,
) where
    F: Fn(AttemptJob) -> RunningAttempt + Send + 'static,
{
    let now = Instant::now();
    let mut state = State {
        in_flight_requests: 0,
        completed_requests: 0,
        windows: Vec::new(),
        starts: VecDeque::new(),
        unscoped_quota: None,
        cooldown_until: None,
        last_activity: now,
    };
    let mut queued: VecDeque<AttemptJob> = VecDeque::new();
    let mut running = FuturesUnordered::new();
    let mut jobs_open = true;
    let mut cancellations_open = true;

    loop {
        while let Some(Some((completed_before, outcome))) = running.next().now_or_never() {
            complete_attempt(&mut state, completed_before, outcome);
        }

        let mut deadline = None;
        if let Some(job) = queued.pop_front() {
            if job.reply.is_closed() {
                continue;
            } else if let Some(until) = state.reserve(Instant::now()) {
                deadline = Some(until);
                queued.push_front(job);
            } else {
                let completed_before = state.completed_requests;
                let attempt = dispatch(job);
                running.push(async move { (completed_before, attempt.await) });
                continue;
            }
        }

        if !jobs_open && queued.is_empty() && running.is_empty() {
            return;
        }
        let idle_deadline = (queued.is_empty() && running.is_empty()).then(|| {
            let now = Instant::now();
            let deadline = state.retain_until();
            if deadline <= now && coordinator.strong_count() > 1 {
                now + MIN_RETENTION
            } else {
                deadline
            }
        });
        tokio::select! {
            biased;
            outcome = running.next(), if !running.is_empty() => {
                if let Some((completed_before, outcome)) = outcome {
                    complete_attempt(&mut state, completed_before, outcome);
                }
            },
            cancellation = cancellations.recv(), if cancellations_open => match cancellation {
                Some(()) => queued.retain(|job| !job.reply.is_closed()),
                None => cancellations_open = false,
            },
            job = receiver.recv(), if jobs_open => match job {
                Some(job) => queued.push_back(job),
                None => jobs_open = false,
            },
            () = async { if let Some(deadline) = deadline { tokio::time::sleep_until(deadline.into()).await } }, if deadline.is_some() => {},
            () = async { if let Some(deadline) = idle_deadline { tokio::time::sleep_until(deadline.into()).await } }, if idle_deadline.is_some() => {
                if state.retain_until() <= Instant::now() && queued.is_empty() && running.is_empty()
                    && ActorChannels::try_retire(&coordinator) {
                    return;
                }
            },
        }
    }
}

fn complete_attempt(state: &mut State, completed_before: usize, (reply, response): AttemptOutcome) {
    state.release();
    if let Some(response) = response {
        let feedback = response.as_ref().ok().map(|response| {
            ResponseFeedback::parse(
                response.headers(),
                response.status().as_u16(),
                Instant::now(),
            )
        });
        if let Some(feedback) = feedback {
            state.observe(feedback, Instant::now(), completed_before);
        }
        let server_controls_retry = response.as_ref().is_ok_and(|response| {
            response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
                && (rate_limit_delay(response.headers()).is_some() || !state.windows.is_empty())
        });
        let _ = reply.send(AttemptReply {
            response,
            server_controls_retry,
        });
    }
}

/// Canonicalize valid OAuth endpoint URLs for registry matching, preserving the
/// original text when parsing fails.
fn normalize_endpoint(endpoint: &str) -> String {
    Url::parse(endpoint).map_or_else(|_| endpoint.to_owned(), |url| url.to_string())
}

/// Scheduling constraints extracted from one HTTP response by the actor.
///
/// Known policies carry their associated remaining/reset quota inside [`Window`].
/// Ambiguous quota feedback is kept separately, and a 429 contributes a shared
/// cooldown. [`State::observe`] merges these observations with existing constraints
/// and accounts for other requests that were active or admitted since dispatch.
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
                remaining_requests: available,
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
    use std::sync::Mutex;

    fn quota_headers(limit: &str, remaining: &str, reset: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-ratelimit-limit", limit.parse().unwrap());
        headers.insert("x-ratelimit-remaining", remaining.parse().unwrap());
        headers.insert("x-ratelimit-reset", reset.parse().unwrap());
        headers
    }

    #[test]
    fn it_parses_all_advertised_windows_and_relative_reset() {
        let mut headers = HeaderMap::new();
        headers.insert("x-ratelimit-limit", "10, 10;w=1, 60;w=60".parse().unwrap());
        headers.insert("x-ratelimit-reset", "1".parse().unwrap());
        let feedback = ResponseFeedback::parse(&headers, 200, Instant::now());
        assert_eq!(feedback.windows.len(), 2);
        assert_eq!(feedback.windows[0].limit, 10);
        assert_eq!(feedback.windows[0].duration, Duration::from_secs(1));
        assert_eq!(feedback.windows[1].limit, 60);
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
    fn it_rejects_unusable_retry_timing_and_uses_the_longest_valid_delay() {
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
    fn it_deduplicates_windows_and_keeps_the_stricter_duplicate() {
        let headers = quota_headers(
            "20, 20;w=1, 20;w=1, 1800;w=60, 15;w=1, 100000;w=172800",
            "19",
            "1",
        );
        let feedback = ResponseFeedback::parse(&headers, 200, Instant::now());
        assert_eq!(feedback.windows.len(), 3);
        assert_eq!(feedback.windows[0].limit, 15);
        assert_eq!(feedback.windows[2].limit, 100_000);
        assert_eq!(feedback.windows[2].duration, Duration::from_hours(48));
    }

    #[test]
    fn it_state_reserves_fallback_capacity_and_does_not_refund_failed_starts() {
        let now = Instant::now();
        let mut state = State {
            in_flight_requests: 0,
            completed_requests: 0,
            windows: Vec::new(),
            starts: VecDeque::new(),
            unscoped_quota: None,
            cooldown_until: None,
            last_activity: now,
        };
        for _ in 0..10 {
            assert_eq!(state.reserve(Instant::now()), None);
        }
        assert!(state.reserve(Instant::now()).is_some());
        assert_eq!(state.starts.len(), 10);
        state.release();
        assert_eq!(state.starts.len(), 10);
        assert_eq!(state.in_flight_requests, 9);
    }

    #[test]
    fn it_state_applies_feedback_before_admitting_the_next_attempt() {
        let now = Instant::now();
        let mut state = State {
            in_flight_requests: 0,
            completed_requests: 0,
            windows: Vec::new(),
            starts: VecDeque::new(),
            unscoped_quota: None,
            cooldown_until: None,
            last_activity: now,
        };
        assert_eq!(state.reserve(now), None);
        let headers = quota_headers("2;w=60", "0", "60");
        state.release();
        state.observe(
            ResponseFeedback::parse(&headers, 429, Instant::now()),
            Instant::now(),
            0,
        );
        assert!(state.reserve(Instant::now()).is_some());
        assert_eq!(state.windows[0].limit, 2);
        assert_eq!(
            state.windows[0]
                .remote
                .as_ref()
                .map(|quota| quota.remaining_requests),
            Some(0)
        );
        assert!(state.cooldown_until.is_some());
    }

    #[tokio::test]
    async fn it_coordinators_share_by_normalized_origin_and_auth_identity() {
        let registry = StacRequestRegistry::new();
        let credentials = super::super::StacProviderAuthentication {
            endpoint: "https://auth.example/token/".to_owned(),
            client_id: "client".to_owned(),
            username: "user".to_owned(),
            password: "secret-value-must-not-be-debugged".to_owned(),
        };
        let first = registry.shared(
            &Url::parse("https://STAC.example:443/api").unwrap(),
            Some(&credentials),
        );
        let same = registry.shared(
            &Url::parse("https://stac.example/other").unwrap(),
            Some(&credentials),
        );
        let anonymous = registry.shared(&Url::parse("https://stac.example").unwrap(), None);
        assert!(Arc::ptr_eq(&first, &same));
        assert!(!Arc::ptr_eq(&first, &anonymous));
        assert!(!format!("{first:?}").contains("secret-value-must-not-be-debugged"));
        tokio::task::yield_now().await;
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
        let registry = StacRequestRegistry::new();
        let client =
            StacClient::new(reqwest::Client::new(), &server.url_str("/"), registry).unwrap();
        let request = client.get(server.url_str("/items"));
        let result: Value = client
            .execute_json(
                request,
                &RetryPolicy::new().max_retries(4).initial_delay_ms(1),
            )
            .await
            .unwrap();
        assert_eq!(result, json!({"complete": true}));
    }

    #[tokio::test]
    async fn it_routes_parallel_results_to_their_own_callers() {
        use httptest::{Expectation, Server, matchers::request, responders};
        use serde_json::Value;
        let server = Server::run();
        for (path, body) in [("/one", r#"{"id":1}"#), ("/two", r#"{"id":2}"#)] {
            server.expect(
                Expectation::matching(request::method_path("GET", path))
                    .times(1)
                    .respond_with(responders::status_code(200).body(body)),
            );
        }
        let registry = StacRequestRegistry::new();
        let client =
            StacClient::new(reqwest::Client::new(), &server.url_str("/"), registry).unwrap();
        let mut tasks = tokio::task::JoinSet::new();
        for path in ["/one", "/two"] {
            let client = client.clone();
            let url = server.url_str(path);
            tasks.spawn(async move {
                client
                    .execute_json::<Value>(client.get(url), &RetryPolicy::new())
                    .await
                    .unwrap()
            });
        }
        let mut values = Vec::new();
        while let Some(value) = tasks.join_next().await {
            values.push(value.unwrap());
        }
        assert!(values.contains(&serde_json::json!({"id": 1})));
        assert!(values.contains(&serde_json::json!({"id": 2})));
    }

    #[tokio::test]
    async fn it_cancels_an_inflight_attempt_and_allows_the_next_job() {
        use httptest::{Expectation, Server, matchers::request, responders};
        use serde_json::Value;
        let server = Server::run();
        let starts = Arc::new(Mutex::new(0));
        let response_starts = starts.clone();
        server.expect(
            Expectation::matching(request::method_path("GET", "/items"))
                .times(2)
                .respond_with(move || {
                    let mut starts = response_starts.lock().unwrap();
                    *starts += 1;
                    let (delay, body) = if *starts == 1 {
                        (
                            Duration::from_secs(10),
                            serde_json::json!({"discarded": true}),
                        )
                    } else {
                        (Duration::ZERO, serde_json::json!({"ok": true}))
                    };
                    responders::delay_and_then(delay, responders::json_encoded(body))
                }),
        );
        let registry = StacRequestRegistry::new();
        let client =
            StacClient::new(reqwest::Client::new(), &server.url_str("/"), registry).unwrap();
        let first_client = client.clone();
        let first_url = server.url_str("/items");
        let first = tokio::spawn(async move {
            first_client
                .execute_json::<Value>(first_client.get(first_url), &RetryPolicy::new())
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while *starts.lock().unwrap() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first request should reach the server before cancellation");
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            client.execute_json::<Value>(client.get(server.url_str("/items")), &RetryPolicy::new()),
        )
        .await
        .expect("the next caller should proceed after cancellation")
        .unwrap();
        assert_eq!(result, serde_json::json!({"ok": true}));
    }

    #[tokio::test]
    async fn it_returns_terminal_http_errors() {
        use httptest::{Expectation, Server, matchers::request, responders};
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/missing"))
                .times(1)
                .respond_with(responders::status_code(404)),
        );
        let registry = StacRequestRegistry::new();
        let client =
            StacClient::new(reqwest::Client::new(), &server.url_str("/"), registry).unwrap();
        let error = client
            .execute_json::<serde_json::Value>(
                client.get(server.url_str("/missing")),
                &RetryPolicy::new().stop_on_status(&[400, 404]),
            )
            .await
            .unwrap_err();
        assert_eq!(error.status(), Some(reqwest::StatusCode::NOT_FOUND));
    }
}

#[cfg(test)]
#[path = "coordinator_state_tests.rs"]
mod state_tests;

#[cfg(test)]
#[path = "coordinator_actor_tests.rs"]
mod actor_tests;

#[cfg(test)]
#[path = "coordinator_retry_tests.rs"]
mod retry_tests;

#[cfg(test)]
#[path = "coordinator_registry_tests.rs"]
mod registry_tests;
