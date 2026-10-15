//! Process-wide FIFO admission and shared rate-limit feedback for STAC requests.
use super::auth::StacAuthentication;
use crate::util::retry::{RetryPolicy, retry_http};
use chrono::{DateTime, Utc};
use reqwest::header::{HeaderMap, RETRY_AFTER};
use serde::de::DeserializeOwned;
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::Notify;
use url::Url;

const FALLBACK_429: Duration = Duration::from_secs(1);
const MAX_RETRY_AFTER: Duration = Duration::from_mins(5);
const MAX_RETRIES_429: usize = crate::util::retry::DEFAULT_MAX_RETRIES as usize - 1;
const MAX_WINDOW: Duration = Duration::from_hours(24);
const MAX_WINDOW_LIMIT: usize = 10_000;
const DEFAULT_WINDOW: Window = Window {
    limit: 10,
    duration: Duration::from_secs(1),
    starts: VecDeque::new(),
};
const MIN_RETENTION: Duration = Duration::from_mins(5);

#[derive(Clone, Eq)]
pub(super) struct Identity {
    scheme: String,
    host: String,
    port: u16,
    token_endpoint: Option<String>,
    client_id: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

impl PartialEq for Identity {
    fn eq(&self, other: &Self) -> bool {
        self.scheme == other.scheme
            && self.host == other.host
            && self.port == other.port
            && self.token_endpoint == other.token_endpoint
            && self.client_id == other.client_id
            && self.username == other.username
            && self.password == other.password
    }
}

impl Hash for Identity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.scheme.hash(state);
        self.host.hash(state);
        self.port.hash(state);
        self.token_endpoint.hash(state);
        self.client_id.hash(state);
        self.username.hash(state);
        self.password.hash(state);
    }
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

#[derive(Clone, Debug)]
struct Window {
    limit: usize,
    duration: Duration,
    starts: VecDeque<Instant>,
}

#[derive(Debug)]
struct State {
    queue: VecDeque<u64>,
    next_ticket: u64,
    next_dispatch: u64,
    in_flight: usize,
    windows: Vec<Window>,
    bootstrap_starts: VecDeque<Instant>,
    outstanding: std::collections::HashSet<u64>,
    bootstrap_complete: bool,
    cooldown_until: Option<Instant>,
    remote_remaining: Option<usize>,
    remote_reset: Option<Instant>,
    remote_observation: u64,
}

#[derive(Debug)]
pub(super) struct StacRequestCoordinator {
    identity: Identity,
    state: Mutex<State>,
    changed: Notify,
    retention_secs: AtomicU64,
}

struct RegistryEntry {
    coordinator: Arc<StacRequestCoordinator>,
    retain_until: Instant,
}

static REGISTRY: OnceLock<Mutex<HashMap<Identity, RegistryEntry>>> = OnceLock::new();

impl StacRequestCoordinator {
    /// Each server has one anonymous bucket and one bucket for each configured
    /// authentication identity. Provider/collection ids and access tokens do not
    /// create additional buckets.
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
            .expect("STAC coordinator registry poisoned");
        let now = Instant::now();
        registry.retain(|_, entry| {
            entry.retain_until > now || Arc::strong_count(&entry.coordinator) > 1
        });
        if let Some(entry) = registry.get_mut(&identity) {
            entry.retain_until =
                now + Duration::from_secs(entry.coordinator.retention_secs.load(Ordering::Relaxed));
            let coordinator = entry.coordinator.clone();
            return coordinator;
        }
        let coordinator = Arc::new(Self {
            identity: identity.clone(),
            state: Mutex::new(State {
                queue: VecDeque::new(),
                next_ticket: 0,
                next_dispatch: 0,
                in_flight: 0,
                windows: Vec::new(),
                bootstrap_starts: VecDeque::new(),
                outstanding: std::collections::HashSet::new(),
                bootstrap_complete: false,
                cooldown_until: None,
                remote_remaining: None,
                remote_reset: None,
                remote_observation: 0,
            }),
            changed: Notify::new(),
            retention_secs: AtomicU64::new(MIN_RETENTION.as_secs()),
        });
        registry.insert(
            identity,
            RegistryEntry {
                coordinator: coordinator.clone(),
                retain_until: now + MIN_RETENTION,
            },
        );
        coordinator
    }

    /// Executes and decodes a STAC request, owning admission, header feedback and
    /// retries. Tokens are read after admission, and each retry enters the queue.
    pub(super) async fn execute<T: DeserializeOwned>(
        self: &Arc<Self>,
        client: &reqwest::Client,
        request: reqwest::Request,
        authentication: Option<&StacAuthentication>,
        policy: &RetryPolicy,
    ) -> reqwest::Result<T> {
        // The rate-limit loop has already exhausted its attempts when it returns
        // a 429. Never start an exponential retry cycle for that final response.
        let policy = policy.clone().stop_on_status(&[429]);
        retry_http(
            || async {
                self.execute_rate_limited(client, &request, authentication)
                    .await?
                    .error_for_status()?
                    .json::<T>()
                    .await
            },
            &format!("Fetch STAC items from {}", request.url()),
            &policy,
            |error| error.status().map(|status| status.as_u16()),
        )
        .await
    }

    async fn execute_rate_limited(
        self: &Arc<Self>,
        client: &reqwest::Client,
        request: &reqwest::Request,
        authentication: Option<&StacAuthentication>,
    ) -> reqwest::Result<reqwest::Response> {
        for attempt in 0..=MAX_RETRIES_429 {
            let permit = self.acquire().await;
            let request_copy = request
                .try_clone()
                .expect("STAC API searches are replayable GET requests");
            let mut request_builder =
                reqwest::RequestBuilder::from_parts(client.clone(), request_copy);
            if let Some(authentication) = authentication {
                let token = authentication.access_token().await;
                request_builder = request_builder.bearer_auth(token);
            }
            let response = request_builder.send().await?;
            let status = response.status().as_u16();
            permit.observe(response.headers(), status);
            if status != 429 || attempt == MAX_RETRIES_429 {
                return Ok(response);
            }
        }
        unreachable!("bounded STAC 429 retry loop always returns")
    }

    async fn acquire(self: &Arc<Self>) -> Permit {
        let ticket = {
            let mut state = self.state.lock().expect("STAC coordinator state poisoned");
            let ticket = state.next_ticket;
            state.next_ticket = state.next_ticket.wrapping_add(1);
            state.queue.push_back(ticket);
            ticket
        };
        let mut queued = QueuedTicket {
            coordinator: self.clone(),
            ticket,
            active: true,
        };
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let wait = {
                let mut state = self.state.lock().expect("STAC coordinator state poisoned");
                let now = Instant::now();
                prune_history(&mut state, now);
                let at_front = state.queue.front() == Some(&ticket);
                let cooldown = state.cooldown_until.filter(|until| *until > now);
                let bootstrap_wait = !state.bootstrap_complete && state.in_flight > 0;
                let next_window = state
                    .windows
                    .iter()
                    .filter_map(|window| {
                        (window.starts.len() >= window.limit)
                            .then(|| {
                                window
                                    .starts
                                    .front()
                                    .and_then(|at| at.checked_add(window.duration))
                            })
                            .flatten()
                    })
                    .min();
                let remote_wait = match (state.remote_remaining, state.remote_reset) {
                    (Some(0), Some(reset)) if reset > now => Some(reset),
                    _ => None,
                };
                if at_front
                    && cooldown.is_none()
                    && !bootstrap_wait
                    && next_window.is_none()
                    && remote_wait.is_none()
                {
                    state.queue.pop_front();
                    queued.active = false;
                    state.next_dispatch = state.next_dispatch.wrapping_add(1);
                    let dispatch = state.next_dispatch;
                    state.in_flight += 1;
                    state.outstanding.insert(dispatch);
                    if !state.bootstrap_complete {
                        state.bootstrap_starts.push_back(now);
                    }
                    for window in &mut state.windows {
                        window.starts.push_back(now);
                    }
                    if let Some(remaining) = &mut state.remote_remaining {
                        *remaining = remaining.saturating_sub(1);
                    }
                    if state.remote_remaining == Some(0)
                        && state.remote_reset.is_none_or(|deadline| deadline <= now)
                    {
                        state.remote_reset = now.checked_add(FALLBACK_429);
                    }
                    (dispatch, None)
                } else {
                    let deadline = [cooldown, next_window, remote_wait]
                        .into_iter()
                        .flatten()
                        .min();
                    (0, deadline)
                }
            };
            let (dispatch, deadline) = wait;
            if dispatch != 0 {
                self.notify();
                refresh_registry(self);
                return Permit {
                    coordinator: self.clone(),
                    dispatch,
                    completed: false,
                };
            }
            tokio::select! {
                () = notified => {},
                () = async { if let Some(deadline) = deadline { tokio::time::sleep_until(deadline.into()).await } else { std::future::pending::<()>().await } } => {},
            }
        }
    }

    fn notify(&self) {
        self.changed.notify_waiters();
    }
}

struct QueuedTicket {
    coordinator: Arc<StacRequestCoordinator>,
    ticket: u64,
    active: bool,
}

impl Drop for QueuedTicket {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let coordinator = self.coordinator.clone();
        let ticket = self.ticket;
        let mut state = coordinator
            .state
            .lock()
            .expect("STAC coordinator state poisoned");
        if let Some(index) = state.queue.iter().position(|queued| *queued == ticket) {
            state.queue.remove(index);
        }
        drop(state);
        coordinator.notify();
    }
}

struct Permit {
    coordinator: Arc<StacRequestCoordinator>,
    dispatch: u64,
    completed: bool,
}

impl Permit {
    fn observe(mut self, headers: &HeaderMap, status: u16) {
        let retry_after = retry_after(headers);
        let reset = header_seconds(headers, "x-ratelimit-reset");
        let limits = parse_limits(headers);
        let remaining = headers
            .get("x-ratelimit-remaining")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<usize>().ok());
        let mut state = self
            .coordinator
            .state
            .lock()
            .expect("STAC coordinator state poisoned");
        let now = Instant::now();
        prune_history(&mut state, now);
        update_windows(&mut state, limits, now);
        state.outstanding.remove(&self.dispatch);
        if let Some(remaining) = remaining.filter(|_| self.dispatch >= state.remote_observation) {
            state.remote_observation = self.dispatch;
            let observed_available = remaining.saturating_sub(state.outstanding.len());
            state.remote_remaining = Some(
                state
                    .remote_remaining
                    .map_or(observed_available, |current| {
                        current.min(observed_available)
                    }),
            );
            if let Some(reset) = reset {
                state.remote_reset = now.checked_add(reset);
            } else if observed_available == 0
                && state.remote_reset.is_none_or(|deadline| deadline <= now)
            {
                state.remote_reset = now.checked_add(FALLBACK_429);
            }
        }
        if status == 429 {
            let advertised_delay = retry_after
                .into_iter()
                .chain(reset)
                .max()
                .unwrap_or_default();
            let delay = if advertised_delay.is_zero() {
                FALLBACK_429
            } else {
                advertised_delay.min(MAX_RETRY_AFTER)
            };
            state.cooldown_until = Some(
                state
                    .cooldown_until
                    .unwrap_or_else(Instant::now)
                    .max(now.checked_add(delay).unwrap_or(now + MAX_RETRY_AFTER)),
            );
            if state.remote_remaining.is_none() {
                state.bootstrap_complete = true;
            }
        }
        let retention = state
            .windows
            .iter()
            .map(|window| window.duration.as_secs())
            .max()
            .unwrap_or(0)
            .max(MIN_RETENTION.as_secs());
        self.coordinator
            .retention_secs
            .fetch_max(retention, Ordering::Relaxed);
        state.in_flight = state.in_flight.saturating_sub(1);
        self.completed = true;
        drop(state);
        refresh_registry(&self.coordinator);
        self.coordinator.notify();
    }
}

fn update_windows(state: &mut State, limits: Vec<Window>, now: Instant) {
    if !limits.is_empty() {
        // Keep every pre-discovery attempt, including attempts whose
        // transport failed before headers arrived. Newly advertised windows
        // inherit the longest retained local history.
        let history = state
            .windows
            .iter()
            .max_by_key(|window| window.duration)
            .map_or_else(
                || state.bootstrap_starts.clone(),
                |window| window.starts.clone(),
            );
        let mut updated = limits;
        for window in &mut updated {
            if let Some(previous) = state
                .windows
                .iter_mut()
                .find(|previous| previous.duration == window.duration)
            {
                window.starts = std::mem::take(&mut previous.starts);
            } else {
                window.starts = history
                    .iter()
                    .copied()
                    .filter(|started| now.duration_since(*started) < window.duration)
                    .collect();
            }
        }
        state.windows = updated;
    }
    if !state.bootstrap_complete && state.windows.is_empty() {
        // Headers can be absent on otherwise healthy STAC servers. Keep
        // useful concurrency while limiting an uninformed burst.
        let mut window = DEFAULT_WINDOW;
        window.starts = state
            .bootstrap_starts
            .iter()
            .copied()
            .filter(|started| now.duration_since(*started) < window.duration)
            .collect();
        state.windows.push(window);
    }
    state.bootstrap_starts.clear();
    state.bootstrap_complete = true;
}

fn refresh_registry(coordinator: &StacRequestCoordinator) {
    if let Some(registry) = REGISTRY.get()
        && let Ok(mut registry) = registry.lock()
    {
        let retain_until = Instant::now()
            + Duration::from_secs(coordinator.retention_secs.load(Ordering::Relaxed));
        if let Some(entry) = registry.get_mut(&coordinator.identity) {
            entry.retain_until = retain_until;
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if !self.completed {
            let coordinator = self.coordinator.clone();
            let dispatch = self.dispatch;
            // Request cancellation must release an admitted slot synchronously,
            // including when a runtime is shutting down.
            let mut state = coordinator
                .state
                .lock()
                .expect("STAC coordinator state poisoned");
            state.in_flight = state.in_flight.saturating_sub(1);
            state.outstanding.remove(&dispatch);
            drop(state);
            refresh_registry(&coordinator);
            coordinator.notify();
        }
    }
}

fn normalize_endpoint(endpoint: &str) -> String {
    Url::parse(endpoint).map_or_else(|_| endpoint.to_owned(), |url| url.to_string())
}

fn prune_history(state: &mut State, now: Instant) {
    for window in &mut state.windows {
        while window
            .starts
            .front()
            .is_some_and(|at| now.duration_since(*at) >= window.duration)
        {
            window.starts.pop_front();
        }
    }
    if state.cooldown_until.is_some_and(|until| until <= now) {
        state.cooldown_until = None;
    }
    if state.remote_reset.is_some_and(|reset| reset <= now) {
        state.remote_remaining = None;
        state.remote_reset = None;
    }
}

fn parse_limits(headers: &HeaderMap) -> Vec<Window> {
    let Some(value) = headers
        .get("x-ratelimit-limit")
        .and_then(|v| v.to_str().ok())
    else {
        return Vec::new();
    };
    value
        .split(',')
        .filter_map(|item| {
            let mut fields = item.trim().split(';');
            let limit = fields.next()?.trim().parse::<usize>().ok()?;
            let duration = fields.find_map(|field| {
                field
                    .trim()
                    .strip_prefix("w=")
                    .and_then(|w| w.parse::<u64>().ok())
            })?;
            (limit > 0 && duration > 0).then_some(Window {
                limit: limit.min(MAX_WINDOW_LIMIT),
                duration: Duration::from_secs(duration).min(MAX_WINDOW),
                starts: VecDeque::new(),
            })
        })
        .collect()
}

fn header_seconds(headers: &HeaderMap, name: &str) -> Option<Duration> {
    headers
        .get(name)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(|seconds| Duration::from_secs(seconds).min(MAX_WINDOW))
}

fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds).min(MAX_RETRY_AFTER));
    }
    let date = DateTime::parse_from_rfc2822(value)
        .ok()?
        .with_timezone(&Utc);
    let now = DateTime::<Utc>::from(SystemTime::now());
    (date > now)
        .then(|| (date - now).to_std().ok())
        .flatten()
        .map(|duration| duration.min(MAX_RETRY_AFTER))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_execute_json_owns_http_and_decode_retries() {
        use httptest::{Expectation, Server, matchers::request, responders};
        use serde_json::{Value, json};

        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/items"))
                .times(3)
                .respond_with(responders::cycle![
                    responders::status_code(500),
                    responders::status_code(200).body("invalid json"),
                    responders::json_encoded(json!({"complete": true})),
                ]),
        );
        let coordinator =
            StacRequestCoordinator::shared(&Url::parse(&server.url_str("/")).unwrap(), None);
        let client = reqwest::Client::new();
        let request = client.get(server.url_str("/items")).build().unwrap();
        let policy = RetryPolicy::new().max_retries(3).initial_delay_ms(1);
        let result: Value = coordinator
            .execute(&client, request, None, &policy)
            .await
            .expect("the coordinator retries HTTP and decode failures internally");
        assert_eq!(result, json!({"complete": true}));
        let state = coordinator.state.lock().unwrap();
        assert_eq!(state.next_dispatch, 3);
        assert_eq!(state.in_flight, 0);
        assert!(state.outstanding.is_empty());
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
            header_seconds(&headers, "x-ratelimit-reset"),
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
            state.bootstrap_complete = true;
            state.windows.push(Window {
                limit: 1,
                duration: Duration::from_mins(1),
                starts: VecDeque::from([Instant::now()]),
            });
        }

        let first_coordinator = coordinator.clone();
        let mut first = tokio::spawn(async move { first_coordinator.acquire().await.dispatch });
        wait_for_queue(&coordinator, 1).await;
        let second_coordinator = coordinator.clone();
        let mut second = tokio::spawn(async move { second_coordinator.acquire().await.dispatch });
        wait_for_queue(&coordinator, 2).await;
        assert!(!first.is_finished());

        coordinator.state.lock().unwrap().windows[0].starts.clear();
        coordinator.notify();
        let first_dispatch = tokio::time::timeout(Duration::from_millis(200), &mut first)
            .await
            .expect("first queued request should be admitted")
            .unwrap();

        // The first admission fills the one request window again. Open it
        // explicitly to let the second waiter proceed without a one minute test.
        coordinator.state.lock().unwrap().windows[0].starts.clear();
        coordinator.notify();
        let second_dispatch = tokio::time::timeout(Duration::from_millis(200), &mut second)
            .await
            .expect("second queued request should be admitted")
            .unwrap();
        assert!(first_dispatch < second_dispatch);
    }

    #[tokio::test]
    async fn it_cancelling_a_queued_attempt_does_not_block_later_requests() {
        let coordinator = isolated_coordinator("cancel");
        let first = coordinator.acquire().await;
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
            state.bootstrap_complete = true;
            let now = Instant::now();
            state.windows.push(Window {
                limit: 10,
                duration: Duration::from_secs(1),
                starts: VecDeque::from([now.checked_sub(Duration::from_secs(2)).unwrap()]),
            });
            state.windows.push(Window {
                limit: 60,
                duration: Duration::from_mins(1),
                starts: std::iter::repeat_n(now.checked_sub(Duration::from_secs(30)).unwrap(), 60)
                    .collect(),
            });
        }

        let mut waiting = Box::pin(coordinator.acquire());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut waiting)
                .await
                .is_err()
        );
        coordinator.state.lock().unwrap().windows[1].starts.clear();
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
        newer_headers.insert("x-ratelimit-remaining", "8".parse().unwrap());
        newer_headers.insert("x-ratelimit-reset", "60".parse().unwrap());
        newer.observe(&newer_headers, 200);
        assert_eq!(coordinator.state.lock().unwrap().remote_remaining, Some(7));

        let mut stale_headers = HeaderMap::new();
        stale_headers.insert("x-ratelimit-remaining", "9".parse().unwrap());
        stale_headers.insert("x-ratelimit-reset", "60".parse().unwrap());
        older.observe(&stale_headers, 200);
        assert_eq!(coordinator.state.lock().unwrap().remote_remaining, Some(7));
    }

    #[tokio::test]
    async fn it_counts_bootstrap_attempts_after_transport_failure() {
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
            "both bootstrap attempts reserve window capacity"
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
        assert_eq!(coordinator.state.lock().unwrap().remote_remaining, Some(0));
        assert!(
            coordinator
                .state
                .lock()
                .unwrap()
                .remote_reset
                .is_some_and(|deadline| deadline > Instant::now())
        );
        drop(second);
    }
}
