//! Quota regressions retained from the original coordinator implementation.
use super::*;

fn state(now: Instant) -> State {
    State {
        in_flight_requests: 0,
        completed_requests: 0,
        windows: Vec::new(),
        starts: VecDeque::new(),
        unscoped_quota: None,
        cooldown_until: None,
        last_activity: now,
    }
}

fn observe(state: &mut State, headers: &HeaderMap, status: u16, now: Instant) {
    let completed_before = state.completed_requests;
    state.release();
    state.observe(
        ResponseFeedback::parse(headers, status, now),
        now,
        completed_before,
    );
}

fn parse_limits(headers: &HeaderMap) -> Vec<Window> {
    ResponseFeedback::parse(headers, 200, Instant::now()).windows
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

#[test]
fn it_does_not_assign_ambiguous_feedback_to_the_wrong_window() {
    let mut headers = quota_headers(&[("20;w=1", "19", "1"), ("1800;w=60", "1799", "50")]);
    headers.insert("x-ratelimit-remaining", "0".parse().unwrap());
    let now = Instant::now();
    let feedback = ResponseFeedback::parse(&headers, 200, now);
    let quota = feedback.unscoped_quota.unwrap();
    assert_eq!(quota.remaining_requests, 0);
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
    assert_eq!(feedback.unscoped_quota.unwrap().remaining_requests, 0);
    assert_eq!(feedback.windows.len(), 1);
    assert_eq!(feedback.windows[0].duration, Duration::from_mins(1));
    assert_eq!(
        feedback.windows[0]
            .remote
            .as_ref()
            .unwrap()
            .remaining_requests,
        1799
    );
}

#[test]
fn it_outstanding_requests_reduce_remaining_and_stale_feedback_cannot_refill_it() {
    let now = Instant::now();
    let mut state = state(now);
    assert_eq!(state.reserve(now), None);
    observe(
        &mut state,
        &quota_headers(&[("10;w=60", "9", "60")]),
        200,
        now,
    );
    assert_eq!(state.reserve(now), None);
    assert_eq!(state.reserve(now), None);
    observe(
        &mut state,
        &quota_headers(&[("10;w=60", "8", "60")]),
        200,
        now,
    );
    assert_eq!(state.in_flight_requests, 1);
    assert_eq!(
        state.windows[0].remote.as_ref().unwrap().remaining_requests,
        7
    );
    observe(
        &mut state,
        &quota_headers(&[("10;w=60", "9", "60")]),
        200,
        now,
    );
    assert_eq!(
        state.windows[0].remote.as_ref().unwrap().remaining_requests,
        7
    );
    assert_eq!(state.reserve(now), None);
    observe(
        &mut state,
        &quota_headers(&[("10;w=60", "0", "60")]),
        200,
        now,
    );
    assert_eq!(state.reserve(now), Some(now + Duration::from_mins(1)));
}

#[test]
fn it_preserves_repeated_and_alternating_policies() {
    let now = Instant::now();
    let mut state = state(now);
    let minute = ("1800, 1800;w=60, 1800;w=60", "1799", "50");
    let second = ("20, 20;w=1, 20;w=1", "19", "1");
    assert_eq!(state.reserve(now), None);
    observe(&mut state, &quota_headers(&[minute, second]), 200, now);
    assert_eq!(state.windows.len(), 2);
    for (duration, limit, remaining) in [(60, 1800, 1799), (1, 20, 19)] {
        let window = state
            .windows
            .iter()
            .find(|w| w.duration == Duration::from_secs(duration))
            .unwrap();
        assert_eq!(window.limit, limit);
        assert_eq!(
            window.remote.as_ref().unwrap().remaining_requests,
            remaining
        );
    }
    assert_eq!(state.reserve(now), None);
    observe(&mut state, &quota_headers(&[second]), 200, now);
    assert_eq!(state.reserve(now), None);
    observe(&mut state, &quota_headers(&[minute]), 200, now);
    assert_eq!(state.windows.len(), 2);
    assert_eq!(state.starts.len(), 3);
}

#[test]
fn it_waits_for_the_latest_binding_constraint() {
    let now = Instant::now();
    let mut state = state(now);
    assert_eq!(state.reserve(now), None);
    observe(
        &mut state,
        &quota_headers(&[
            ("2;w=1", "1", "1"),
            ("7;w=37", "6", "37"),
            ("90;w=3600", "89", "3600"),
        ]),
        200,
        now,
    );
    state.windows[0].remote.as_mut().unwrap().remaining_requests = 0;
    state.windows[1].remote.as_mut().unwrap().remaining_requests = 0;
    assert_eq!(state.reserve(now), Some(now + Duration::from_secs(37)));
    assert_eq!(state.reserve(now + Duration::from_secs(37)), None);
}

#[test]
fn it_enforces_the_long_window_after_the_short_window_opens() {
    let now = Instant::now();
    let mut state = state(now);
    state.windows = vec![
        Window::new(10, Duration::from_secs(1)),
        Window::new(60, Duration::from_mins(1)),
    ];
    let start = now.checked_sub(Duration::from_secs(30)).unwrap();
    state.starts = std::iter::repeat_n(start, 60).collect();
    assert_eq!(state.reserve(now), Some(start + Duration::from_mins(1)));
    assert_eq!(state.reserve(now + Duration::from_secs(30)), None);
}

#[test]
fn it_keeps_failed_and_cancelled_starts_charged() {
    let now = Instant::now();
    let mut state = state(now);
    assert_eq!(state.reserve(now), None);
    state.release();
    assert_eq!(state.reserve(now), None);
    observe(
        &mut state,
        &quota_headers(&[("2;w=60", "0", "60")]),
        200,
        now,
    );
    assert_eq!(state.in_flight_requests, 0);
    assert_eq!(state.starts.len(), 2);
    assert_eq!(state.reserve(now), Some(now + Duration::from_mins(1)));
}

#[test]
fn it_allows_the_advertised_parallel_capacity_without_refunding_completions() {
    let now = Instant::now();
    let mut state = state(now);
    state.observe(
        ResponseFeedback::parse(&quota_headers(&[("20;w=1", "20", "1")]), 200, now),
        now,
        state.completed_requests,
    );
    for _ in 0..20 {
        assert_eq!(state.reserve(now), None);
    }
    assert_eq!(state.in_flight_requests, 20);
    assert_eq!(state.reserve(now), Some(now + Duration::from_secs(1)));
    for _ in 0..20 {
        state.release();
    }
    assert_eq!(state.in_flight_requests, 0);
    assert_eq!(state.reserve(now), Some(now + Duration::from_secs(1)));
    assert_eq!(state.reserve(now + Duration::from_secs(1)), None);
}

#[test]
fn it_replaces_fallback_capacity_with_learned_policies() {
    let now = Instant::now();
    let mut state = state(now);
    for _ in 0..10 {
        assert_eq!(state.reserve(now), None);
        state.release();
    }
    assert_eq!(state.reserve(now), Some(now + DEFAULT_WINDOW_DURATION));
    state.observe(
        ResponseFeedback::parse(&quota_headers(&[("20;w=1", "10", "1")]), 200, now),
        now,
        state.completed_requests,
    );
    assert_eq!(state.reserve(now), None);
    assert_eq!(state.starts.len(), 11);
}

#[test]
fn it_learns_a_longer_policy_using_the_shared_history() {
    let now = Instant::now();
    let mut state = state(now);
    assert_eq!(state.reserve(now), None);
    observe(
        &mut state,
        &quota_headers(&[("20;w=1", "19", "1")]),
        200,
        now,
    );
    let later = now + Duration::from_secs(2);
    assert_eq!(state.reserve(later), None);
    observe(
        &mut state,
        &quota_headers(&[("2;w=60", "0", "60")]),
        200,
        later,
    );
    assert_eq!(state.starts.len(), 2);
    assert_eq!(state.reserve(later), Some(later + Duration::from_mins(1)));
}

#[test]
fn it_uses_bounded_fallback_reset_and_the_longer_429_delay() {
    let now = Instant::now();
    let mut state = state(now);
    let mut headers = HeaderMap::new();
    headers.insert("x-ratelimit-remaining", "0".parse().unwrap());
    state.observe(
        ResponseFeedback::parse(&headers, 200, now),
        now,
        state.completed_requests,
    );
    assert_eq!(state.reserve(now), Some(now + DEFAULT_RETRY_DELAY));
    assert_eq!(state.reserve(now + DEFAULT_RETRY_DELAY), None);
    headers.insert(RETRY_AFTER, "1".parse().unwrap());
    headers.insert("x-ratelimit-reset", "2".parse().unwrap());
    observe(&mut state, &headers, 429, now + DEFAULT_RETRY_DELAY);
    assert_eq!(
        state.reserve(now + DEFAULT_RETRY_DELAY),
        Some(now + DEFAULT_RETRY_DELAY + Duration::from_secs(2))
    );
}

#[test]
fn it_retains_history_and_remote_deadlines_until_the_latest_expiry() {
    let now = Instant::now();
    let mut state = state(now);
    state.last_activity = now.checked_sub(Duration::from_mins(10)).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-ratelimit-remaining", "0".parse().unwrap());
    headers.insert("x-ratelimit-reset", "3600".parse().unwrap());
    state.observe(
        ResponseFeedback::parse(&headers, 200, now),
        now,
        state.completed_requests,
    );
    assert_eq!(state.retain_until(), now + Duration::from_hours(1));
    state
        .windows
        .push(Window::new(100_000, Duration::from_hours(48)));
    assert_eq!(
        state.retain_until(),
        state.last_activity + Duration::from_hours(48)
    );
    state.prune(now + Duration::from_hours(1));
    assert!(state.unscoped_quota.is_none());
    assert_eq!(state.windows.len(), 1);
}
