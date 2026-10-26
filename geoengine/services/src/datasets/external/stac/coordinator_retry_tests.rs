//! HTTP retry regressions retained from PR #1343.
use super::*;
use std::sync::Mutex;

#[tokio::test]
async fn it_returns_the_http_error_when_the_attempt_budget_is_exhausted() {
    use httptest::{Expectation, Server, matchers::request, responders};

    for max_retries in [0, 1, 3] {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/items"))
                .times(max_retries.max(1) as usize)
                .respond_with(responders::status_code(500)),
        );
        let registry = StacRequestRegistry::new();
        let coordinator = registry.shared(&Url::parse(&server.url_str("/")).unwrap(), None);
        let client = reqwest::Client::new();
        let request = client.get(server.url_str("/items")).build().unwrap();
        let policy = RetryPolicy::new()
            .max_retries(max_retries)
            .initial_delay_ms(1);

        let error = coordinator
            .execute::<serde_json::Value>(&client, request, None, &policy)
            .await
            .expect_err("exhausting the attempt budget returns the HTTP error");

        assert_eq!(
            error.status(),
            Some(reqwest::StatusCode::INTERNAL_SERVER_ERROR)
        );
    }
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
    let registry = StacRequestRegistry::new();
    let coordinator = registry.shared(&Url::parse(&server.url_str("/")).unwrap(), None);
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
    let registry = StacRequestRegistry::new();
    let coordinator = registry.shared(&Url::parse(&server.url_str("/")).unwrap(), None);
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
        let registry = StacRequestRegistry::new();
        let coordinator = registry.shared(&Url::parse(&server.url_str("/")).unwrap(), None);
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
        let registry = StacRequestRegistry::new();
        let coordinator = registry.shared(&Url::parse(&server.url_str("/")).unwrap(), None);
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
