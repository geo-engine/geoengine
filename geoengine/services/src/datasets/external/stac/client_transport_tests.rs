//! STAC request validation and provider-specific HTTP transport configuration.
use super::coordinator::StacClientError;
use super::*;
use httptest::{Expectation, Server, matchers::request, responders};
use std::time::Duration;

#[tokio::test]
async fn it_rejects_streaming_request_bodies_without_panicking() {
    let client = StacClient::new(
        reqwest::Client::new(),
        "https://non-replayable.test",
        StacRequestRegistry::new(),
    )
    .unwrap();
    let body =
        reqwest::Body::wrap_stream(futures::stream::pending::<Result<Vec<u8>, std::io::Error>>());
    let request = client.get("https://non-replayable.test/items").body(body);
    let error = tokio::time::timeout(
        Duration::from_millis(100),
        client.execute_json::<serde_json::Value>(request, &crate::util::retry::RetryPolicy::new()),
    )
    .await
    .expect("non-replayable requests must fail before sending or retrying")
    .unwrap_err();
    assert!(matches!(error, StacClientError::RequestNotReplayable));
}

#[tokio::test]
async fn it_shares_quota_channels_while_preserving_each_clients_timeout() {
    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/items"))
            .times(2)
            .respond_with(responders::delay_and_then(
                Duration::from_millis(400),
                responders::json_encoded(serde_json::json!({"complete": true})),
            )),
    );
    let registry = StacRequestRegistry::new();
    let fast = StacClient::new(
        reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap(),
        &server.url_str("/"),
        registry.clone(),
    )
    .unwrap();
    let slow = StacClient::new(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        &server.url_str("/"),
        registry,
    )
    .unwrap();
    let policy = crate::util::retry::RetryPolicy::new().max_retries(1);
    let error = fast
        .execute_json::<serde_json::Value>(fast.get(server.url_str("/items")), &policy)
        .await
        .unwrap_err();
    assert!(matches!(error, StacClientError::Http { source } if source.is_timeout()));
    let result = slow
        .execute_json::<serde_json::Value>(slow.get(server.url_str("/items")), &policy)
        .await
        .unwrap();
    assert_eq!(result, serde_json::json!({"complete": true}));
    let origin = url::Url::parse(&server.url_str("/")).unwrap();
    assert!(Arc::ptr_eq(
        &fast.request_registry.shared(&origin, None),
        &slow.request_registry.shared(&origin, None),
    ));
}
