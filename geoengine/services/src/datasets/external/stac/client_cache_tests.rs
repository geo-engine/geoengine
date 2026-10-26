//! STAC client coordinator-cache behavior.
use super::*;

#[tokio::test]
async fn it_reuses_the_coordinator_across_requests_and_client_clones() {
    use httptest::{Expectation, Server, matchers::request, responders};
    use serde_json::Value;

    let server = Server::run();
    for path in ["/first", "/second", "/third"] {
        server.expect(
            Expectation::matching(request::method_path("GET", path))
                .times(1)
                .respond_with(responders::status_code(200).body("null")),
        );
    }
    let registry = StacRequestRegistry::new();
    let client = StacClient::new(
        reqwest::Client::new(),
        &server.url_str("/"),
        registry.clone(),
    )
    .unwrap();
    let clone = client.clone();
    let policy = crate::util::retry::RetryPolicy::new().max_retries(1);

    client
        .execute_json::<Value>(client.get(server.url_str("/first")), &policy)
        .await
        .unwrap();
    assert_eq!(registry.lookup_count(), 1);
    clone
        .execute_json::<Value>(clone.get(server.url_str("/second")), &policy)
        .await
        .unwrap();
    client
        .execute_json::<Value>(client.get(server.url_str("/third")), &policy)
        .await
        .unwrap();

    assert_eq!(registry.lookup_count(), 1);
}

#[tokio::test]
async fn it_gives_auth_reconfiguration_a_separate_cache_from_existing_clones() {
    use httptest::{
        Expectation, Server, all_of,
        matchers::{any, contains, not, request},
        responders,
    };
    use serde_json::{Value, json};

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("POST", "/token"))
            .times(1)
            .respond_with(responders::json_encoded(json!({
                "access_token": "cache-test-token", "expires_in": 3600
            }))),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/anonymous"),
            request::headers(not(contains(("authorization", any())))),
        ])
        .times(2)
        .respond_with(responders::status_code(200).body("null")),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/authenticated"),
            request::headers(contains(("authorization", "Bearer cache-test-token"))),
        ])
        .times(2)
        .respond_with(responders::status_code(200).body("null")),
    );

    let registry = StacRequestRegistry::new();
    let anonymous = StacClient::new(
        reqwest::Client::new(),
        &server.url_str("/"),
        registry.clone(),
    )
    .unwrap();
    let existing_anonymous_clone = anonymous.clone();
    let policy = crate::util::retry::RetryPolicy::new().max_retries(1);
    existing_anonymous_clone
        .execute_json::<Value>(
            existing_anonymous_clone.get(server.url_str("/anonymous")),
            &policy,
        )
        .await
        .unwrap();
    assert_eq!(registry.lookup_count(), 1);

    let authenticated = anonymous
        .clone()
        .with_authentication(Some(StacProviderAuthentication {
            endpoint: server.url_str("/token"),
            client_id: "client".to_owned(),
            username: "user".to_owned(),
            password: "password".to_owned(),
        }))
        .await
        .unwrap();
    for _ in 0..2 {
        authenticated
            .execute_json::<Value>(authenticated.get(server.url_str("/authenticated")), &policy)
            .await
            .unwrap();
    }
    anonymous
        .execute_json::<Value>(anonymous.get(server.url_str("/anonymous")), &policy)
        .await
        .unwrap();

    assert_eq!(registry.lookup_count(), 2);
}

#[test]
fn it_replaces_a_cached_coordinator_once_after_runtime_shutdown() {
    use httptest::{Expectation, Server, matchers::request, responders};
    use serde_json::Value;

    let server = Server::run();
    for path in ["/first", "/second", "/third"] {
        server.expect(
            Expectation::matching(request::method_path("GET", path))
                .times(1)
                .respond_with(responders::status_code(200).body("null")),
        );
    }
    let registry = StacRequestRegistry::new();
    let client = StacClient::new(
        reqwest::Client::new(),
        &server.url_str("/"),
        registry.clone(),
    )
    .unwrap();
    let policy = crate::util::retry::RetryPolicy::new().max_retries(1);
    let request = |path: &str| client.get(server.url_str(path));
    let first_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    first_runtime.block_on(async {
        client
            .execute_json::<Value>(request("/first"), &policy)
            .await
            .unwrap();
    });
    assert_eq!(registry.lookup_count(), 1);
    drop(first_runtime);
    assert!(client.cached_coordinator.load_full().unwrap().is_closed());

    let second_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    second_runtime.block_on(async {
        client
            .execute_json::<Value>(request("/second"), &policy)
            .await
            .unwrap();
        assert!(!client.cached_coordinator.load_full().unwrap().is_closed());
        client
            .execute_json::<Value>(request("/third"), &policy)
            .await
            .unwrap();
    });
    assert_eq!(registry.lookup_count(), 2);
}
