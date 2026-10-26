//! STAC client registry reuse, authentication isolation, and runtime recovery.
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

    let origin = url::Url::parse(&server.url_str("/")).unwrap();
    let coordinator = registry.shared(&origin, None);

    client
        .execute_json::<Value>(client.get(server.url_str("/first")), &policy)
        .await
        .unwrap();
    clone
        .execute_json::<Value>(clone.get(server.url_str("/second")), &policy)
        .await
        .unwrap();
    client
        .execute_json::<Value>(client.get(server.url_str("/third")), &policy)
        .await
        .unwrap();

    assert!(Arc::ptr_eq(&coordinator, &registry.shared(&origin, None)));
}

#[tokio::test]
async fn it_preserves_existing_clones_authentication_after_reconfiguration() {
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
                "access_token": "registry-test-token", "expires_in": 3600
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
            request::headers(contains(("authorization", "Bearer registry-test-token"))),
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
}

#[test]
fn it_replaces_a_closed_coordinator_after_runtime_shutdown() {
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
    let origin = url::Url::parse(&server.url_str("/")).unwrap();
    let first_coordinator = first_runtime.block_on(async {
        client
            .execute_json::<Value>(request("/first"), &policy)
            .await
            .unwrap();
        registry.shared(&origin, None)
    });
    drop(first_runtime);
    assert!(first_coordinator.is_closed());

    let second_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    second_runtime.block_on(async {
        client
            .execute_json::<Value>(request("/second"), &policy)
            .await
            .unwrap();
        let replacement = registry.shared(&origin, None);
        assert!(!replacement.is_closed());
        assert!(!Arc::ptr_eq(&first_coordinator, &replacement));
        client
            .execute_json::<Value>(request("/third"), &policy)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&replacement, &registry.shared(&origin, None)));
    });
}
