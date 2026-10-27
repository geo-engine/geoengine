//! Registry ownership and lifecycle regressions without network sockets.
use super::*;
use futures::future::join_all;

fn origin(label: &str) -> Url {
    Url::parse(&format!("https://{label}.registry.test")).unwrap()
}

fn registry_with_capacity(capacity: usize) -> StacRequestRegistry {
    StacRequestRegistry {
        coordinators: Arc::new(CoordinatorMap {
            entries: DashMap::new(),
            capacity: Arc::new(Semaphore::new(capacity)),
        }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn it_bounds_concurrent_distinct_identities_without_evicting_retained_quotas() {
    let registry = registry_with_capacity(2);
    let barrier = Arc::new(tokio::sync::Barrier::new(33));
    let lookups: Vec<_> = (0..32)
        .map(|id| {
            let registry = registry.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                registry.shared(&origin(&format!("bounded-{id}")), None)
            })
        })
        .collect();
    barrier.wait().await;
    let mut coordinators = Vec::new();
    for result in join_all(lookups).await {
        match result.unwrap() {
            Ok(coordinator) => coordinators.push(coordinator),
            Err(error) => assert!(matches!(error, StacClientError::RegistryFull)),
        }
    }
    assert_eq!(coordinators.len(), 2);
    assert_eq!(registry.coordinators.entries.len(), 2);
    for coordinator in &coordinators {
        let same = registry
            .shared(&origin_from_identity(&coordinator.identity), None)
            .unwrap();
        assert!(Arc::ptr_eq(coordinator, &same));
    }
    let retained = Arc::downgrade(&coordinators.pop().unwrap());
    assert!(matches!(
        registry.shared(&origin("new-identity"), None),
        Err(StacClientError::RegistryFull)
    ));
    assert!(
        retained.upgrade().is_some(),
        "capacity pressure must preserve quota history"
    );
    // Simulate the actor removing this entry after its retention deadline.
    assert!(ActorChannels::try_retire(&retained));
    registry.shared(&origin("new-identity"), None).unwrap();
    assert_eq!(registry.coordinators.entries.len(), 2);
}

fn origin_from_identity(identity: &Identity) -> Url {
    Url::parse(&format!(
        "{}://{}:{}",
        identity.scheme, identity.host, identity.port
    ))
    .unwrap()
}

#[tokio::test]
async fn it_propagates_registry_capacity_errors_without_sending_or_retrying() {
    let registry = registry_with_capacity(0);
    let client = StacClient::new(
        reqwest::Client::new(),
        "https://full.registry.test",
        registry,
    )
    .unwrap();
    let error = client
        .execute_json::<serde_json::Value>(
            client.get("https://full.registry.test/items"),
            &RetryPolicy::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StacClientError::RegistryFull));
    assert_eq!(error.status(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn it_serializes_concurrent_lookups_for_the_same_identity() {
    let registry = StacRequestRegistry::new();
    let origin = origin("concurrent-lookups");
    let barrier = Arc::new(tokio::sync::Barrier::new(65));
    let lookups: Vec<_> = (0..64)
        .map(|_| {
            let registry = registry.clone();
            let origin = origin.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                registry.shared(&origin, None).unwrap()
            })
        })
        .collect();
    barrier.wait().await;
    let tasks = join_all(lookups).await;
    let coordinators: Vec<_> = tasks.into_iter().map(Result::unwrap).collect();
    assert!(
        coordinators
            .iter()
            .all(|coordinator| Arc::ptr_eq(&coordinators[0], coordinator))
    );
}

#[tokio::test]
async fn it_ignores_retirement_from_an_old_coordinator_after_replacement() {
    let registry = StacRequestRegistry::new();
    let origin = origin("retired-generation");
    let old = registry.shared(&origin, None).unwrap();
    let old_weak = Arc::downgrade(&old);
    drop(old);
    assert!(ActorChannels::try_retire(&old_weak));
    let replacement = registry.shared(&origin, None).unwrap();
    assert!(!old_weak.ptr_eq(&Arc::downgrade(&replacement)));
    assert!(ActorChannels::try_retire(&old_weak));
    let looked_up = registry.shared(&origin, None).unwrap();
    assert!(Arc::ptr_eq(&replacement, &looked_up));
}

#[test]
fn it_replaces_a_closed_actor_after_its_tokio_runtime_shuts_down() {
    // Even a full registry must replace a stopped actor for an existing identity.
    let registry = registry_with_capacity(1);
    let origin = origin("independent-runtimes");
    let first = {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async { registry.shared(&origin, None).unwrap() })
    };
    assert!(first.jobs.is_closed());
    let second_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let second = second_runtime.block_on(async { registry.shared(&origin, None).unwrap() });
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(!second.jobs.is_closed());
    drop(second_runtime);
    assert!(second.jobs.is_closed());
}

#[test]
fn it_returns_an_error_when_the_actor_runtime_stops_during_a_request() {
    let registry = StacRequestRegistry::new();
    let origin = origin("shutdown-during-request");
    let actor_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let channels = actor_runtime.block_on(async { registry.shared(&origin, None).unwrap() });
    let client = StacClient::new(reqwest::Client::new(), origin.as_str(), registry).unwrap();
    let caller_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let policy = RetryPolicy::new();
    let mut request =
        Box::pin(client.execute_json::<serde_json::Value>(client.get(origin), &policy));
    assert!(
        caller_runtime
            .block_on(async { request.as_mut().now_or_never() })
            .is_none()
    );

    // A different runtime owns the actor, so no HTTP work runs before shutdown.
    drop(actor_runtime);
    assert!(channels.jobs.is_closed());
    let error = caller_runtime
        .block_on(async { tokio::time::timeout(Duration::from_millis(100), request).await })
        .expect("a stopped coordinator must fail without retrying")
        .unwrap_err();
    assert!(matches!(error, StacClientError::CoordinatorStopped));
}

#[test]
fn it_returns_an_error_when_submission_races_with_actor_shutdown() {
    let registry = StacRequestRegistry::new();
    let origin = origin("shutdown-before-submission");
    let actor_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let channels = actor_runtime.block_on(async { registry.shared(&origin, None).unwrap() });
    let client = StacClient::new(reqwest::Client::new(), origin.as_str(), registry).unwrap();
    let request = client.get(origin).build().unwrap();
    drop(actor_runtime);

    let caller_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let error = caller_runtime
        .block_on(client.execute_attempt(&channels, &request))
        .unwrap_err();
    assert!(matches!(error, StacClientError::CoordinatorStopped));
}

#[tokio::test]
async fn it_separates_independent_registry_owners() {
    let first_registry = StacRequestRegistry::new();
    let second_registry = StacRequestRegistry::new();
    let origin = origin("independent-owners");
    let first = first_registry.shared(&origin, None).unwrap();
    let second = second_registry.shared(&origin, None).unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn it_does_not_keep_the_registry_alive_through_coordinator_ownership() {
    let registry = StacRequestRegistry::new();
    let coordinator = registry
        .shared(&origin("weak-registry-owner"), None)
        .unwrap();
    let weak_registry = Arc::downgrade(&registry.coordinators);
    drop(registry);
    assert!(weak_registry.upgrade().is_none());
    assert!(coordinator.registry.upgrade().is_none());
}

#[tokio::test]
async fn it_separates_registry_entries_by_every_configured_credential() {
    let registry = StacRequestRegistry::new();
    let origin = origin("credential-buckets");
    let credentials = StacProviderAuthentication {
        endpoint: "https://auth.example/token".to_owned(),
        client_id: "client".to_owned(),
        username: "user".to_owned(),
        password: "secret".to_owned(),
    };
    let shared = registry.shared(&origin, Some(&credentials)).unwrap();
    let mut buckets = vec![shared];
    for field in 0..4 {
        let mut different = credentials.clone();
        match field {
            0 => different.endpoint = "https://other-auth.example/token".to_owned(),
            1 => different.client_id = "other-client".to_owned(),
            2 => different.username = "other-user".to_owned(),
            _ => different.password = "other-secret".to_owned(),
        }
        let coordinator = registry.shared(&origin, Some(&different)).unwrap();
        assert!(
            buckets
                .iter()
                .all(|other| !Arc::ptr_eq(&coordinator, other))
        );
        buckets.push(coordinator);
    }
}

#[tokio::test]
async fn it_keeps_cloned_registry_handles_in_the_same_sharing_domain() {
    let registry = StacRequestRegistry::new();
    let cloned = registry.clone();
    let origin = origin("cloned-registry");
    let first = registry.shared(&origin, None).unwrap();
    let second = cloned.shared(&origin, None).unwrap();
    assert!(Arc::ptr_eq(&first, &second));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn it_never_retires_a_coordinator_handed_out_by_a_racing_lookup() {
    let registry = StacRequestRegistry::new();
    for iteration in 0..64 {
        let origin = origin(&format!("retirement-race-{iteration}"));
        let original = registry.shared(&origin, None).unwrap();
        let weak = Arc::downgrade(&original);
        drop(original);
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let lookup_registry = registry.clone();
        let lookup_origin = origin.clone();
        let lookup_barrier = barrier.clone();
        let lookup = tokio::spawn(async move {
            lookup_barrier.wait().await;
            lookup_registry.shared(&lookup_origin, None).unwrap()
        });
        let retire_barrier = barrier.clone();
        let retire = tokio::spawn(async move {
            retire_barrier.wait().await;
            ActorChannels::try_retire(&weak)
        });
        barrier.wait().await;
        let coordinator = lookup.await.unwrap();
        retire.await.unwrap();
        let current = registry.shared(&origin, None).unwrap();
        assert!(Arc::ptr_eq(&current, &coordinator));
        assert!(!coordinator.jobs.is_closed());
    }
}
