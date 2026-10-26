//! Registry ownership and lifecycle regressions without network sockets.
use super::*;

fn origin(label: &str) -> Url {
    Url::parse(&format!("https://{label}.registry.test")).unwrap()
}

fn identity(label: &str) -> Identity {
    Identity {
        scheme: "https".to_owned(),
        host: format!("{label}.registry.test"),
        port: 443,
        token_endpoint: None,
        client_id: None,
        username: None,
        password: None,
    }
}

fn lookup(label: &str) -> oneshot::Receiver<Arc<StacRequestCoordinator>> {
    let (reply, receiver) = oneshot::channel();
    assert!(
        registry()
            .send(RegistryCommand::Lookup {
                identity: identity(label),
                reply
            })
            .is_ok()
    );
    receiver
}

#[tokio::test]
async fn it_serializes_concurrent_lookups_for_the_same_identity() {
    let origin = origin("concurrent-lookups");
    let coordinators =
        futures::future::join_all((0..64).map(|_| StacRequestCoordinator::shared(&origin, None)))
            .await;
    assert!(
        coordinators
            .iter()
            .all(|coordinator| Arc::ptr_eq(&coordinators[0], coordinator))
    );
}

#[tokio::test]
async fn it_keeps_pending_lookup_replies_alive_during_retirement() {
    let label = "pending-lookup";
    let coordinator = StacRequestCoordinator::shared(&origin(label), None).await;
    let weak = Arc::downgrade(&coordinator);
    drop(coordinator);

    // FIFO registry commands put an owning Arc into this reply before checking
    // retirement, even though the caller has not consumed that reply yet.
    let pending = lookup(label);
    assert!(!StacRequestCoordinator::try_retire(&weak).await);
    let reconstructed = pending.await.unwrap();
    assert!(weak.ptr_eq(&Arc::downgrade(&reconstructed)));
    drop(reconstructed);
    assert!(StacRequestCoordinator::try_retire(&weak).await);
}

#[tokio::test]
async fn it_does_not_leak_ownership_when_a_lookup_is_cancelled() {
    let label = "cancelled-lookup";
    let coordinator = StacRequestCoordinator::shared(&origin(label), None).await;
    let weak = Arc::downgrade(&coordinator);
    drop(coordinator);
    let (reply, receiver) = oneshot::channel();
    drop(receiver);
    assert!(
        registry()
            .send(RegistryCommand::Lookup {
                identity: identity(label),
                reply
            })
            .is_ok()
    );
    assert!(StacRequestCoordinator::try_retire(&weak).await);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn it_ignores_retirement_from_an_old_coordinator_after_replacement() {
    let origin = origin("retired-generation");
    let old = StacRequestCoordinator::shared(&origin, None).await;
    let old_weak = Arc::downgrade(&old);
    drop(old);
    assert!(StacRequestCoordinator::try_retire(&old_weak).await);
    let replacement = StacRequestCoordinator::shared(&origin, None).await;
    assert!(!old_weak.ptr_eq(&Arc::downgrade(&replacement)));
    assert!(StacRequestCoordinator::try_retire(&old_weak).await);
    let looked_up = StacRequestCoordinator::shared(&origin, None).await;
    assert!(Arc::ptr_eq(&replacement, &looked_up));
}

#[test]
fn it_preserves_the_registry_across_independent_tokio_runtime_lifetimes() {
    let origin = origin("independent-runtimes");
    let first = {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(StacRequestCoordinator::shared(&origin, None))
    };
    let second = {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(StacRequestCoordinator::shared(&origin, None))
    };
    assert!(Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn it_separates_registry_entries_by_every_configured_credential() {
    let origin = origin("credential-buckets");
    let credentials = super::super::StacProviderAuthentication {
        endpoint: "https://auth.example/token".to_owned(),
        client_id: "client".to_owned(),
        username: "user".to_owned(),
        password: "secret".to_owned(),
    };
    let shared = StacRequestCoordinator::shared(&origin, Some(&credentials)).await;
    let mut buckets = vec![shared];
    for field in 0..4 {
        let mut different = credentials.clone();
        match field {
            0 => different.endpoint = "https://other-auth.example/token".to_owned(),
            1 => different.client_id = "other-client".to_owned(),
            2 => different.username = "other-user".to_owned(),
            _ => different.password = "other-secret".to_owned(),
        }
        let coordinator = StacRequestCoordinator::shared(&origin, Some(&different)).await;
        assert!(
            buckets
                .iter()
                .all(|other| !Arc::ptr_eq(&coordinator, other))
        );
        buckets.push(coordinator);
    }
}

#[tokio::test]
async fn it_replaces_a_closed_request_actor_without_retiring_its_replacement() {
    let origin = origin("closed-request-actor");
    let stale = StacRequestCoordinator::shared(&origin, None).await;
    let weak = Arc::downgrade(&stale);
    let (jobs, receiver) = mpsc::unbounded_channel();
    let (cancellations, cancellation_receiver) = mpsc::unbounded_channel();
    stale
        .channels
        .set(ActorChannels {
            jobs,
            cancellations,
        })
        .unwrap();
    // Dropping the actor's receivers models its Tokio runtime shutting down.
    drop(receiver);
    drop(cancellation_receiver);
    let replacement = StacRequestCoordinator::shared(&origin, None).await;
    assert!(!Arc::ptr_eq(&stale, &replacement));
    assert!(StacRequestCoordinator::try_retire(&weak).await);
    let current = StacRequestCoordinator::shared(&origin, None).await;
    assert!(Arc::ptr_eq(&replacement, &current));
}
