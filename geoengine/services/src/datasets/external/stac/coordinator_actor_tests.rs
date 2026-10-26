//! Actor scheduling tests that do not require opening local TCP sockets.
use super::*;
use std::future::pending;

fn test_job(id: usize) -> (AttemptJob, oneshot::Receiver<AttemptReply>) {
    let client = reqwest::Client::new();
    let request = client
        .get(format!("https://actor.test/{id}"))
        .build()
        .expect("test URL should build");
    let (reply, receiver) = oneshot::channel();
    (
        AttemptJob {
            client,
            request,
            authentication: None,
            reply,
        },
        receiver,
    )
}

#[tokio::test]
async fn it_runs_parallel_attempts_and_skips_a_cancelled_fifo_head() {
    let registry = StacRequestRegistry::new();
    let (started, mut starts) = mpsc::unbounded_channel::<String>();
    let identity = identity_for(&Url::parse("https://parallel.actor.test").unwrap(), None);
    let coordinator = registry.shared_with_dispatch(identity, move |job| {
        let path = job.request.url().path().to_owned();
        let started = started.clone();
        Box::pin(async move {
            let AttemptJob { mut reply, .. } = job;
            let _ = started.send(path);
            tokio::select! {
                () = reply.closed() => (reply, None),
                outcome = pending::<AttemptOutcome>() => outcome,
            }
        }) as RunningAttempt
    });
    let jobs = coordinator.jobs.clone();
    let cancellations = coordinator.cancellations.clone();

    let mut replies = Vec::new();
    for id in 0..10 {
        let (job, reply) = test_job(id);
        jobs.send(job).expect("actor should receive each test job");
        replies.push(reply);
    }
    let mut first_wave = Vec::new();
    for _ in 0..10 {
        first_wave.push(
            tokio::time::timeout(Duration::from_secs(1), starts.recv())
                .await
                .expect("fallback capacity should admit ten requests")
                .expect("actor should report each started request"),
        );
    }
    assert_eq!(
        first_wave,
        (0..10).map(|id| format!("/{id}")).collect::<Vec<_>>()
    );

    let (cancelled_job, cancelled_reply) = test_job(10);
    let (later_job, later_reply) = test_job(11);
    jobs.send(cancelled_job)
        .expect("actor should receive the queued test job");
    jobs.send(later_job)
        .expect("actor should receive the following test job");
    replies.push(cancelled_reply);
    replies.push(later_reply);

    assert!(
        tokio::time::timeout(Duration::from_millis(20), starts.recv())
            .await
            .is_err()
    );

    drop(replies.remove(10));
    cancellations
        .send(())
        .expect("actor should receive the cancellation wakeup");
    let next = tokio::time::timeout(Duration::from_secs(2), starts.recv())
        .await
        .expect("the next FIFO job should run when the rate window opens")
        .expect("actor should report the next started request");
    assert_eq!(next, "/11", "the cancelled queue head must be skipped");
    assert!(!first_wave.contains(&"/10".to_owned()));

    drop(replies);
    drop(jobs);
    drop(cancellations);
}

// Drive real actor jobs and reply decoding with controlled responses and no sockets.
type StartedAttempt = (String, oneshot::Sender<reqwest::Response>);

fn controlled_coordinator(
    label: &str,
) -> (
    StacRequestRegistry,
    Arc<ActorChannels>,
    mpsc::UnboundedReceiver<StartedAttempt>,
) {
    let registry = StacRequestRegistry::new();
    let (started, starts) = mpsc::unbounded_channel();
    let identity = identity_for(
        &Url::parse(&format!("https://{label}.actor.test")).unwrap(),
        None,
    );
    let coordinator = registry.shared_with_dispatch(identity, move |job| {
        let started = started.clone();
        Box::pin(async move {
            let (finish, response) = oneshot::channel();
            let path = job.request.url().path().to_owned();
            let mut reply = job.reply;
            started.send((path, finish)).unwrap();
            tokio::select! {
                () = reply.closed() => (reply, None),
                response = response => (reply, response.ok().map(Ok)),
            }
        }) as RunningAttempt
    });
    (registry, coordinator, starts)
}

fn execute_job(
    coordinator: Arc<ActorChannels>,
    id: usize,
) -> tokio::task::JoinHandle<Result<serde_json::Value, StacClientError>> {
    execute_job_with_policy(coordinator, id, RetryPolicy::new().max_retries(1))
}

fn execute_job_with_policy(
    coordinator: Arc<ActorChannels>,
    id: usize,
    policy: RetryPolicy,
) -> tokio::task::JoinHandle<Result<serde_json::Value, StacClientError>> {
    tokio::spawn(async move {
        // Use the fixture's registry and origin so lookup selects its dispatcher.
        let registry = StacRequestRegistry {
            coordinators: coordinator.registry.upgrade().unwrap(),
        };
        let origin = format!("https://{}/", coordinator.identity.host);
        let client = StacClient::new(reqwest::Client::new(), &origin, registry).unwrap();
        client
            .execute_json(client.get(format!("https://actor.test/{id}")), &policy)
            .await
    })
}

async fn next_start(starts: &mut mpsc::UnboundedReceiver<StartedAttempt>) -> StartedAttempt {
    tokio::time::timeout(Duration::from_secs(2), starts.recv())
        .await
        .expect("the actor should dispatch the eligible attempt")
        .expect("the actor should keep its dispatch channel open")
}

fn response(id: usize, remaining: Option<usize>) -> reqwest::Response {
    let mut builder = httptest::http::Response::builder();
    if let Some(remaining) = remaining {
        builder = builder
            .header("x-ratelimit-limit", "100;w=60")
            .header("x-ratelimit-remaining", remaining)
            .header("x-ratelimit-reset", "1");
    }
    builder.body(format!("{{\"id\":{id}}}")).unwrap().into()
}

#[tokio::test]
async fn it_routes_out_of_order_responses_without_waiting_for_earlier_attempts() {
    let (_registry, coordinator, mut starts) = controlled_coordinator("response-routing");
    let first = execute_job(coordinator.clone(), 1);
    let (first_path, first_response) = next_start(&mut starts).await;
    let second = execute_job(coordinator, 2);
    let (second_path, second_response) = next_start(&mut starts).await;
    assert_eq!(first_path, "/1");
    assert_eq!(second_path, "/2");
    second_response.send(response(2, None)).unwrap();
    assert_eq!(second.await.unwrap().unwrap(), serde_json::json!({"id": 2}));
    assert!(
        !first.is_finished(),
        "a slow response must not serialize other attempts"
    );
    first_response.send(response(1, None)).unwrap();
    assert_eq!(first.await.unwrap().unwrap(), serde_json::json!({"id": 1}));
}

#[tokio::test]
async fn it_releases_cancelled_inflight_bookkeeping_before_merging_feedback() {
    let (_registry, coordinator, mut starts) = controlled_coordinator("inflight-cancel");
    let first = execute_job(coordinator.clone(), 1);
    let (_, first_response) = next_start(&mut starts).await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let second = execute_job(coordinator.clone(), 2);
    let (_, second_response) = next_start(&mut starts).await;
    second_response.send(response(2, Some(1))).unwrap();
    second.await.unwrap().unwrap();
    assert!(
        first_response.is_closed(),
        "cancellation must drop the HTTP future"
    );
    let third = execute_job(coordinator, 3);
    let (_, third_response) =
        tokio::time::timeout(Duration::from_millis(200), next_start(&mut starts))
            .await
            .expect("cancelled attempts must not reduce the next server remaining quota");
    third_response.send(response(3, None)).unwrap();
    third.await.unwrap().unwrap();
}

#[tokio::test]
async fn it_retries_rate_limits_through_admission_without_policy_backoff() {
    let (_registry, coordinator, mut starts) = controlled_coordinator("rate-limit-retry");
    let query = execute_job_with_policy(
        coordinator,
        1,
        RetryPolicy::new().max_retries(2).initial_delay_ms(10_000),
    );
    let (_, first_response) = next_start(&mut starts).await;
    let limited = Instant::now();
    first_response
        .send(
            httptest::http::Response::builder()
                .status(429)
                .header(RETRY_AFTER, "1")
                .body("")
                .unwrap()
                .into(),
        )
        .unwrap();
    let (path, retried_response) = next_start(&mut starts).await;
    assert_eq!(path, "/1");
    assert!(limited.elapsed() >= Duration::from_secs(1));
    retried_response.send(response(1, None)).unwrap();
    assert_eq!(query.await.unwrap().unwrap(), serde_json::json!({"id": 1}));
}

#[tokio::test]
async fn it_charges_completed_newer_attempts_against_delayed_quota_feedback() {
    for (scoped, cancelled) in [(false, false), (false, true), (true, false), (true, true)] {
        let (_registry, coordinator, mut starts) = controlled_coordinator("delayed-quota");
        let first = execute_job(coordinator.clone(), 1);
        let (_, first_response) = next_start(&mut starts).await;
        let second = execute_job(coordinator.clone(), 2);
        let (_, mut second_response) = next_start(&mut starts).await;

        // The newer request consumes the server's final slot, but supplies no
        // feedback, either because its headers omit quotas or it is cancelled.
        if cancelled {
            second.abort();
            assert!(second.await.unwrap_err().is_cancelled());
            tokio::time::timeout(Duration::from_secs(1), second_response.closed())
                .await
                .unwrap();
        } else {
            second_response.send(response(2, None)).unwrap();
            second.await.unwrap().unwrap();
        }

        // The older response was prepared before the newer request consumed its
        // slot. Its remaining=1 must not make that capacity available again.
        let mut delayed = response(1, Some(1));
        if !scoped {
            delayed.headers_mut().remove("x-ratelimit-limit");
        }
        first_response.send(delayed).unwrap();
        first.await.unwrap().unwrap();

        let third = execute_job(coordinator, 3);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), starts.recv())
                .await
                .is_err(),
            "a completed newer attempt must stay charged until reset"
        );
        let (_, third_response) = next_start(&mut starts).await;
        third_response.send(response(3, None)).unwrap();
        third.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn it_preserves_learned_quota_when_providers_are_reconstructed() {
    let (registry, coordinator, mut starts) = controlled_coordinator("retained-quota");
    let previous = Arc::downgrade(&coordinator);
    let first = execute_job(coordinator.clone(), 1);
    let (_, first_response) = next_start(&mut starts).await;
    first_response.send(response(1, Some(0))).unwrap();
    first.await.unwrap().unwrap();
    drop(coordinator);
    let reconstructed = registry.shared(
        &Url::parse("https://retained-quota.actor.test/collection").unwrap(),
        None,
    );
    assert!(previous.ptr_eq(&Arc::downgrade(&reconstructed)));
    let second = execute_job(reconstructed, 2);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), starts.recv())
            .await
            .is_err()
    );
    let (_, second_response) = next_start(&mut starts).await;
    second_response.send(response(2, None)).unwrap();
    second.await.unwrap().unwrap();
}

#[tokio::test]
async fn it_retires_only_registry_owned_coordinators() {
    let registry = StacRequestRegistry::new();
    let coordinator = registry.shared(
        &Url::parse("https://idle-retirement.actor.test").unwrap(),
        None,
    );
    let weak = Arc::downgrade(&coordinator);
    assert!(!ActorChannels::try_retire(&weak));
    drop(coordinator);
    assert!(ActorChannels::try_retire(&weak));
    assert!(weak.upgrade().is_none());
    tokio::task::yield_now().await;
}
