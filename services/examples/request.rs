use geoengine_services::server;
use std::{thread, time};
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

/// Example of a client communicating with the geo engine
#[tokio::main]
async fn main() {
    // TODO: use special config for port etc. for starting the server and connecting to it
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let (server, success) = tokio::join!(
        server::start_server(Some(shutdown_rx), None),
        queries(shutdown_tx),
    );
    server.expect("server run");

    if success {
        std::process::exit(0)
    } else {
        std::process::exit(1)
    }
}

async fn queries(shutdown_tx: Sender<()>) -> bool {
    // TODO: use special config for port etc. for starting the server and connecting to it
    let base_url = "http://localhost:3030/".to_string();

    let mut success = false;
    if wait_for_server(&base_url).await {
        success = issue_queries(&base_url).await.is_ok();
    }

    shutdown_tx.send(()).expect("shutdown webserver");

    success
}

async fn issue_queries(base_url: &str) -> Result<(), reqwest::Error> {
    let client = reqwest::Client::new();
    let res = client
        .post(&format!("{}{}", base_url, "user/register"))
        .body(
            r#"{
            "email": "foo@bar.de",
            "password": "secret123",
            "real_name": "Foo Bar"
        }"#,
        )
        .send()
        .await?;

    println!("{:?}", res);

    Ok(())
}

const WAIT_SERVER_RETRIES: i32 = 5;
const WAIT_SERVER_RETRY_INTERVAL: u64 = 1;

async fn wait_for_server(base_url: &str) -> bool {
    let mut started = false;
    for _ in 0..WAIT_SERVER_RETRIES {
        if reqwest::get(base_url).await.is_ok() {
            started = true;
            break;
        } else {
            thread::sleep(time::Duration::from_secs(WAIT_SERVER_RETRY_INTERVAL));
        }
    }
    started
}
