use geoengine_services::server;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

// Example of a client communicating with the geo engine
#[tokio::main]
async fn main() {
    // TODO: use special config for port etc. for starting the server and connecting to it
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let (server, _) = tokio::join!(
        server::start_server(Some(shutdown_rx)),
        queries(shutdown_tx),
    );
    server.expect("server run");
}

async fn queries(shutdown_tx: Sender<()>) {
    // TODO: use special config for port etc. for starting the server and connecting to it
    let base_url = "http://localhost:3030/".to_string();

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
        .await
        .expect("register user");

    println!("{:?}", res);

    shutdown_tx.send(()).expect("shutdown webserver");
}
