use geoengine_services::error::Error;
use geoengine_services::server;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // TODO: use special config for port etc. for starting the server and connecting to it
    let base_url = "http://localhost:3030/".to_string();

    eprintln!("Starting server… {}", base_url);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let (server, interrupt_success) = tokio::join!(
        server::start_server(Some(shutdown_rx), None),
        server::interrupt_handler(shutdown_tx, Some(|| eprintln!("Shutting down server…"))),
    );

    server.and(interrupt_success)
}
