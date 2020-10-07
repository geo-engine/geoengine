use geoengine_services::error::Error;
use geoengine_services::server;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let (server, interrupt_success) = tokio::join!(
        server::start_server(Some(shutdown_rx), None),
        server::interrupt_handler(shutdown_tx, Some(|| eprintln!("Shutting down server…"))),
    );

    server.and(interrupt_success)
}
