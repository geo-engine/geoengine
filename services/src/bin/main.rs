use geoengine_services::error::{Error, Result};
use geoengine_services::server;
use tokio::sync::oneshot::{self, Receiver};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let (server, interrupt_success) = tokio::join!(
        start_server(shutdown_rx),
        server::interrupt_handler(shutdown_tx, Some(|| eprintln!("Shutting down server…"))),
    );

    server.and(interrupt_success)
}

#[cfg(not(feature = "pro"))]
pub async fn start_server(shutdown_rx: Receiver<()>) -> Result<()> {
    server::start_server(Some(shutdown_rx), None).await
}

#[cfg(feature = "pro")]
pub async fn start_server(shutdown_rx: Receiver<()>) -> Result<()> {
    geoengine_services::pro::server::start_pro_server(Some(shutdown_rx), None).await
}
