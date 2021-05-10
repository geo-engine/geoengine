use flexi_logger::{Age, Cleanup, Criterion, Duplicate, Logger, Naming};
use geoengine_services::error::Error;
use geoengine_services::server;
use log::info;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> Result<(), Error> {
    Logger::with_str("info")
        .log_to_file()
        .rotate(
            Criterion::Age(Age::Day),
            Naming::Timestamps,
            Cleanup::KeepLogFiles(7),
        )
        .duplicate_to_stderr(Duplicate::All)
        .start_with_specfile("logspec.toml")
        .expect("initialized logger");

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let (server, interrupt_success) = tokio::join!(
        server::start_server(Some(shutdown_rx), None),
        server::interrupt_handler(shutdown_tx, Some(|| info!("Shutting down server…"))),
    );

    server.and(interrupt_success)
}
