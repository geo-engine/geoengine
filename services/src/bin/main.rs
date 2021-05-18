use flexi_logger::{Age, Cleanup, Criterion, Logger, Naming};
use geoengine_services::error::Error;
use geoengine_services::server;
use geoengine_services::util::config;
use geoengine_services::util::config::get_config_element;
use log::info;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> Result<(), Error> {
    initialize_logging();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let (server, interrupt_success) = tokio::join!(
        server::start_server(Some(shutdown_rx), None),
        server::interrupt_handler(shutdown_tx, Some(|| info!("Shutting down server…"))),
    );

    server.and(interrupt_success)
}

fn initialize_logging() {
    let logging_config: config::Logging = get_config_element().unwrap();
    Logger::with_str(logging_config.log_spec)
        .log_to_file()
        .rotate(
            Criterion::Age(Age::Day),
            Naming::Timestamps,
            Cleanup::KeepLogFiles(7),
        )
        .duplicate_to_stderr(logging_config.duplicate_to_term)
        .start()
        .expect("initialized logger");
}
