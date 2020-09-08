use geoengine_services::server;
use std::path::Path;
use std::{thread, time};
use tokio::signal;
use tokio::sync::oneshot::{self, Sender};

/// Example of a client communicating with the geo engine
#[tokio::main]
async fn main() -> Result<(), ()> {
    // TODO: use special config for port etc. for starting the server and connecting to it
    let base_url = "http://localhost:3030/".to_string();

    let static_files_directory = Path::new(file!()).with_file_name("openlayers-wms-static/");

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    eprintln!(
        "Starting server… serving files from `{}`",
        static_files_directory.display()
    );

    let (server, startup_success, interrupt_success) = tokio::join!(
        server::start_server(Some(shutdown_rx), Some(static_files_directory)),
        output_info(&base_url),
        interrupt_handler(shutdown_tx),
    );
    server.unwrap();

    startup_success.and(interrupt_success)
}

async fn output_info(base_url: &str) -> Result<(), ()> {
    if !server_has_started(base_url).await {
        return Err(());
    }

    eprintln!(
        "Server is listening… visit {}{}",
        base_url, "static/index.html"
    );

    Ok(())
}

async fn interrupt_handler(shutdown_tx: Sender<()>) -> Result<(), ()> {
    signal::ctrl_c().await.or(Err(()))?;

    eprintln!("Shutting down server…");

    shutdown_tx.send(())
}

const WAIT_SERVER_RETRIES: i32 = 5;
const WAIT_SERVER_RETRY_INTERVAL: u64 = 1;

async fn server_has_started(base_url: &str) -> bool {
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
