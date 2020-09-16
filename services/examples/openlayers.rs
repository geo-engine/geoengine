use clap::Clap;
use geoengine_services::error::Error;
use geoengine_services::server;
use std::path::Path;
use std::{thread, time};
use tokio::sync::oneshot;

#[derive(Clap)]
struct Opts {
    #[clap(subcommand)]
    protocol: Protocol,
}

#[derive(Clap)]
enum Protocol {
    WMS,
    WFS,
}

/// Example of a client communicating with the geo engine
#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();

    // TODO: use special config for port etc. for starting the server and connecting to it
    let base_url = "http://localhost:3030/".to_string();

    let static_files_directory = Path::new(file!()).with_file_name(match opts.protocol {
        Protocol::WMS => "openlayers-wms-static/",
        Protocol::WFS => "openlayers-wfs-static/",
    });

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    eprintln!(
        "Starting server… serving files from `{}`",
        static_files_directory.display()
    );

    let (server, startup_success, interrupt_success) = tokio::join!(
        server::start_server(Some(shutdown_rx), Some(static_files_directory)),
        output_info(&base_url),
        server::interrupt_handler(shutdown_tx, Some(|| eprintln!("Shutting down server…"))),
    );

    server.and(startup_success).and(interrupt_success)
}

async fn output_info(base_url: &str) -> Result<(), Error> {
    if !server_has_started(base_url).await {
        return Err(Error::ServerStartup);
    }

    eprintln!(
        "Server is listening… visit {}{}",
        base_url, "static/index.html"
    );

    Ok(())
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
