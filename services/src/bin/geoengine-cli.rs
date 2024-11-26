use clap::{Parser, Subcommand};
use geoengine_services::cli::{
    check_heartbeat, check_successful_startup, CheckSuccessfulStartup, Heartbeat,
};

/// CLI for Geo Engine Utilities
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Checks the program's `STDERR` for successful startup
    CheckSuccessfulStartup(CheckSuccessfulStartup),

    /// Checks if the Geo Engine server is alive
    Heartbeat(Heartbeat),
}

impl Commands {
    async fn execute(self) -> Result<(), anyhow::Error> {
        match self {
            Commands::CheckSuccessfulStartup(params) => check_successful_startup(params).await,
            Commands::Heartbeat(params) => check_heartbeat(params).await,
        }
    }
}

#[tokio::main]
#[allow(clippy::print_stderr)]
async fn main() {
    let cli = Cli::parse();

    if let Err(err) = cli.command.execute().await {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}
