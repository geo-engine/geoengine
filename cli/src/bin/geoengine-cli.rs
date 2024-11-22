use clap::{Parser, Subcommand};
use geoengine_cli::{check_successful_startup, CheckSuccessfulStartup};

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
}

#[tokio::main]
#[allow(clippy::print_stderr)]
async fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::CheckSuccessfulStartup(params) => check_successful_startup(params).await,
    };

    if let Err(err) = result {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}
