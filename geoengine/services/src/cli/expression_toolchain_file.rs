use anyhow::Context;
use clap::Parser;
use geoengine_expression::write_minimal_toolchain_file;
use std::{fs::File, io::BufWriter, path::PathBuf};

const RUST_TOOLCHAIN_TOML: &str = std::include_str!("../../../rust-toolchain.toml");

/// Generates an rustup toolchain file for compiling expressions
#[derive(Debug, Default, Parser)]
pub struct ExpressionToolchainFile {
    /// Output file path. If not provided, outputs to STDOUT
    #[arg(long, default_value = "None")]
    file: Option<PathBuf>,
}

/// Checks the program's `STDERR` for successful startup
#[allow(clippy::print_stderr)]
pub async fn output_toolchain_file(params: ExpressionToolchainFile) -> Result<(), anyhow::Error> {
    match params.file {
        None => crate::util::spawn_blocking(move || {
            let stdout = std::io::stdout();
            let mut writer = BufWriter::new(stdout);
            write_minimal_toolchain_file(&mut writer, RUST_TOOLCHAIN_TOML)
        })
        .await?
        .context("Cannot write toolchain file"),
        Some(path) => crate::util::spawn_blocking(move || {
            let file = File::create(path)?;
            let mut writer = BufWriter::new(file);
            write_minimal_toolchain_file(&mut writer, RUST_TOOLCHAIN_TOML)
        })
        .await?
        .context("Cannot write toolchain file"),
    }
}
