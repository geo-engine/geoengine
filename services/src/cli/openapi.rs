#![allow(clippy::print_stderr, clippy::print_stdout)] // okay in CLI

use crate::api::apidoc::ApiDoc;
use clap::Parser;
use geoengine_operators::util::spawn_blocking;
use utoipa::OpenApi;

/// Checks if the Geo Engine server is alive
#[derive(Debug, Parser)]
pub struct OpenAPIGenerate;

/// Outputs OpenAPI JSON to `STDOUT`
pub async fn output_openapi_json(_params: OpenAPIGenerate) -> Result<(), anyhow::Error> {
    spawn_blocking(_output_openapi_json).await?
}

fn _output_openapi_json() -> Result<(), anyhow::Error> {
    let mut spec = ApiDoc::openapi();

    // make server a wildcard
    spec.servers = Some(vec![utoipa::openapi::ServerBuilder::new()
        .url("{server}/api")
        .parameter(
            "server",
            utoipa::openapi::ServerVariableBuilder::new()
                .default_value("https://geoengine.io")
                .build(),
        )
        .build()]);

    println!("{}", serde_json::to_string_pretty(&spec)?);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_cmd::cargo::CommandCargoExt;
    use std::{
        path::{Path, PathBuf},
        process::{Command, Stdio},
    };

    #[test]
    fn it_generates_json() {
        let cli_result = Command::cargo_bin("geoengine-cli")
            .unwrap()
            .arg("openapi")
            .current_dir(workspace_dir())
            .stdout(Stdio::piped())
            .output()
            .unwrap();

        assert!(
            cli_result.status.success(),
            "failed to run CLI: {cli_result:?}",
        );

        let _openapi_spec: serde_json::Value = serde_json::from_slice(&cli_result.stdout).unwrap();
    }

    fn workspace_dir() -> PathBuf {
        let output = Command::new(env!("CARGO"))
            .arg("locate-project")
            .arg("--workspace")
            .arg("--message-format=plain")
            .output()
            .unwrap()
            .stdout;
        let cargo_path = Path::new(std::str::from_utf8(&output).unwrap().trim());
        cargo_path.parent().unwrap().to_path_buf()
    }

    #[tokio::test]
    async fn it_runs_successfully() {
        output_openapi_json(OpenAPIGenerate).await.unwrap();
    }
}
