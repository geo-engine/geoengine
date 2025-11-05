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
    spec.servers = Some(vec![
        utoipa::openapi::ServerBuilder::new()
            .url("{server}/api")
            .parameter(
                "server",
                utoipa::openapi::ServerVariableBuilder::new()
                    .default_value("https://geoengine.io")
                    .build(),
            )
            .build(),
    ]);

    println!("{}", serde_json::to_string_pretty(&spec)?);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::{Path, PathBuf},
        process::{Command, Stdio},
    };

    /// Adapted from `https://docs.rs/assert_cmd/latest/src/assert_cmd/cargo.rs.html#222-233`
    // TODO: find out proper function from `assert_cmd` crate
    fn cargo_bin_str(name: &str) -> PathBuf {
        use std::env;

        // Adapted from
        // https://github.com/rust-lang/cargo/blob/485670b3983b52289a2f353d589c57fae2f60f82/tests/testsuite/support/mod.rs#L507
        fn target_dir() -> PathBuf {
            env::current_exe()
                .ok()
                .map(|mut path| {
                    path.pop();
                    if path.ends_with("deps") {
                        path.pop();
                    }
                    path
                })
                .expect("this should only be used where a `current_exe` can be set")
        }

        let env_var = format!("CARGO_BIN_EXE_{name}");
        env::var_os(env_var).map_or_else(
            || target_dir().join(format!("{}{}", name, env::consts::EXE_SUFFIX)),
            std::convert::Into::into,
        )
    }

    #[test]
    fn it_generates_json() {
        let cli_result = Command::new(cargo_bin_str("geoengine-cli"))
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
