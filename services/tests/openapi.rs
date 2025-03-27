#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::print_stderr)] // okay in tests

use assert_cmd::cargo::CommandCargoExt;
use geoengine_services::test_data;
use pretty_assertions::assert_eq;
use std::process::{Command, Stdio};

#[tokio::test]
async fn it_has_the_latest_openapi_schema_stored_in_the_repository() {
    // change cwd s.t. the config file can be found
    std::env::set_current_dir(test_data!("..")).expect("failed to set current directory");

    let startup_result = Command::cargo_bin("geoengine-cli")
        .unwrap()
        .args(["openapi"])
        .stdout(Stdio::piped())
        .output()
        .unwrap();

    assert!(
        startup_result.status.success(),
        "failed to output openapi schema from CLI: {startup_result:?}",
    );

    let spec_from_cli = String::from_utf8(startup_result.stdout).unwrap();
    let spec_from_file = include_str!("../../openapi.json");

    assert_eq!(spec_from_cli, spec_from_file);
}
