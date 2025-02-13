#!/usr/bin/env -S cargo +nightly -Zscript

---cargo
[package]
edition = "2024"

[dependencies]
tempfile = "3.15"
---
//! This script updates the dependencies for the `expression/deps-workspace` by performing the following steps:
//! 
//! 1. Creates a temporary directory.
//! 2. Copies all files from the `expression/deps-workspace` directory to the temporary directory.
//! 4. Runs `cargo update` in the temporary directory to update the dependencies.
//! 5. Copies the updated `Cargo.lock` file back to the `expression/deps-workspace` directory.
//! 
//! If any step fails, the script will print an error message and exit with a non-zero status code.

use std::fs;
use std::path::Path;

fn main() {
    let temp_dir = tempfile::tempdir().unwrap();

    let deps_workspace = Path::new("expression/deps-workspace");

    if !deps_workspace.exists() || !deps_workspace.is_dir() {
        eprintln!("Dependencies workspace does not exist at {:?}", deps_workspace);
        std::process::exit(1);
    }

    for entry in fs::read_dir(deps_workspace).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            fs::copy(&path, temp_dir.path().join(path.file_name().unwrap())).unwrap();
        }
    }

    eprintln!("Copied dependencies workspace to {:?}", temp_dir);

    // Run `cargo update` in the temporary directory

    let status = std::process::Command::new("cargo")
        .arg("update")
        .current_dir(temp_dir.path())
        .status()
        .unwrap();

    if !status.success() {
        eprintln!("Failed to update dependencies");
        std::process::exit(1);
    }

    eprintln!("Updated dependencies successfully");

    // Copy the updated lockfile back to the workspace

    fs::copy(temp_dir.path().join("Cargo.lock"), deps_workspace.join("Cargo.lock")).unwrap();

    eprintln!("Copied updated lockfile back to {:?}", deps_workspace);
}
