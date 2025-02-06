#!/usr/bin/env -S cargo +nightly -Zscript

---cargo
[package]
edition = "2024"

[dependencies]
toml = { version = "0.8", features = ["parse"] }
---
//! This script checks if the `geo` and `geo-types` versions in the workspace and dependencies workspace match.
//! 
//! In more detail, it does the following:
//! 
//! 1. Parses the `Cargo.lock` file in the workspace and dependencies workspace.
//! 2. Extracts the versions of the `geo` and `geo-types` packages.
//! 3. Compares the versions by checking if there is an overlap between the versions in the two lockfiles.
//!
//! If any step fails, the script will print an error message and exit with a non-zero status code.

use std::fs;
use std::path::Path;
use toml::Table;

fn main() {
    let deps_workspace_lockfile = Path::new("expression/deps-workspace/Cargo.lock");
    let workspace_lockfile = Path::new("Cargo.lock");

    if !deps_workspace_lockfile.exists() || !deps_workspace_lockfile.is_file() {
        eprintln!("`Cargo.lock` in dependencies workspace does not exist at {deps_workspace_lockfile:?}");
        std::process::exit(1);
    }

    if !workspace_lockfile.exists() || !workspace_lockfile.is_file() {
        eprintln!("`Cargo.lock` in workspace does not exist at {workspace_lockfile:?}");
        std::process::exit(1);
    }

    let workspace_geo_versions = find_geo_versions(workspace_lockfile);
    let deps_workspace_geo_versions = find_geo_versions(deps_workspace_lockfile);

    assert!(
        workspace_geo_versions.geo_overlaps(&deps_workspace_geo_versions),
        "`geo` versions in workspace and dependencies workspace do not match"
    );

    assert!(
        workspace_geo_versions.geo_types_overlaps(&deps_workspace_geo_versions),
        "`geo-types` versions in workspace and dependencies workspace do not match"
    );

    eprintln!("`geo` versions in workspace and dependencies workspace match");
}

fn find_geo_versions(path: &Path) -> GeoVersions {
    let lockfile = fs::read_to_string(path).unwrap().parse::<Table>().unwrap();

    let packages = lockfile["package"].as_array().unwrap();

    let mut versions = GeoVersions {
        geo: Vec::new(),
        geo_types: Vec::new(),
    };

    for package in packages {
        let version_vec: &mut Vec<String> = match package["name"].as_str().unwrap()  {
            "geo" => &mut versions.geo,
            "geo-types" => &mut versions.geo_types,
            _ => continue,
        };

        version_vec.push(package["version"].as_str().unwrap().to_string());
    }

    versions
}

#[derive(Debug)]
struct GeoVersions {
    pub geo: Vec<String>,
    pub geo_types: Vec<String>,
}

impl GeoVersions {
    pub fn geo_overlaps(&self, other: &Self) -> bool {
        for geo_version in &self.geo {
            if other.geo.contains(&geo_version) {
                return true;
            }
        }
        return false;
    }

    pub fn geo_types_overlaps(&self, other: &Self) -> bool {
        for geo_version in &self.geo_types {
            if other.geo_types.contains(&geo_version) {
                return true;
            }
        }
        return false;
    }
}
