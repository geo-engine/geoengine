use crate::error::{self, ExpressionExecutionError};
use snafu::ResultExt;
use std::path::{Path, PathBuf};

pub type Result<T, E = ExpressionExecutionError> = std::result::Result<T, E>;

const DEPS_CARGO_TOML: &[u8] = std::include_bytes!("../deps-workspace/Cargo.toml");
const DEPS_CARGO_LOCK: &[u8] = std::include_bytes!("../../Cargo.lock");
const DEPS_LIB_RS: &[u8] = std::include_bytes!("../deps-workspace/lib.rs");

/// A pre-built workspace for linking dependencies.
///
/// If this is dropped, the workspace will be deleted.
///
#[derive(Debug)]
pub struct ExpressionDependencies {
    // we need to hold this to keep the tempdir alive
    _cargo_workspace: tempfile::TempDir,
    dependencies: PathBuf,
}

impl ExpressionDependencies {
    pub fn new() -> Result<Self> {
        let cargo_workspace = tempfile::tempdir().context(error::TempDir)?;

        Self::copy_deps_workspace(cargo_workspace.path()).context(error::DepsWorkspace)?;

        // build the dependencies using cargo throuhh a subprocess
        // note, that we do not use the cargo crate here, because it led to a deadlock in tests
        let output = std::process::Command::new("cargo")
            .current_dir(cargo_workspace.path())
            .arg("build")
            .arg("--release")
            .arg("--offline")
            .output()
            .map_err(|e| ExpressionExecutionError::DepsBuild {
                debug: format!("{e:?}"),
            })?;

        if !output.status.success() {
            return Err(ExpressionExecutionError::DepsBuild {
                debug: String::from_utf8_lossy(&output.stderr).to_string(),
            });
        }

        let dependencies = cargo_workspace.path().join("target/release/deps/");

        Ok(Self {
            _cargo_workspace: cargo_workspace,
            dependencies,
        })
    }

    pub fn linker_path(&self) -> &Path {
        self.dependencies.as_path()
    }

    fn copy_deps_workspace(cargo_workspace: &Path) -> Result<(), std::io::Error> {
        std::fs::write(cargo_workspace.join("Cargo.toml"), DEPS_CARGO_TOML)?;
        std::fs::write(cargo_workspace.join("Cargo.lock"), DEPS_CARGO_LOCK)?;
        std::fs::write(cargo_workspace.join("lib.rs"), DEPS_LIB_RS)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_compiles_dependencies() {
        let deps = ExpressionDependencies::new().unwrap();

        assert!(deps.linker_path().exists());
        assert!(deps.linker_path().ends_with("target/release/deps/"));
    }
}
