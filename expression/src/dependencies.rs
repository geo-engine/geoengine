use crate::error::{self, ExpressionExecutionError};
use cargo::{
    core::{
        compiler::{BuildConfig, MessageFormat},
        Shell, Workspace,
    },
    ops::CompileOptions,
    util::command_prelude::CompileMode,
};
use snafu::{OptionExt, ResultExt, Whatever};
use std::path::{Path, PathBuf};

pub type Result<T, E = ExpressionExecutionError> = std::result::Result<T, E>;

const DEPS_CARGO_TOML: &[u8] = std::include_bytes!("../deps-workspace/Cargo.toml");
const DEPS_CARGO_LOCK: &[u8] = std::include_bytes!("../deps-workspace/Cargo.lock");
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

        let dependencies = Self::cargo_build(cargo_workspace.path()).map_err(|e| {
            ExpressionExecutionError::DepsBuild {
                debug: format!("{e:?}"),
            }
        })?;

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

    /// Builds the dependencies workspace.
    /// We will use the libraries in the `target/release/deps/` folder.
    ///
    /// We return [`Whatever`] since [`cargo::util::errors::CargoResult`] has a lifetime.
    ///
    fn cargo_build(cargo_workspace: &Path) -> Result<PathBuf, Whatever> {
        let homedir = cargo::util::homedir(cargo_workspace)
            .whatever_context("Could not find home directory, e.g. $HOME")?;

        // TODO: make shell output configurable?
        let dev_null_shell = Shell::from_write(Box::new(std::io::empty()));
        let cargo_config =
            cargo::util::config::Config::new(dev_null_shell, cargo_workspace.into(), homedir);

        let workspace = Workspace::new(&cargo_workspace.join("Cargo.toml"), &cargo_config)
            .whatever_context("Invalid workspace")?;

        let mut build_config = BuildConfig::new(&cargo_config, None, true, &[], CompileMode::Build)
            .whatever_context("Invalid cargo build config")?;
        build_config.requested_profile = "release".into();
        build_config.message_format = MessageFormat::Short;

        let mut compile_options = CompileOptions::new(&cargo_config, CompileMode::Build)
            .whatever_context("Invalid compile options")?;
        compile_options.build_config = build_config;

        let compilation_result = cargo::ops::compile(&workspace, &compile_options)
            .whatever_context("Compilation failed")?;

        debug_assert_eq!(
            compilation_result.deps_output.keys().len(),
            1,
            "Expected only one deps output"
        );

        compilation_result
            .deps_output
            .values()
            .next()
            .cloned()
            .whatever_context("Missing deps output")
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
