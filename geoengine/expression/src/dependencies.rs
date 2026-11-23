use crate::{
    error::{self, ExpressionExecutionError},
    toolchain::write_minimal_toolchain_file,
};
use snafu::{ResultExt, ensure};
use std::{
    fs::{File, Permissions},
    io::BufWriter,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

pub type Result<T, E = ExpressionExecutionError> = std::result::Result<T, E>;

const RUST_TOOLCHAIN_TOML: &str = std::include_str!("../../rust-toolchain.toml");

/// The libraries built by `build.rs`
const DEFAULT_DEPENDENCIES_DIR: &str = env!("EXPRESSION_DEPS_DIR");
/// Identifies the build of the dependencies, see `build.rs`
const FINGERPRINT: &str = env!("EXPRESSION_DEPS_FINGERPRINT");
/// Must match `FINGERPRINT_FILE` in `build.rs`
const FINGERPRINT_FILE: &str = "FINGERPRINT";
/// `rustc -V` of the build of the dependencies
const RUSTC_VERSION: &str = env!("EXPRESSION_DEPS_RUSTC_VERSION");

const WORKSPACE_PREFIX: &str = "geoengine-expression-";
const LOCK_FILE: &str = ".lock";
/// A new workspace is locked right after its creation, so only an older one can be abandoned.
const MIN_STALE_AGE: Duration = Duration::from_secs(60);

/// A workspace for the [`LinkedExpression`](crate::LinkedExpression)s, which link against the
/// dependencies that `build.rs` built.
///
/// If this is dropped, the workspace will be deleted.
/// If the process dies before that, the next [`ExpressionDependencies::new`] deletes it.
///
#[derive(Debug)]
pub struct ExpressionDependencies {
    // we need to hold this to keep the tempdir alive
    workspace: tempfile::TempDir,
    // the file lock marks the workspace as alive, the kernel releases it even on `SIGKILL`
    _lock: File,
    dependencies: PathBuf,
}

impl ExpressionDependencies {
    /// The directory with the dependencies that `build.rs` built.
    /// It is in the build directory, so a deployment copies it and passes its path to [`Self::new`].
    pub fn default_dependencies_dir() -> &'static Path {
        Path::new(DEFAULT_DEPENDENCIES_DIR)
    }

    /// Creates the workspace in the `parent` directory, which is created if necessary.
    /// All processes using the same directory clean up after each other.
    ///
    /// `dependencies` is the directory with the libraries that `build.rs` built for this binary,
    /// e.g. [`Self::default_dependencies_dir`].
    /// Nobody but the owner of the server may write to it, since the server loads code from it.
    pub fn new(parent: &Path, dependencies: &Path) -> Result<Self> {
        // `rustc` runs in the workspace, so relative paths would point elsewhere
        let parent = std::path::absolute(parent).context(error::TempDir)?;
        let parent = parent.as_path();
        let dependencies = std::path::absolute(dependencies).context(error::DepsMissing {
            path: dependencies.to_path_buf(),
        })?;
        let dependencies = dependencies.as_path();

        std::fs::create_dir_all(parent).context(error::TempDir)?;

        let workspace = tempfile::Builder::new()
            .prefix(WORKSPACE_PREFIX)
            // the server loads and runs the libraries in here, so nobody else may touch them
            .permissions(Permissions::from_mode(0o700))
            .tempdir_in(parent)
            .context(error::TempDir)?;

        let lock = Self::lock_workspace(workspace.path()).context(error::DepsWorkspace)?;

        remove_stale_workspaces(parent, MIN_STALE_AGE);

        // the expressions are compiled in here, so the toolchain file selects their `rustc`
        write_minimal_toolchain_file(
            &mut BufWriter::new(
                File::create(workspace.path().join("rust-toolchain.toml"))
                    .context(error::DepsWorkspace)?,
            ),
            RUST_TOOLCHAIN_TOML,
        )
        .context(error::DepsWorkspace)?;

        check_dependencies(dependencies)?;
        check_rustc_version(workspace.path())?;

        Ok(Self {
            workspace,
            _lock: lock,
            dependencies: dependencies.to_path_buf(),
        })
    }

    pub fn linker_path(&self) -> &Path {
        self.dependencies.as_path()
    }

    pub(crate) fn workspace_path(&self) -> &Path {
        self.workspace.path()
    }

    fn lock_workspace(workspace: &Path) -> Result<File, std::io::Error> {
        let lock = File::create(workspace.join(LOCK_FILE))?;
        lock.lock()?;
        Ok(lock)
    }
}

/// Fails if the directory is missing or was built for another binary.
/// Otherwise, `rustc` would fail later with errors about incompatible crates.
fn check_dependencies(dependencies: &Path) -> Result<()> {
    let fingerprint = std::fs::read_to_string(dependencies.join(FINGERPRINT_FILE)).context(
        error::DepsMissing {
            path: dependencies.to_path_buf(),
        },
    )?;

    ensure!(
        fingerprint.trim() == FINGERPRINT,
        error::DepsMismatch {
            path: dependencies.to_path_buf(),
            expected: FINGERPRINT,
            actual: fingerprint.trim(),
        }
    );

    Ok(())
}

/// Fails if the `rustc` that compiles the expressions differs from the one that built the dependencies
fn check_rustc_version(workspace: &Path) -> Result<()> {
    let output = Command::new("rustc")
        .current_dir(workspace)
        .arg("-V")
        .output()
        .context(error::Compiler)?;

    let version = String::from_utf8_lossy(&output.stdout);

    ensure!(
        output.status.success() && version.trim() == RUSTC_VERSION,
        error::RustcMismatch {
            expected: RUSTC_VERSION,
            actual: if output.status.success() {
                version.trim().to_string()
            } else {
                String::from_utf8_lossy(&output.stderr).trim().to_string()
            },
        }
    );

    Ok(())
}

/// Deletes the workspaces of dead processes, since these never dropped their [`ExpressionDependencies`].
/// This is best effort and must not prevent the creation of a new workspace.
fn remove_stale_workspaces(parent: &Path, min_age: Duration) {
    let entries = match std::fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!("Cannot search for stale expression workspaces: {e}");
            return;
        }
    };

    for entry in entries.flatten() {
        let is_workspace = entry
            .file_name()
            .to_string_lossy()
            .starts_with(WORKSPACE_PREFIX)
            // does not follow symlinks
            && entry.file_type().is_ok_and(|t| t.is_dir());
        if !is_workspace {
            continue;
        }

        // the workspaces of other users are not accessible
        let Ok(lock) = File::open(entry.path().join(LOCK_FILE)) else {
            continue;
        };
        // the lock file's age, because the directory's changes with every library
        let is_old_enough = lock
            .metadata()
            .and_then(|m| m.modified())
            .is_ok_and(|modified| modified.elapsed().is_ok_and(|age| age >= min_age));
        if !is_old_enough || lock.try_lock().is_err() {
            continue; // the owner is alive
        }

        match std::fs::remove_dir_all(entry.path()) {
            Ok(()) => tracing::info!("Removed stale expression workspace {:?}", entry.path()),
            Err(e) => tracing::warn!(
                "Cannot remove stale expression workspace {:?}: {e}",
                entry.path()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_uses_the_prebuilt_dependencies() {
        let deps = ExpressionDependencies::new(
            &std::env::temp_dir(),
            ExpressionDependencies::default_dependencies_dir(),
        )
        .unwrap();

        assert!(deps.linker_path().join(FINGERPRINT_FILE).exists());
        assert!(
            deps.workspace_path().join("rust-toolchain.toml").exists(),
            "the toolchain file selects the `rustc` of the expressions"
        );

        let permissions = std::fs::metadata(deps.workspace_path())
            .unwrap()
            .permissions();
        assert_eq!(permissions.mode() & 0o777, 0o700);
    }

    #[test]
    fn it_rejects_missing_dependencies() {
        let workspaces = tempfile::tempdir().unwrap();
        let missing = workspaces.path().join("missing");

        let result = ExpressionDependencies::new(workspaces.path(), &missing);

        assert!(
            matches!(result, Err(ExpressionExecutionError::DepsMissing { path, .. }) if path == missing)
        );
    }

    #[test]
    fn it_rejects_dependencies_of_another_build() {
        let workspaces = tempfile::tempdir().unwrap();
        let dependencies = tempfile::tempdir().unwrap();
        std::fs::write(
            dependencies.path().join(FINGERPRINT_FILE),
            "0123456789abcdef",
        )
        .unwrap();

        let result = ExpressionDependencies::new(workspaces.path(), dependencies.path());

        assert!(matches!(
            result,
            Err(ExpressionExecutionError::DepsMismatch { actual, .. }) if actual == "0123456789abcdef"
        ));
    }

    #[test]
    fn it_removes_only_workspaces_of_dead_processes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let workspace = |name: &str| {
            let path = temp_dir.path().join(format!("{WORKSPACE_PREFIX}{name}"));
            std::fs::create_dir(&path).unwrap();
            File::create(path.join(LOCK_FILE)).unwrap();
            path
        };

        let dead = workspace("dead");

        let alive = workspace("alive");
        let alive_lock = File::open(alive.join(LOCK_FILE)).unwrap();
        alive_lock.lock().unwrap();

        let without_lock_file = temp_dir.path().join(format!("{WORKSPACE_PREFIX}foreign"));
        std::fs::create_dir(&without_lock_file).unwrap();

        let other_prefix = temp_dir.path().join("other");
        std::fs::create_dir(&other_prefix).unwrap();
        File::create(other_prefix.join(LOCK_FILE)).unwrap();

        let link_target = tempfile::tempdir().unwrap();
        File::create(link_target.path().join(LOCK_FILE)).unwrap();
        let link = temp_dir.path().join(format!("{WORKSPACE_PREFIX}link"));
        std::os::unix::fs::symlink(link_target.path(), &link).unwrap();

        remove_stale_workspaces(temp_dir.path(), Duration::ZERO);

        assert!(!dead.exists());
        assert!(alive.exists());
        assert!(without_lock_file.exists());
        assert!(other_prefix.exists());
        assert!(link_target.path().join(LOCK_FILE).exists());

        let too_young = workspace("young");
        remove_stale_workspaces(temp_dir.path(), Duration::from_secs(3600));
        assert!(too_young.exists());

        // the directory was just changed, e.g. by building a library, but the owner is long dead
        let recently_changed = workspace("changed");
        File::options()
            .write(true)
            .open(recently_changed.join(LOCK_FILE))
            .unwrap()
            .set_modified(std::time::SystemTime::now() - Duration::from_secs(7200))
            .unwrap();
        std::fs::create_dir(recently_changed.join("library")).unwrap();
        remove_stale_workspaces(temp_dir.path(), Duration::from_secs(3600));
        assert!(!recently_changed.exists());
    }
}
