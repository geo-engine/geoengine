use crate::{
    error::{self, ExpressionExecutionError},
    util::write_minimal_toolchain_file,
};
use snafu::ResultExt;
use std::{
    fs::{File, Permissions},
    io::BufWriter,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::Duration,
};

pub type Result<T, E = ExpressionExecutionError> = std::result::Result<T, E>;

const DEPS_CARGO_TOML: &[u8] = std::include_bytes!("../deps-workspace/Cargo.toml");
const DEPS_CARGO_LOCK: &[u8] = std::include_bytes!("../deps-workspace/Cargo.lock");
const DEPS_LIB_RS: &[u8] = std::include_bytes!("../deps-workspace/lib.rs");
const RUST_TOOLCHAIN_TOML: &str = std::include_str!("../../rust-toolchain.toml");

const WORKSPACE_PREFIX: &str = "geoengine-expression-";
const LOCK_FILE: &str = ".lock";
/// A new workspace is locked right after its creation, so only an older one can be abandoned.
const MIN_STALE_AGE: Duration = Duration::from_secs(60);

/// A pre-built workspace for linking dependencies.
/// It also contains the libraries of all [`LinkedExpression`](crate::LinkedExpression)s built with it.
///
/// If this is dropped, the workspace will be deleted.
/// If the process dies before that, the next [`ExpressionDependencies::new`] deletes it.
///
#[derive(Debug)]
pub struct ExpressionDependencies {
    // we need to hold this to keep the tempdir alive
    cargo_workspace: tempfile::TempDir,
    // the file lock marks the workspace as alive, the kernel releases it even on `SIGKILL`
    _lock: File,
    dependencies: PathBuf,
}

impl ExpressionDependencies {
    /// Creates the workspace in the `parent` directory, which is created if necessary.
    /// All processes using the same directory clean up after each other.
    pub fn new(parent: &Path) -> Result<Self> {
        std::fs::create_dir_all(parent).context(error::TempDir)?;

        let cargo_workspace = tempfile::Builder::new()
            .prefix(WORKSPACE_PREFIX)
            // the server loads and runs the libraries in here, so nobody else may touch them
            .permissions(Permissions::from_mode(0o700))
            .tempdir_in(parent)
            .context(error::TempDir)?;

        let lock = Self::lock_workspace(cargo_workspace.path()).context(error::DepsWorkspace)?;

        remove_stale_workspaces(parent, MIN_STALE_AGE);

        Self::copy_deps_workspace(cargo_workspace.path()).context(error::DepsWorkspace)?;

        // build the dependencies using cargo through a subprocess
        // note, that we do not use the cargo crate here, because it led to a deadlock in tests
        let output = std::process::Command::new("cargo")
            .current_dir(cargo_workspace.path())
            .arg("build")
            .arg("--release")
            .arg("--locked")
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
            cargo_workspace,
            _lock: lock,
            dependencies,
        })
    }

    pub fn linker_path(&self) -> &Path {
        self.dependencies.as_path()
    }

    pub(crate) fn workspace_path(&self) -> &Path {
        self.cargo_workspace.path()
    }

    fn lock_workspace(cargo_workspace: &Path) -> Result<File, std::io::Error> {
        let lock = File::create(cargo_workspace.join(LOCK_FILE))?;
        lock.lock()?;
        Ok(lock)
    }

    fn copy_deps_workspace(cargo_workspace: &Path) -> Result<(), std::io::Error> {
        // the `[workspace]` keeps cargo from searching a parent workspace if the path is inside of one
        std::fs::write(
            cargo_workspace.join("Cargo.toml"),
            [DEPS_CARGO_TOML, b"\n[workspace]\n"].concat(),
        )?;
        std::fs::write(cargo_workspace.join("Cargo.lock"), DEPS_CARGO_LOCK)?;
        std::fs::write(cargo_workspace.join("lib.rs"), DEPS_LIB_RS)?;

        write_minimal_toolchain_file(
            &mut BufWriter::new(File::create(cargo_workspace.join("rust-toolchain.toml"))?),
            RUST_TOOLCHAIN_TOML,
        )?;

        Ok(())
    }
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
    fn it_compiles_dependencies() {
        let deps = ExpressionDependencies::new(&std::env::temp_dir()).unwrap();

        assert!(deps.linker_path().exists());
        assert!(deps.linker_path().ends_with("target/release/deps/"));

        let permissions = std::fs::metadata(deps.workspace_path())
            .unwrap()
            .permissions();
        assert_eq!(permissions.mode() & 0o777, 0o700);
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
