//! Builds the dependencies of the expressions (`deps-workspace`) once at build time.
//!
//! The libraries are put into `$OUT_DIR/expression-deps`, which the expressions link against at runtime.
//! A deployment copies this directory next to the binary and sets `[expression] dependencies_path`.

use std::{
    ffi::OsStr,
    fs::File,
    hash::{DefaultHasher, Hash, Hasher},
    io::BufWriter,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

#[path = "src/toolchain.rs"]
mod toolchain;

const DEPS_WORKSPACE_FILES: [&str; 3] = ["Cargo.toml", "Cargo.lock", "lib.rs"];
const RUST_TOOLCHAIN_TOML: &str = "../rust-toolchain.toml";
/// Must match `FINGERPRINT_FILE` in `src/dependencies.rs`
const FINGERPRINT_FILE: &str = "FINGERPRINT";

fn main() {
    for file in DEPS_WORKSPACE_FILES {
        println!("cargo::rerun-if-changed=deps-workspace/{file}");
    }
    println!("cargo::rerun-if-changed={RUST_TOOLCHAIN_TOML}");
    println!("cargo::rerun-if-changed=src/toolchain.rs");
    println!("cargo::rerun-if-env-changed=RUSTUP_TOOLCHAIN");

    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").expect("cargo sets `OUT_DIR`"));
    let workspace = out_dir.join("deps-workspace");
    let target_dir = out_dir.join("deps-target");
    let deps_dir = out_dir.join("expression-deps");

    let rustc_version = copy_deps_workspace(&workspace);
    let artifacts = build_deps_workspace(&workspace, &target_dir);
    let fingerprint = fingerprint(&rustc_version);

    // only the libraries of this build, so `rustc` never finds two candidates for a crate
    if deps_dir.exists() {
        std::fs::remove_dir_all(&deps_dir).expect("cannot clear the dependencies directory");
    }
    std::fs::create_dir_all(&deps_dir).expect("cannot create the dependencies directory");
    for artifact in artifacts {
        let file_name = artifact.file_name().expect("artifacts are files");
        std::fs::copy(&artifact, deps_dir.join(file_name))
            .unwrap_or_else(|e| panic!("cannot copy {}: {e}", artifact.display()));
    }
    std::fs::write(deps_dir.join(FINGERPRINT_FILE), &fingerprint)
        .expect("cannot write the fingerprint");

    println!(
        "cargo::rustc-env=EXPRESSION_DEPS_DIR={}",
        deps_dir.to_str().expect("`OUT_DIR` is valid UTF-8")
    );
    println!("cargo::rustc-env=EXPRESSION_DEPS_FINGERPRINT={fingerprint}");
    println!("cargo::rustc-env=EXPRESSION_DEPS_RUSTC_VERSION={rustc_version}");
}

/// Writes the workspace and returns the version of its `rustc`, which compiles the expressions later
fn copy_deps_workspace(workspace: &Path) -> String {
    std::fs::create_dir_all(workspace).expect("cannot create the dependencies workspace");

    for file in DEPS_WORKSPACE_FILES {
        let mut content = std::fs::read(Path::new("deps-workspace").join(file))
            .unwrap_or_else(|e| panic!("cannot read deps-workspace/{file}: {e}"));
        if file == "Cargo.toml" {
            // keeps cargo from searching a parent workspace
            content.extend_from_slice(b"\n[workspace]\n");
        }
        std::fs::write(workspace.join(file), content)
            .unwrap_or_else(|e| panic!("cannot write {file}: {e}"));
    }

    let rust_toolchain_toml =
        std::fs::read_to_string(RUST_TOOLCHAIN_TOML).expect("cannot read `rust-toolchain.toml`");
    toolchain::write_minimal_toolchain_file(
        &mut BufWriter::new(
            File::create(workspace.join("rust-toolchain.toml"))
                .expect("cannot create `rust-toolchain.toml`"),
        ),
        &rust_toolchain_toml,
    )
    .expect("cannot write `rust-toolchain.toml`");

    let output = clean_command("rustc", workspace)
        .arg("-V")
        .output()
        .expect("cannot run `rustc -V`");
    assert!(output.status.success(), "`rustc -V` failed");
    String::from_utf8(output.stdout)
        .expect("`rustc -V` prints UTF-8")
        .trim()
        .to_string()
}

/// Builds the workspace and returns the libraries that `rustc` needs to link an expression
fn build_deps_workspace(workspace: &Path, target_dir: &Path) -> Vec<PathBuf> {
    let output = clean_command("cargo", workspace)
        .args(["build", "--release", "--locked"])
        .arg("--target-dir")
        .arg(target_dir)
        .arg("--message-format=json-render-diagnostics")
        .stderr(Stdio::inherit())
        .output()
        .expect("cannot run `cargo build` for the expression dependencies");
    assert!(
        output.status.success(),
        "cannot build the expression dependencies, see the output above"
    );

    let stdout = String::from_utf8(output.stdout).expect("cargo prints UTF-8");
    let mut artifacts = Vec::new();
    for line in stdout.lines() {
        let message: serde_json::Value =
            serde_json::from_str(line).expect("cargo prints one JSON message per line");
        if message["reason"] != "compiler-artifact" {
            continue;
        }
        let Some(filenames) = message["filenames"].as_array() else {
            continue;
        };
        artifacts.extend(
            filenames
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(PathBuf::from)
                // `.rlib`s of libraries, `.so`s of proc macros; `.rmeta`s are only for pipelining
                .filter(|path| {
                    path.extension()
                        .is_some_and(|ext| ext == OsStr::new("rlib") || ext == OsStr::new("so"))
                }),
        );
    }
    assert!(!artifacts.is_empty(), "cargo reported no libraries");
    artifacts
}

/// A command that does not inherit the settings of the outer build, e.g. the coverage
/// instrumentation of `cargo llvm-cov` or the target directory.
/// The libraries must be built as at runtime, where the server only calls `rustc`.
fn clean_command(program: &str, current_dir: &Path) -> Command {
    const KEEP: [&str; 6] = [
        "CARGO_HOME",
        "CARGO_MAKEFLAGS", // the jobserver of the outer build
        "CARGO_NET_",
        "CARGO_HTTP_",
        "CARGO_REGISTRIES_",
        "CARGO_REGISTRY_",
    ];
    const REMOVE: [&str; 5] = [
        "CARGO",
        "RUSTC",
        "RUSTDOC",
        "RUSTFLAGS",
        "LLVM_PROFILE_FILE",
    ];

    let mut command = Command::new(program);
    command.current_dir(current_dir);
    for (key, _) in std::env::vars_os() {
        let Some(key) = key.to_str() else {
            continue;
        };
        let keep = KEEP.iter().any(|prefix| key.starts_with(prefix));
        let remove = REMOVE.iter().any(|prefix| key.starts_with(prefix));
        if remove && !keep {
            command.env_remove(key);
        }
    }
    command
}

/// Identifies the inputs of this build, so the server notices a directory that belongs to another binary
fn fingerprint(rustc_version: &str) -> String {
    let mut hasher = DefaultHasher::new();
    for file in DEPS_WORKSPACE_FILES {
        std::fs::read(Path::new("deps-workspace").join(file))
            .unwrap_or_else(|e| panic!("cannot read deps-workspace/{file}: {e}"))
            .hash(&mut hasher);
    }
    rustc_version.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}
