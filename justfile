mod api-clients
mod backend 'geoengine'
mod common 'common.justfile'
mod python
mod www

_default:
    @just --list --list-submodules

# Install dependencies for all submodules.
[group("install")]
install: backend::install python::install

# Call lint for all submodules.
[group("lint")]
lint: lint-justfiles lint-openapi-spec backend::lint python::lint www::lint

# Validate OpenAPI spec.
[group("lint")]
lint-openapi-spec: api-clients::lint-openapi-spec

# Check if justfiles are formatted correctly.
[group("lint")]
lint-justfiles: format-justfiles

# Check the format for all submodules. Format them with `--write`.
[arg("write", long="write", value="true", help="Whether to write the formatted files back to disk.")]
[group("lint")]
format write="false": (format-justfiles write) (www::format write)

# Check if justfiles are formatted correctly. Format them with `--write`.
[arg("write", long="write", value="true", help="Whether to write the formatted files back to disk.")]
[group("lint")]
format-justfiles write="false": common::_clear
    just --fmt {{ if write != "true" { "--check" } else { "" } }} --unstable --justfile justfile
    just --fmt {{ if write != "true" { "--check" } else { "" } }} --unstable --justfile common.justfile
    just --fmt {{ if write != "true" { "--check" } else { "" } }} --unstable --justfile geoengine/justfile
    just --fmt {{ if write != "true" { "--check" } else { "" } }} --unstable --justfile python/justfile

# Build all submodules.
[group("build")]
build: backend::build backend::generate-openapi-spec api-clients::build python::build

VERSION_CMD := "cargo metadata --manifest-path geoengine/Cargo.toml --format-version=1 --no-deps | jq -r '.packages[0].version'"
PYTHON_VERSION_CMD := 'pipx run toml-cli get --toml-path "python/pyproject.toml" "project.version"'
PYTHON_DEP_VERSION_CMD := 'pipx run toml-cli get --toml-path "python/pyproject.toml" "project.dependencies[0]" | sed -E "s/.*==\s*//"'
API_CLIENT_VERSION_CMD := "python -c \"import configparser;cp=configparser.ConfigParser();cp.read('api-clients/.generation/config.ini');print(cp['general']['version'])\""

# Increases the version number for Geo Engine and then updates api-clients, Python, UI, etc. accordingly.
increase-version-number: common::_clear
    cargo set-version \
        --bump patch \
        --exclude geoengine-expression-deps \
        --manifest-path geoengine/Cargo.toml

    @echo "New version: `{{ VERSION_CMD }}`"

    just _increase-version-number `{{ VERSION_CMD }}`

_increase-version-number version: (api-clients::update-config version)
    pipx run toml-cli set --toml-path "python/pyproject.toml" "project.version" "{{ version }}"
    pipx run toml-cli set --toml-path "python/pyproject.toml" "project.dependencies[0]" "geoengine-api-client == {{ version }}"

[group("lint")]
lint-version-numbers: common::_clear
    @echo "Checking if version numbers are consistent across the project ({{ shell(VERSION_CMD) }})…"
    @{{ if shell(VERSION_CMD) == shell(PYTHON_VERSION_CMD) { 'echo "Python library version is consistent."' } else { error("Python library has wrong version (" + shell(PYTHON_VERSION_CMD) + ").") } }}
    @{{ if shell(VERSION_CMD) == shell(PYTHON_DEP_VERSION_CMD) { 'echo "Python library dependency is consistent."' } else { error("Python library dependency has wrong version (" + shell(PYTHON_DEP_VERSION_CMD) + ").") } }}
    @{{ if shell(VERSION_CMD) == shell(API_CLIENT_VERSION_CMD) { 'echo "API client version is consistent."' } else { error("API client has wrong version (" + shell(API_CLIENT_VERSION_CMD) + ").") } }}

# Check if there are uncommitted changes in the git repository. If there are, print an error message and exit with a non-zero status code. Otherwise, print a success message.
[group('CI')]
check-no-changes-in-git-repo:
    #!/usr/bin/env bash
    if [ -n "$(git status --porcelain)" ]; then
      echo "Error: Uncommitted changes found in git repository."
      git status --porcelain
      exit 1
    else
      echo "No uncommitted changes found in git repository."
    fi
