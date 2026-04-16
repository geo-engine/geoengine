mod api-clients
mod backend 'geoengine'
mod common 'common.justfile'
mod python

_default:
    @just --list --list-submodules

# Install dependencies for all submodules.
[group("install")]
install: backend::install python::install

# Validate OpenAPI spec.
[group("lint")]
lint-openapi-spec: api-clients::lint-openapi-spec

# Call lint for all submodules.
[group("lint")]
lint: lint-justfiles lint-openapi-spec backend::lint python::lint

# Check if justfiles are formatted correctly.
[group("lint")]
lint-justfiles:
    just --fmt --check --unstable --justfile justfile
    just --fmt --check --unstable --justfile common.justfile
    just --fmt --check --unstable --justfile geoengine/justfile
    just --fmt --check --unstable --justfile python/justfile

# Call format for all submodules.
[group("format")]
format: format-justfiles

# Format justfiles.
[group("format")]
format-justfiles:
    just --fmt --unstable --justfile justfile
    just --fmt --unstable --justfile common.justfile
    just --fmt --unstable --justfile geoengine/justfile
    just --fmt --unstable --justfile python/justfile

# Build all submodules.
[group("build")]
build: backend::build backend::generate-openapi-spec api-clients::build python::build

VERSION_CMD := "cargo metadata --manifest-path geoengine/Cargo.toml --format-version=1 --no-deps | jq -r '.packages[0].version'"

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