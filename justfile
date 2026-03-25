_default:
    @just --list

# Clear the terminal before executing a command. Does not fail in a CI.
_clear:
    @-clear

# Generate OpenAPI specification as JSON file. The file is generated in the current directory.
[group("build")]
generate-openapi-spec: _clear _generate-openapi-spec validate-openapi-spec

_generate-openapi-spec:
    cargo run --bin geoengine-cli -- openapi > openapi.json

# Validate OpenAPI spec.
[group("lint")]
validate-openapi-spec:
    npx @openapitools/openapi-generator-cli validate -i openapi.json
    rm openapitools.json

# Run lints.
[group("lint")]
lint: _clear _lint-clippy validate-openapi-spec

_lint-clippy:
    cargo clippy --all-features --all-targets

# Run clippy for all features and all targets.
[group("lint")]
lint-clippy: _clear _lint-clippy

# Run rustfmt exactly as in CI.
[group("ci")]
ci-rustfmt:
    cargo fmt --all -- --check

# Run clippy exactly as in CI.
[group("ci")]
ci-clippy:
    cargo clippy --all-targets --locked -- -D warnings

# Run sqlfluff exactly as in CI.
[group("ci")]
ci-sqlfluff:
    pipx run sqlfluff==4.0.4 lint

# Validate OpenAPI exactly as in CI and remove generated metadata file.
[group("ci")]
ci-openapi:
    npx @openapitools/openapi-generator-cli validate -i openapi.json
    rm -f openapitools.json

# Build exactly as in CI.
[group("ci")]
ci-build mode="":
    cargo build --locked {{ mode }} --verbose

# Install llvm-cov as done in CI test action.
[group("ci")]
ci-install-llvm-cov:
    cargo install --locked cargo-llvm-cov

# Run tests and generate lcov exactly as in CI test action.
[group("ci")]
ci-test-coverage output_path="lcov.info":
    service postgresql start
    cargo llvm-cov \
    	--locked \
    	--all-features \
    	--profile ci \
    	--lcov \
    	--output-path {{ output_path }}

# Run doctests exactly as in CI test action.
[group("ci")]
ci-test-doc:
    cargo test --doc --all-features --profile ci --locked

# Run all local CI checks in the current environment.
[group("ci")]
ci-local: _clear ci-rustfmt ci-clippy ci-sqlfluff ci-openapi
    just ci-build
    just ci-install-llvm-cov
    just ci-test-coverage
    just ci-test-doc

# Run the application.
[group("run")]
run: _clear
    cargo run

# Run the tests. Optionally, a filter can be provided to run only a subset of the tests.
[group("test")]
test filter="": _clear
    cargo test -- {{ filter }} --nocapture

# Run the tests for the geoengine-services package. Optionally, a filter can be provided to run only a subset of the tests. Example: just test-services workflows::workflow::tests
[group("test")]
test-services filter="": _clear
    cargo test --package geoengine-services -- {{ filter }} --nocapture

# Run the tests for the geoengine-macros package. Optionally, a filter can be provided to run only a subset of the tests. Example: just test-macros workflows::workflow::tests
[group("test")]
test-macros filter="": _clear
    cargo test --package geoengine-macros -- {{ filter }} --nocapture
