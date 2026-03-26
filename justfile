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
    npx --yes @openapitools/openapi-generator-cli validate -i openapi.json
    rm openapitools.json

# Run lints.
[group("lint")]
lint: _clear lint-fmt lint-clippy lint-sql validate-openapi-spec

_lint-clippy:
    cargo clippy --all-features --all-targets

# Run clippy for all features and all targets.
[group("lint")]
lint-clippy: _clear _lint-clippy

# Run rustfmt
[group("lint")]
lint-fmt: _clear
    cargo fmt --all -- --check

# Run sqlfluff.
[group("lint")]
lint-sql: _clear
    pipx run sqlfluff==4.0.4 lint

# Build in dev or release mode.
[arg('mode', pattern='|release')]
[group("build")]
build mode="":
    cargo build --locked {{ mode }} --verbose

# Install dependencies such as llvm-cov.
[group("install")]
install:
    cargo install --locked cargo-llvm-cov

# Run tests and generate lcov exactly as in CI test action.
[group("ci")]
test-coverage output_path="lcov.info":
    service postgresql start
    cargo llvm-cov \
    	--locked \
    	--all-features \
    	--profile ci \
    	--lcov \
    	--output-path {{ output_path }}

# Run all local CI checks in the current environment.
[group("ci")]
ci: _clear install lint test build

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

# Run doctests
[group("test")]
test-doc:
    cargo test --doc --all-features --profile ci --locked
