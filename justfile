_default:
    @just --list

# Generate OpenAPI specification as JSON file. The file is generated in the current directory.
[group("build")]
generate-openapi-spec:
	@-clear
	cargo run --bin geoengine-cli -- openapi > openapi.json

# Run lints.
[group("lint")]
lint:
	@-clear
	just _lint-clippy

_lint-clippy:
	cargo clippy --all-features --all-targets

# Run clippy for all features and all targets.
[group("lint")]
lint-clippy:
	@-clear
	just _lint-clippy

# Run the application.
[group("run")]
run:
	@-clear
	cargo run

# Run the tests. Optionally, a filter can be provided to run only a subset of the tests.
[group("test")]
test filter="":
    @-clear
    cargo test -- {{ filter }} --nocapture

# Run the tests for the geoengine-services package. Optionally, a filter can be provided to run only a subset of the tests. Example: just test-services workflows::workflow::tests
[group("test")]
test-services filter="":
    @-clear
    cargo test --package geoengine-services -- {{ filter }} --nocapture