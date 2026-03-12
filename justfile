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