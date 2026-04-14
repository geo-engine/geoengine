mod backend 'geoengine'
mod common 'common.justfile'
mod python

_default:
    @just --list --list-submodules

# Validate OpenAPI spec.
[group("lint")]
validate-openapi-spec: common::_clear
    npx --yes @openapitools/openapi-generator-cli validate -i openapi.json

# Call lint for all submodules.
[group("lint")]
lint: lint-justfiles validate-openapi-spec backend::lint python::lint

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
