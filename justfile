mod backend 'geoengine'
mod common 'common.justfile'
mod python

_default:
    @just --list

# Validate OpenAPI spec.
[group("lint")]
validate-openapi-spec: common::_clear
    npx --yes @openapitools/openapi-generator-cli validate -i openapi.json

[group("lint")]
lint: validate-openapi-spec backend::lint python::lint
