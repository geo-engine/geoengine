mod backend 'geoengine'
mod common 'common.justfile'
mod www

_default:
    @just --list

# Validate OpenAPI spec.
[group("lint")]
validate-openapi-spec:
    npx --yes @openapitools/openapi-generator-cli validate -i openapi.json
