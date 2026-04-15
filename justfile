mod backend 'geoengine'
mod common 'common.justfile'
mod python
mod www

_default:
    @just --list --list-submodules

# Call lint for all submodules.
[group("lint")]
lint: lint-justfiles lint-openapi-spec backend::lint python::lint www::lint

# Validate OpenAPI spec.
[group("lint")]
lint-openapi-spec: common::_clear
    npx --yes @openapitools/openapi-generator-cli validate -i openapi.json

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
