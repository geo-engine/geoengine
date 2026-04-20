mod api-clients
mod backend 'geoengine'
mod ci 'ci.justfile'
mod common 'common.justfile'
mod python
mod repo 'repository.justfile'
mod ui
mod www

_default:
    @just --list --list-submodules

# Install dependencies for all submodules.
[group("install")]
install: api-clients::install backend::install python::install ui::install www::install

# Call lint for all submodules.
[group("lint")]
lint: repo::lint api-clients::lint backend::lint python::lint ui::lint www::lint

# Check the format for all submodules. Format them with `--write`.
[arg("write", long="write", value="true", help="Whether to write the formatted files back to disk.")]
[group("lint")]
format write="false": (repo::format write) (www::format write) (ui::format write)

# Build all submodules.
[group("build")]
build: backend::build backend::generate-openapi-spec api-clients::build python::build ui::build www::build

# Test all submodules.
[group("test")]
test: api-clients::test backend::test python::test ui::test www::test

# Run all applications at the same time. Usage: `just run`.
[group('run')]
[parallel]
run: backend::run ui::run www::run
