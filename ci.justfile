# This justfile defines the CI tasks for the Geo Engine project.

mod mapi-clients 'api-clients'
mod mbackend 'geoengine'
mod mpython 'python'
mod mrepo 'repository.justfile'
mod mui 'ui'
mod mwww 'www'

[group('ci')]
_default: api-clients backend python ui www repo

[group('ci')]
api-clients: mapi-clients::install mapi-clients::lint mapi-clients::build mapi-clients::test

[group('ci')]
backend: mbackend::install mbackend::lint mbackend::build mbackend::test

[group('ci')]
python: mpython::install mpython::lint mpython::build mpython::test

[group('ci')]
ui: mui::install mui::lint mui::build mui::test

[group('ci')]
www: mwww::install mwww::lint mwww::build mwww::test

# Check that generated code is up to date and that there are no uncommitted changes in the git repository. This ensures that the generated code is up to date and that there are no uncommitted changes in the git repository. This is important for CI because it ensures that the generated code is up to date and that there are no uncommitted changes in the git repository.
[group('ci')]
repo: mbackend::install mrepo::lint
