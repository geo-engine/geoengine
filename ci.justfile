mod api-clients
mod backend 'geoengine'
mod common 'common.justfile'
mod python
mod ui
mod www

# [group('ci')]
# _default: api-clients-ci backend-ci python-ci ui-ci www-ci idempotency

# [group('ci')]
# api-clients-ci: api-clients::install api-clients::lint api-clients::build api-clients::test

# [group('ci')]
# backend-ci: backend::install backend::lint backend::build backend::test

# [group('ci')]
# python-ci: python::install python::lint python::build python::test

# [group('ci')]
# ui-ci: ui::install ui::lint ui::build ui::test

# [group('ci')]
# www-ci: www::install www::lint www::build www::test

# Check that generated code is up to date and that there are no uncommitted changes in the git repository. This ensures that the generated code is up to date and that there are no uncommitted changes in the git repository. This is important for CI because it ensures that the generated code is up to date and that there are no uncommitted changes in the git repository.
[group('ci')]
idempotency: common::_clear backend::generate-openapi-spec api-clients::build www::build && common::check-no-changes-in-git-repo
