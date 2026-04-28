# This justfile contains repository-wide tasks.

mod api-clients
mod backend 'geoengine'
mod common 'common.justfile'
mod python
mod ui
mod www

_default:
    @just --list

# Format all repository-wide files. This includes justfiles and README files. Format them with `--write`.
[arg("write", long="write", value="true", help="Whether to write the formatted files back to disk.")]
[group("lint")]
format write="false": (format-justfiles write) (format-md write) (format-json write)

# Check if justfiles are formatted correctly. Format them with `--write`.
[arg("write", long="write", value="true", help="Whether to write the formatted files back to disk.")]
[group("lint")]
format-justfiles write="false": common::_clear
    just --fmt {{ if write != "true" { "--check" } else { "" } }} --unstable --justfile justfile
    just --fmt {{ if write != "true" { "--check" } else { "" } }} --unstable --justfile common.justfile
    just --fmt {{ if write != "true" { "--check" } else { "" } }} --unstable --justfile ci.justfile
    just --fmt {{ if write != "true" { "--check" } else { "" } }} --unstable --justfile repository.justfile
    just --fmt {{ if write != "true" { "--check" } else { "" } }} --unstable --justfile geoengine/justfile
    just --fmt {{ if write != "true" { "--check" } else { "" } }} --unstable --justfile python/justfile
    just --fmt {{ if write != "true" { "--check" } else { "" } }} --unstable --justfile ui/justfile
    just --fmt {{ if write != "true" { "--check" } else { "" } }} --unstable --justfile www/justfile

FORMAT_FIND_EXCLUDES := '\
    -not -path "./.git/*" \
    -not -path "./.mypy_cache/*" \
    -not -path "./api-clients/python/*" \
    -not -path "./api-clients/rust/*" \
    -not -path "./api-clients/typescript/*" \
    -not -path "./geoengine/target/*" \
    -not -path "./geoengine/test_data/*" \
    -not -path "./geoengine/upload/*" \
    -not -path "./node_modules/*" \
    -not -path "./python/.pytest_cache/*" \
    -not -path "./python/.venv/*" \
    -not -path "./target/*" \
    -not -path "./ui/.angular/*" \
    -not -path "./openapi.json" `# generated file` \
    -not -path "./www/*" `# covered by www itself` \
    '

# Check if markdown files are formatted correctly. Format them with `--write`.
[arg("write", long="write", value="true", help="Whether to write the formatted files back to disk.")]
[group("lint")]
format-md write="false": common::_clear
    find . -type f -name "*.md" {{ FORMAT_FIND_EXCLUDES }} \
    | xargs npx prettier {{ if write != "true" { "--check" } else { "--write" } }}

# Check if JSON files are formatted correctly. Format them with `--write`.
[arg("write", long="write", value="true", help="Whether to write the formatted files back to disk.")]
[group("lint")]
format-json write="false": common::_clear
    find . -type f -name "*.json" {{ FORMAT_FIND_EXCLUDES }} \
    | xargs npx prettier {{ if write != "true" { "--check" } else { "--write" } }}

VERSION_CMD := "cd geoengine && cargo metadata --manifest-path Cargo.toml --format-version=1 --no-deps | jq -r '.packages[0].version'"
PYTHON_VERSION_CMD := 'pipx run toml-cli get --toml-path "python/pyproject.toml" "project.version"'
PYTHON_DEP_VERSION_CMD := 'pipx run toml-cli get --toml-path "python/pyproject.toml" "project.dependencies[0]" | sed -E "s/.*==\s*//"'
API_CLIENT_VERSION_CMD := "python3 -c \"import configparser;cp=configparser.ConfigParser();cp.read('api-clients/.generation/config.ini');print(cp['general']['version'])\""

# Increases the version number for Geo Engine and then updates api-clients, Python, UI, etc. accordingly.
[arg("major", long="major", value="false", help="Whether to increase the minor version instead of the patch version.")]
increase-version-number major="false": common::_clear
    cargo set-version \
        --bump {{ if major == "true" { "minor" } else { "patch" } }} \
        --exclude geoengine-expression-deps \
        --manifest-path geoengine/Cargo.toml

    @echo "New version: `{{ VERSION_CMD }}`"

    just repo _increase-version-number `{{ VERSION_CMD }}`

    git cliff --tag `{{ VERSION_CMD }}` --output CHANGELOG.md

_increase-version-number version: backend::generate-openapi-spec (api-clients::update-config version)
    pipx run toml-cli set --toml-path "python/pyproject.toml" "project.version" "{{ version }}"
    pipx run toml-cli set --toml-path "python/pyproject.toml" "project.dependencies[0]" "geoengine-api-client == {{ version }}"
    cd ui \
    && jq '.version = "{{ version }}"' package.json > package.json.tmp \
    && mv package.json.tmp package.json \
    && npm run prettier -- --write package.json

# Print the current version number of Geo Engine. Usage: `just repo print-version-number`.
print-version-number:
    @echo `{{ VERSION_CMD }}`

print-version-tag:
    @echo v`{{ VERSION_CMD }}`

# Print the current version number of the API client. Usage: `just repo print-api-client-version`.
print-published-api-client-rust-version:
    @cargo info geoengine-api-client | sed -n 's/^version: //p'

# Print the current version number of the API client. Usage: `just repo print-api-client-version`.
print-published-api-client-typescript-version:
    @npm view @geoengine/api-client version

# Print the current version number of the API client. Usage: `just repo print-api-client-version`.
print-published-api-client-python-version:
    @curl -s "https://pypi.org/pypi/geoengine-api-client/json" | jq -r '.info.version'

# Print the current version number of the Python library. Usage: `just repo print-python-library-version`.
print-published-python-version:
    @curl -s "https://pypi.org/pypi/geoengine/json" | jq -r '.info.version'

# Check if the current version of Geo Engine API Client is already published on crates.io. Usage: `just repo is-api-client-rust-already-published`.
is-api-client-rust-already-published: (_is-already-published "print-published-api-client-rust-version")

# Check if the current version of Geo Engine API Client is already published on npm. Usage: `just repo is-api-client-typescript-already-published`.
is-api-client-typescript-already-published: (_is-already-published "print-published-api-client-typescript-version")

# Check if the current version of Geo Engine API Client is already published on PyPI. Usage: `just repo is-api-client-python-already-published`.
is-api-client-python-already-published: (_is-already-published "print-published-api-client-python-version")

# Check if the current version of the Python library is already published on PyPI. Usage: `just repo is-python-library-already-published`.
is-python-library-already-published: (_is-already-published "print-published-python-version")

_is-already-published other-version-cmd:
    @{{ if shell("just repo print-version-number") == shell("just repo $1", other-version-cmd) { 'echo "true"' } else { 'echo "false"' } }}

# Check if the current version of the Geo Engine container is already published on quay.io. Usage: `just repo is-container-already-published geoengine v0.9.1`.
is-container-already-published repository tag organization="geoengine":
    @podman run docker://quay.io/skopeo/stable:latest inspect docker://quay.io/{{ organization }}/{{ repository }}:{{ tag }} > /dev/null 2>&1 && echo "true" || echo "false"

# Check if the current version of Geo Engine is already published as git tag. Usage: `just repo is-tag-already-published`.
is-tag-already-published:
    git rev-parse "{{ `just repo print-version-tag` }}" > /dev/null 2>&1 && echo "true" || echo "false"

# Check repository-wide lints. Usage: `just repo lint`.
lint: format lint-version-numbers lint-generated-code

# Check that version numbers are consistent across the project. Usage: `just repo lint-version-numbers`.
[group("lint")]
lint-version-numbers: common::_clear
    @echo "Checking if version numbers are consistent across the project ({{ shell(VERSION_CMD) }})…"
    @{{ if shell(VERSION_CMD) == shell(PYTHON_VERSION_CMD) { 'echo "Python library version is consistent."' } else { error("Python library has wrong version (" + shell(PYTHON_VERSION_CMD) + ").") } }}
    @{{ if shell(VERSION_CMD) == shell(PYTHON_DEP_VERSION_CMD) { 'echo "Python library dependency is consistent."' } else { error("Python library dependency has wrong version (" + shell(PYTHON_DEP_VERSION_CMD) + ").") } }}
    @{{ if shell(VERSION_CMD) == shell(API_CLIENT_VERSION_CMD) { 'echo "API client version is consistent."' } else { error("API client has wrong version (" + shell(API_CLIENT_VERSION_CMD) + ").") } }}

# Check that generated code is up to date and that there are no uncommitted changes in the git repository. Usage: `just repo lint-generated-code`.
[group('lint')]
lint-generated-code: common::_clear backend::generate-openapi-spec api-clients::build www::build && common::check-no-changes-in-git-repo
