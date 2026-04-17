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

VERSION_CMD := "cargo metadata --manifest-path geoengine/Cargo.toml --format-version=1 --no-deps | jq -r '.packages[0].version'"
PYTHON_VERSION_CMD := 'pipx run toml-cli get --toml-path "python/pyproject.toml" "project.version"'
PYTHON_DEP_VERSION_CMD := 'pipx run toml-cli get --toml-path "python/pyproject.toml" "project.dependencies[0]" | sed -E "s/.*==\s*//"'
API_CLIENT_VERSION_CMD := "python -c \"import configparser;cp=configparser.ConfigParser();cp.read('api-clients/.generation/config.ini');print(cp['general']['version'])\""

# Increases the version number for Geo Engine and then updates api-clients, Python, UI, etc. accordingly.
increase-version-number: common::_clear
    cargo set-version \
        --bump patch \
        --exclude geoengine-expression-deps \
        --manifest-path geoengine/Cargo.toml

    @echo "New version: `{{ VERSION_CMD }}`"

    just _increase-version-number `{{ VERSION_CMD }}`

_increase-version-number version: (api-clients::update-config version)
    pipx run toml-cli set --toml-path "python/pyproject.toml" "project.version" "{{ version }}"
    pipx run toml-cli set --toml-path "python/pyproject.toml" "project.dependencies[0]" "geoengine-api-client == {{ version }}"
    cd ui \
    && jq '.version = "{{ version }}"' package.json > package.json.tmp \
    && mv package.json.tmp package.json \
    && npm run prettier -- --write package.json

# Check repository-wide lints.
lint: format lint-version-numbers lint-generated-code

[group("lint")]
lint-version-numbers: common::_clear
    @echo "Checking if version numbers are consistent across the project ({{ shell(VERSION_CMD) }})…"
    @{{ if shell(VERSION_CMD) == shell(PYTHON_VERSION_CMD) { 'echo "Python library version is consistent."' } else { error("Python library has wrong version (" + shell(PYTHON_VERSION_CMD) + ").") } }}
    @{{ if shell(VERSION_CMD) == shell(PYTHON_DEP_VERSION_CMD) { 'echo "Python library dependency is consistent."' } else { error("Python library dependency has wrong version (" + shell(PYTHON_DEP_VERSION_CMD) + ").") } }}
    @{{ if shell(VERSION_CMD) == shell(API_CLIENT_VERSION_CMD) { 'echo "API client version is consistent."' } else { error("API client has wrong version (" + shell(API_CLIENT_VERSION_CMD) + ").") } }}

# Check that generated code is up to date and that there are no uncommitted changes in the git repository. This ensures that the generated code is up to date and that there are no uncommitted changes in the git repository. This is important for CI because it ensures that the generated code is up to date and that there are no uncommitted changes in the git repository.
[group('lint')]
lint-generated-code: common::_clear backend::generate-openapi-spec api-clients::build www::build && common::check-no-changes-in-git-repo
