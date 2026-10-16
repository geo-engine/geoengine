# AGENTS.md: Python library

Python-specific guidance.
Repo-wide context (generated-code chain, PR rules) is in the root `AGENTS.md`.
Inside this directory, drop the module prefix from the `just` recipes below (`just test` instead of `just python test`).

## Commands

- Install: `just python install` creates `.venv` and installs the local API client (`../api-clients/python`, editable) plus the `dev,test,examples` extras.
  Set `USE_UV=true` to use uv.
- Lint: `just python lint` (ruff format check, ruff check, mypy on `geoengine` and `tests`).
- Format: `just python format-code`.
- Test: `just python test [filter]`, where filter is passed to `pytest -k`.
  Tests in `tests/ge_test.py` **build the Rust server with cargo and launch a real Geo Engine instance**, so they need the backend toolchain and Postgres.
- Build: `just python build`

## Conventions

- Supported Python versions follow `requires-python` in `pyproject.toml` (currently ≥ 3.11).
  Dependencies are declared there too.
- Type every function's parameters and return value; mypy checks the package and the tests.
  Prefer built-in generics (`list[str]`) and `collections.abc` over `typing` aliases.
- Public API gets NumPy-style docstrings (`Parameters`, `Returns`).
  They become the Python API docs on the website (see `www/AGENTS.md`).
- Raise the exception types from `geoengine/error.py` rather than bare `Exception`.
  Library code shouldn't print to stdout.
- The library wraps the generated `geoengine_api_client`.
  When the API changes, regenerate the client first (root `AGENTS.md`), then adapt the wrapper.

## Example notebooks

Jupyter notebooks directly in `examples/` are documentation and are executed in CI; subdirectories are skipped.

- Run all: `just python run-examples`.
  Run one: `just python run-example <file>.ipynb`.
- Each run starts a fresh Geo Engine test instance on `localhost:3030`, so notebooks connect with `ge.initialize("http://localhost:3030/api")` and may only use data that instance provides.
  Don't hardcode other servers, local paths or secrets.
- Use the `geoengine` library, not raw HTTP calls.
  Keep each notebook to one topic, and explain each step, its data sources and assumptions in markdown cells.
