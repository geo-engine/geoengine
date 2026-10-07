# AGENTS.md: API clients

API-client-specific guidance.
Repo-wide context (generated-code chain, PR rules) is in the root `AGENTS.md`.
Inside this directory, drop the module prefix from the `just` recipes below (`just build` instead of `just api-clients build`).

`python/`, `rust/` and `typescript/` are **generated** by openapi-generator from `../openapi.json`.
Never edit them by hand: the next build overwrites them, and CI fails if the committed code differs from a fresh build.

- `just api-clients build` regenerates all three.
  Single language: `build-python`, `build-rust`, `build-typescript`.
  Needs a Java runtime and Python.
- `just api-clients lint` validates `../openapi.json`.
  `just api-clients test` builds and tests each client.
- Fix generator output in the post-processing scripts, `.generation/post-process/{python,rust,typescript}.py`.
  They patch specific generated files after each build (`file_modifications()` lists them).
- Generator settings are in `openapitools.json`.
  It is derived from `.generation/config.ini` (version, git repo) by `just api-clients update-generator-configs`, so change `config.ini` rather than the JSON.
  The version is bumped repo-wide by `just repo increase-version-number`.
- The clients are published as `geoengine-api-client` (PyPI, crates.io) and `@geoengine/api-client` (npm).
