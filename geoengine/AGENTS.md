# AGENTS.md: Backend (Rust)

Backend-specific guidance. Repo-wide context (generated-code chain, PR rules) is in the root `AGENTS.md`.

Requires GDAL ≥ 3.8.4, PROJ ≥ 9.4 (dev headers), and PostgreSQL with PostGIS. Run cargo commands from this directory.
The `just` recipes below are written for the repo root.
Inside this directory, drop the module prefix (`just test` instead of `just backend test`).

## Commands

- Build: `just backend build`. Run the server: `just backend run`. This also builds the `gdalsource-process` binary the server needs.
- Lint: `just backend lint` (rustfmt check + clippy + sqlfluff). CI-strict clippy: `just backend lint-clippy --deny-warnings`.
  - Format: `just backend lint-rustfmt --write`.
  - SQL only: `just backend lint-sql`.
- Before committing: `just ci backend` runs the same install → lint → build → test sequence as CI.
- Tests: `just backend test [filter]`. Per crate: `just backend test-services <filter>`, `test-operators`, `test-datatypes`, `test-macros` (lib tests only; `test-services <filter> --all` includes integration tests).
  - Doctests: `just backend test-doc`.
  - Single test, e.g.: `cargo test -p geoengine-operators --lib -- processing::expression::tests::it_works --nocapture`
- Expression deps sync check: `cargo test --package geoengine-expression --test check-expression-deps`
- After API-visible changes: `just backend generate-openapi-spec`, then `just api-clients build` (see root `AGENTS.md`).

Tests that touch the database need a local Postgres user/db `geoengine`/`geoengine` with PostGIS (see `README.md`).
Test config lives in `Settings-test.toml`, and each test gets its own temporary schema.
Test fixtures (rasters, vectors, provider and layer definitions, mocked HTTP responses) are in `test_data/` in this directory, not the repo-root `test-data/`.
Runtime config is `Settings-default.toml`, overridden by `Settings.toml` and `GEOENGINE__SECTION__KEY` env vars.
If you hit `OS Error 12` / `WouldBlock`, propose to the user to increase `vm.max_map_count`.

## Running a clean instance for manual testing

`just backend run` starts a server on `localhost:3030` (API under `/api`, initial user `admin@localhost`/`adminadmin`).
The server always reads `Settings-default.toml` and `Settings.toml` from this directory; there is no option to pick another file.
`Settings-test.toml` is only used by `cargo test`.
To get a fresh instance in its own schema that does not touch the user's data, override the settings with env vars:

```bash
GEOENGINE__POSTGRES__SCHEMA=agent GEOENGINE__POSTGRES__CLEAR_DATABASE_ON_START=true just backend run
```

Every start drops and recreates the `agent` schema, so restart the server before each test run and do not rely on data from earlier runs.
The server refuses to start if `clear_database_on_start` is `true` for a schema that was created with `false`, so never point it at the user's schema.
If port 3030 is taken, also set `GEOENGINE__WEB__BIND_ADDRESS=127.0.0.1:3031` (the UI's `local` proxy expects 3030).

## Crate architecture

Core layering: `services` depends on `operators`, which depends on `datatypes`; lower crates never depend on higher ones.
Supporting crates: `expression` (used by `operators` and `services`), `macros` (used by `services`) and `crs-constants` (used by `datatypes`).

- **`datatypes`**: primitives (time intervals, bounding boxes, spatial references), feature collections (Arrow-backed), raster tiles/grids, plots.
- **`operators`**: the processing engine. `engine/` defines the operator lifecycle: serializable operator definitions (`RasterOperator`/`VectorOperator`/`PlotOperator`, combined in `TypedOperator`) are initialized against an `ExecutionContext` (which yields result descriptors) and then produce query processors that return async streams of tiles or feature chunks. Implementations live in `source/` (GDAL, OGR, CSV), `processing/`, `plot/` and `adapters/` (stream combinators). There is also `cache/` and `machine_learning/` (ONNX via `ort`). GDAL reads can run out-of-process through the `gdalsource-process` binary (`src/bin/`), managed by a process pool (`[gdal_process_pool]` settings).
- **`expression`**: user expressions are compiled to Rust **at runtime** and dynamically linked. `expression/deps-workspace` pins the dependencies for that compilation and must stay in sync with the workspace (`geo`, `geo-types`, …). Update it with `.scripts/update-expression-deps.rs`.
- **`services`**: the actix-web server.
  - `api/handlers/`: REST endpoints. `api/ogc/`: WMS/WFS/WCS and OGC API. `api/apidoc.rs`: utoipa OpenAPI registration. New endpoints and schemas must be registered there to appear in `openapi.json`.
  - `api/model/`: **API-facing types, kept separate from internal `datatypes`/`operators` types**, with `From`/`TryFrom` conversions both ways. `api/model/processing_graphs/` mirrors every operator as an API type, declared with `#[api_operator]`, and `back_conversion/` maps them back. Exposing a new operator means adding it on both sides. The doc comments on these types become the operator pages on the website (generated from `openapi.json`, see `www/AGENTS.md`).
  - `contexts/`: `PostgresContext`/session handling and `migrations/`.
  - `datasets/` (internal datasets, uploads, `external/` data providers such as STAC, GBIF, Pangaea, NetCDF-CF, Aruna, Copernicus, Wildlive), `layers/` (layer collections, provider registry), `workflows/`, `projects/`, `permissions/`, `users/` (incl. OIDC), `quota/`, `tasks/`, `machine_learning/`.
- **`macros`** (`geoengine-macros`): `#[ge_context::test]` spins up a DB-backed app/context for service tests, with options like `user = "admin"`, `test_execution = "serial"`, `tiling_spec = …`. Also `#[api_operator]` and `#[type_tag]` for OpenAPI-friendly tagged types.
- **`crs-constants`**: zero-dependency static CRS metadata (EPSG bounds), generated from PROJ's EPSG database.

## Processing internals

- **Sub-queries**: raster operators that need a different input region per output tile (reprojection, interpolation, downsampling, neighborhood aggregate) are built on `RasterSubQueryAdapter` (`operators/src/adapters/raster_subquery/`). For each output tile it issues a sub-query to the source and folds the results with a `SubQueryTileAggregator`. Reuse it rather than writing your own tile loop.
- **Cache**: when `[cache] enabled` is set, `services` wraps every initialized operator in a cache operator (`services/src/contexts/mod.rs`, `operators/src/cache/`). So the cache sits between every pair of operators in a graph, not only at the output. Results must therefore be deterministic for a given query, and operators must propagate the `cache_hint` of their input tiles and chunks.
- **Threads**: CPU-heavy work runs on the shared Rayon thread pool from `ctx.thread_pool()`, via `spawn_blocking_with_thread_pool`. Never block the async runtime, and do not create your own thread pools.
- **GDAL**: GDAL opens and reads run in separate `gdalsource-process` worker processes (see `operators` above), limited by `[gdal_process_pool]`. GDAL state such as config options does not carry over from the server process; pass it with the dataset parameters (`gdal_config_options`).

## Database migrations

Migrations are in `services/src/contexts/migrations/` as `migration_NNNN_<name>.rs` (+ `.sql`). A new migration must:

- implement `Migration` with `prev_version`/`version`,
- be registered in `all_migrations()` in `mod.rs`,
- be reflected in `current_schema.sql`, which fresh databases use directly.

Tests compare the migrated schema with the current schema. Migrations run automatically when the server starts, so there is no manual migration step.
The full step-by-step procedure, including pitfalls, is the `new-migration` skill in `.agents/skills/new-migration/SKILL.md` at the repo root.

## SQL

SQL lives in the migrations and in `test_data/` (e.g. GBIF/GFBio fixtures). It is PostgreSQL and is linted with sqlfluff (`.sqlfluff`, jinja templater):

- Keywords in uppercase (`SELECT`, `FROM`, `WHERE`); table and column names in lowercase.
- A few non-reserved PostgreSQL keywords (`name`, `data`, `location`, …) are allowed as identifiers; the list is in `.sqlfluff`.

## Conventions

- Workspace clippy is `pedantic` plus `unwrap_used`, `print_stdout`/`print_stderr` and `dbg_macro` warnings. No `unwrap()`/`expect()` in production code: functions that can fail return `Result` and propagate with `?`. Errors use `snafu`.
- `expect` is for tests, or where an error is truly impossible. Its message says why the value should be Ok, e.g. `.expect("env variable IMPORTANT_PATH should be set by wrapper_script.sh")`. Test function names start with `it_`.
- Log with `tracing` macros (`tracing::debug!`, …) and add context as structured fields. `dbg!` is for local debugging only.
- Serde JSON uses `#[serde(rename_all = "camelCase")]`.
- Public items get `///` docs. Update `README.md` when adding developer-facing setup steps.
