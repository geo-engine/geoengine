# AGENTS.md

This file provides guidance to AI coding agents (Claude Code, Codex, OpenCode & GitHub Copilot) when working with code in this repository.

## What Geo Engine is

A server for processing and visualizing geospatial data, with native time-series support and stream processing for large datasets.

- **Users and interfaces**: data scientists use the `geoengine` Python library (often in Jupyter). Domain users use the UI apps: `gis` for exploring and analysing data, `eodyssey`, and the domain-specific `dashboards/*`. Admins use `manager`. External GIS clients use OGC services (OGC API Tiles, WMS, WFS, WCS). All of these go through the REST API.
- **Data**: raster and vector data, read through GDAL/OGR. It comes from internal datasets, user uploads and external data providers (STAC, GBIF, Pangaea, NetCDF-CF/EBV, …). Layers and layer collections organize it for browsing.
- **Processing model**: a processing graph is a JSON tree of operators with sources as its leaves. It is evaluated lazily. Nothing is computed until a query arrives with a query rectangle (spatial bounds, time interval, bands or columns). Results are streamed: rasters as tiles (512×512 pixels by default), vectors as Arrow feature collections in chunks.
- **Assumptions and constraints**:
  - Every raster tile and feature has a time interval, stored in milliseconds and half-open `[start, end)`. Time is never optional.
  - Data must never be loaded into memory as a whole. Operators work on streams.
  - Operators with several inputs require the same spatial reference. Reprojection is an explicit operator, not an implicit conversion.
  - The processing graph id is a UUID v5 hash of the processing graph, so registering the same processing graph twice yields the same id. A workflow is never changed in place.
  - Access is controlled by `Read`/`Owner` permissions on layers, collections, projects, datasets, ML models and providers. Besides registered and OIDC users there are anonymous users (`POST /anonymous`, enabled by default via `[session] anonymous_access`), so features must not assume a user with an email or password. Quotas are optional (`[quota]` in `geoengine/Settings-default.toml`).
  - Types are persisted in PostgreSQL (processing graphs, provider and dataset definitions, symbologies). Changing them requires a migration (see the `new-migration` skill).

## Repository layout

A monorepo with five projects. Each has its own `justfile`, which the root `justfile` mounts as a `just` module:

| Directory      | just module   | What it is                                                                                     |
| -------------- | ------------- | ---------------------------------------------------------------------------------------------- |
| `geoengine/`   | `backend`     | Rust Cargo workspace: the Geo Engine server, CLI and core libraries                            |
| `api-clients/` | `api-clients` | Python, Rust and TypeScript clients **generated** from `openapi.json`                          |
| `python/`      | `python`      | `geoengine` Python library, built on the generated Python API client                           |
| `ui/`          | `ui`          | Angular workspace: the `common` and `core` libraries plus apps (gis, edv, dashboards, manager) |
| `www/`         | `www`         | Astro website and docs, including operator/plot docs (`www/src/content/docs/docs/`)            |

Each project directory has its own `AGENTS.md` with that project's commands, architecture and conventions. Read it before working in that project.

Run recipes from the repo root as `just <module> <recipe>`, e.g. `just backend test`, or `just backend::test`. `just` lists everything. Top-level `just install|build|lint|test|run` fan out to all modules. `just ci <module>` reproduces a project's CI pipeline (install → lint → build → test).

## Generated-code chain (important)

The Rust server is the single source of truth for the API:

1. Rust handlers/types annotated with `utoipa` → `just backend generate-openapi-spec` writes `/openapi.json` (via `geoengine-cli openapi`).
2. `just api-clients build` regenerates `api-clients/{python,rust,typescript}` from it with openapi-generator, plus post-processing scripts in `api-clients/.generation/`.
3. `python/` installs `api-clients/python` in editable mode. `ui/` npm-links `api-clients/typescript` as `@geoengine/api-client` (`just ui install` sets this up).

CI (`just repo lint-generated-code`) regenerates the spec, clients and www, then fails on any git diff. **After any API-visible backend change, regenerate `openapi.json` and the API clients and commit them.** Never hand-edit `openapi.json` or the files in `api-clients/*/`.

Versions are kept consistent across Cargo, `python/pyproject.toml`, `api-clients/.generation/config.ini` and `ui/package.json`. Bump them all with `just repo increase-version-number`.

## Repo-wide lint

`just repo lint` checks prettier formatting of Markdown/JSON and justfiles, version consistency, and generated-code freshness. Fix formatting with `just format --write`.

## Agent skills

Reusable step-by-step procedures live in `.agents/skills/<name>/SKILL.md` (Agent Skills format: `name` and `description` frontmatter). Codex, OpenCode and GitHub Copilot read them from there. Claude Code only reads `.claude/skills/`, which is a symlink to `.agents/skills/`, so a new skill only needs to be added to `.agents/skills/`.

- `new-migration`: add a database migration to the backend.

## Testing new features

Besides automated tests, try new features against a running instance where that makes sense:

- Backend: start a clean instance with `just backend run` and restart it before each test run. How to give it its own, freshly cleared database schema is described in `geoengine/AGENTS.md`.
- UI: where it makes sense, test UI changes in the browser against a live backend, not only with unit tests. Use `just ui run <project>` with a local `just backend run`, or `--type=nightly` if the change needs no backend changes (see `ui/AGENTS.md`).

## PRs and commits

Branch as `feat/<desc>` or `fix/<desc>`. PR titles follow conventional commits, `type(scope): description`, with scopes `backend`, `ui`, `python`, `api-client` or `www`; this is enforced by CI. Changelog is generated with git-cliff. Keep unrelated changes in separate PRs. The PR description says how to test the change, the expected behavior, and any configuration changes.
