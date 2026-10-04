# AGENTS.md

This file provides guidance to AI coding agents (Claude Code, Codex, OpenCode & GitHub Copilot) when working with code in this repository.

## Repository layout

A monorepo with five projects. Each has its own `justfile`, which the root `justfile` mounts as a just module:

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

## PRs and commits

Branch as `feat/<desc>` or `fix/<desc>`. PR titles follow conventional commits, `type(scope): description`, with scopes `backend`, `ui`, `python`, `api-client` or `www`; this is enforced by CI. Changelog is generated with git-cliff. Keep unrelated changes in separate PRs. The PR description says how to test the change, the expected behavior, and any configuration changes.
