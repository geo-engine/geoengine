---
applyTo: "backend/**/*.rs"
---

# Backend Developer Instructions

These instructions are for contributors working on the Rust backend code under the `geoengine/` directory.
They cover common developer workflows (build, run, test), code style, CI expectations, and links to relevant resources.

## Quick commands

- **Build:** `just backend build` (or `cargo build --workspace --all-targets`)
- **Run (dev):** `just backend run` (starts the backend server with development config)
- **Run tests:** `just backend test` (runs unit and integration tests)
- **Run fmt:** `just backend fmt` (formats code with `rustfmt`)
- **Lint / Clippy:** `just backend lint` (runs `cargo clippy` and project lint checks)

These `just` targets are defined in the repository `justfile` at the project root; use them where available to ensure consistent environment and flags.

## Development workflow

- Run `just backend ci` before committing.
- Write or update unit and integration tests for new functionality.

## Environment & Configuration

- Configuration is loaded from environment variables and config files read by the service. For local development, see `backend/README.md` for example `.env` values and service dependencies.
- If the backend depends on external services (databases, caches, etc.), prefer using the repository's `podman` containers, e.g., PostGIS (when available), or local test fixtures.

## Database & migrations

- If the backend uses persistent storage or migrations, update migration files and include migration commands in the PR description.
- Test migrations on a local test database before pushing.

## Testing

- Unit tests: run `just backend test -- unit` (or the equivalent `cargo test --lib`)
- Use fixtures under `test-data/` when applicable for reproducible test inputs.
- Start test function names with `it_` and use descriptive names that indicate what the test is verifying.
- Use `unwrap()` only in test code.

## Error handling & logging

- Always return `Result<T, E>` if the function can fail and propagate errors with `?`. Do not panic.
- Never use `unwrap()` or `expect()` in production code. Instead, propagate errors using the `?` operator or handle them gracefully.
- Use structured logging where appropriate and include contextual fields for easier debugging.

## Formatting & style

- Use `rustfmt` (run via `just backend fmt`) to format code.
- Run `cargo clippy --all-targets -- -D warnings` during CI to ensure lints are addressed locally first.
- Use `camelCase` for JSON field names when serializing/deserializing with serde, and use `#[serde(rename_all = "camelCase")]` on structs to enforce this convention.

## Documentation

- Public APIs must have `///` doc comments.
- Update `backend/README.md` when adding new developer-facing behavior or start-up steps.

## Debugging

- Use `dbg!` macros for logging during development, and remove or replace with structured logging before merging.

## Useful links

- Project README: [README.md](../../README.md)
- Backend README: [geoengine/README.md](../../geoengine/README.md)
- Justfile (tasks): [justfile](../../justfile)
- OpenAPI spec: [openapi.json](../../openapi.json)

## Notes for Copilot / Assistant

- Use this document as the primary source when answering developer questions about backend workflows and conventions.
