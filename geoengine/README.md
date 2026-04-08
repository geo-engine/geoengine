# Geo Engine — Backend

This directory contains the Geo Engine backend service: a Rust-based API server
that implements the project's geospatial data APIs and core processing logic.

## Quick Overview

- Language: Rust
- Build tool: `cargo` (part of the Rust toolchain)
- Purpose: serve geospatial APIs, data processing, and storage integration

## Requirements

- Install Rust (stable toolchain) and `cargo` — see <https://rustup.rs/>
- GDAL (>= 3.8.4) and its development headers (`libgdal-dev` on Debian/Ubuntu)
- Proj (>= 9.4) and its development headers (`libproj-dev` on Debian/Ubuntu)
- A PostgreSQL database (if using the PostGIS extension for geospatial storage)

## Build & Run (local)

`just backend run` — builds and runs the backend server with development configuration.
See `just backend build` for just building without running.

Adjust `Settings.toml` or environment variables (database connection, bind address, API keys)
as required by your environment.

## Tests & Formatting

- Format code: `just backend fmt`
- Run Clippy lints: `just backend lint`
- Run unit and integration tests: `just backend test`

## API Specification

The OpenAPI specification for the public API is available at the repository
root: [openapi.json](../openapi.json).

## Development Notes

- Use `RUST_LOG` to control runtime logging verbosity (e.g. `RUST_LOG=debug`).
- See [backend/CODESTYLE.md](backend/CODESTYLE.md) for coding guidelines.
- Many common development tasks are exposed via the top-level `justfile`.
