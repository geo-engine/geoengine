# Geo Engine

[![CI](https://github.com/geo-engine/geoengine/actions/workflows/ci.yml/badge.svg)](https://github.com/geo-engine/geoengine/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/geo-engine/geoengine/badge.svg?branch=master)](https://coveralls.io/github/geo-engine/geoengine?branch=master)
[![Documentation](https://img.shields.io/badge/documentation-docs.geoengine.io-blue)](https://docs.geoengine.io/)

This workspace contains the Geo Engine crates.

## Documentation

- [Geo Engine Docs](https://docs.geoengine.io/)

## Development

- While Geo Engine should build on Linux and Windows environments, we currently only support Ubuntu Linux 22.04 LTS.
- You need a recent Rust environment with a Rust nightly compiler. We recommend rustup to manage Rust `https://rustup.rs/`.

### Dependencies

```
# Build essentials
apt install build-essential
# lld linker
apt install clang lld
# GDAL (>= 3.2.1), Ubuntu 22.04 ships GDAL 3.4.1
apt install libgdal-dev gdal-bin
# Proj build dependencies (if libproj >= 7.2 not installed)
apt install cmake sqlite3
```

### Lints

Please run Clippy with
`cargo clippy --all-targets --all-features`
before creating a pull request.

### Testing

Please provide tests with all new features and run
`cargo test`
before creating a pull request.

Edit `Settings-test.toml` for environment specific test parameters.

#### PostgreSQL

For running the PostgreSQL tests, you need to have it installed.
Furthermore, you need to create a default user `geoengine` with password `geoengine`.

```
sudo -u postgres psql << EOF
\set AUTOCOMMIT on
CREATE USER geoengine WITH PASSWORD 'geoengine' CREATEDB;
CREATE DATABASE geoengine OWNER geoengine;
EOF
```

### Benchmarks

For performance-critical features, we aim to provide benchmarks in the `benches` directory.
If you plan on optimizing a feature of Geo Engine, please confirm it this way.

## Deployment

Deploy an instance using `cargo run --package geoengine-services --bin main --release`.

### Features

The PostgreSQL storage backend can optionally be enabled using `--features postgres` in the `cargo` command.

### Configuration

Copy `Settings-default.toml` to `Settings.toml` and edit per your requirements.

## Troubleshooting

### Running out of memory map areas

Problem: When running Geo Engine or its tests you encounter one of the following (or similar) errors:

- `Cannot allocate Memory (OS Error 12)`
- `Os { code: 11, kind: WouldBlock, message: "Resource temporarily unavailable" }`

Solution: Adjust the system's `max_map_count` parameter:

- Temporary configuration:
  - `sudo sysctl -w vm.max_map_count=262144`
- Permanent configuration:
  - Add the following line to /etc/sysctl.d/local.conf
    - `vm.max_map_count=262144`
  - Reload the system config:
    - `sudo sysctl --system`

## Contributing

We are grateful for any contributions.
Please make sure to read our [contributing guide](CONTRIBUTING.md).
