# Geo Engine

[![CI](https://github.com/geo-engine/geoengine/actions/workflows/ci.yml/badge.svg?event=merge_group)](https://github.com/geo-engine/geoengine/actions/workflows/ci.yml?query=event%3Amerge_group)
[![Coverage Status](https://coveralls.io/repos/github/geo-engine/geoengine/badge.svg?branch=main)](https://coveralls.io/github/geo-engine/geoengine?branch=main)
[![Documentation](https://img.shields.io/badge/documentation-docs.geoengine.io-blue)](https://docs.geoengine.io/)

Geo Engine is a geospatial data processing engine that allows you to perform spatial analyses and visualizations.
Its query engine has native time support and can handle large datasets through stream processing.
It supports various geospatial data formats and provides a robust API for integrating with other applications, e.g., by providing OGC APIs.

You can find more documentation at [docs.geoengine.io](https://docs.geoengine.io/).

This workspace contains the Geo Engine server.

## Build

We currently only support Ubuntu Linux 24.04 LTS.

You need to have a Rust compiler and the package manager Cargo installed.
We recommend [rustup.rs](https://rustup.rs) to manage both.

You can run the server using `cargo run --release`.

### Dependencies

There are some dependencies that need to be installed on the system:

```
# Build essentials
apt install build-essential
# lld linker
apt install clang lld
# GDAL (>=  3.8.4)
apt install libgdal-dev gdal-bin
# Proj build dependencies (if libproj >= 9.4 not installed)
apt install cmake sqlite3
# Protocl Buffers
apt install protobuf-compiler
```

### Configuration

You can override the values in `Settings-default.toml` by creating a `Settings.toml` and modifying the parameters to suit your requirements.

## Development

### Lints

### Rust Code

Please run Clippy with
`cargo clippy --all-targets --all-features`
before creating a pull request.

#### SQL Files

We use [SQLFluff](https://sqlfluff.com/) to lint our SQL files.

You can install it with `pip install sqlfluff`.

To lint all SQL files, run `sqlfluff lint`.
This will also be checked in our CI.

There is a VSCode extension available [here](https://marketplace.visualstudio.com/items?itemName=dorzey.vscode-sqlfluff).
This helps to format the files.

### Testing

Please provide tests with all new features and run
`cargo test`
before creating a pull request.

Edit `Settings-test.toml` for environment-specific test parameters.

#### PostgreSQL

For running the PostgreSQL tests, you need to have it installed.
Furthermore, you need to create a default user `geoengine` with the password `geoengine`.

```
sudo -u postgres psql << EOF
\set AUTOCOMMIT on
CREATE USER geoengine WITH PASSWORD 'geoengine' CREATEDB;
CREATE DATABASE geoengine OWNER geoengine;
\c geoengine
CREATE EXTENSION postgis;
EOF
```

During development, you can use the following setting in your `Settings.toml` to clean the database on server startup:

```
[postgres]
clear_database_on_start = true
```

##### NFDI / GFBio

For running the NFDI/GFBio ABCD data providers (`GfbioAbcdDataProviderDefinition` and `GfbioCollectionsDataProviderDefinition`) during development, you need to integrate the test data like this:

```bash
# delete existing data
sudo -u postgres psql --dbname=geoengine -c \
  "DROP SCHEMA IF EXISTS abcd CASCADE;"

# insert data
cat test_data/gfbio/init_test_data.sql test_data/gfbio/test_data.sql | \
  sudo -u postgres psql --dbname=geoengine --single-transaction --file -
```

##### GBIF

For running the GBIF data provider (`GbifDataProviderDefinition`) during development, you need to integrate the test data like this:

```bash
# delete existing data
sudo -u postgres psql --dbname=geoengine -c \
  "DROP SCHEMA IF EXISTS gbif CASCADE;"

# insert data
cat test_data/gbif/init_test_data.sql test_data/gbif/test_data.sql | \
  sudo -u postgres psql --dbname=geoengine --single-transaction --file -
```

### Benchmarks

For performance-critical features, we aim to provide benchmarks in the `benches` directory.
If you plan on optimizing a feature of Geo Engine, please confirm it this way.

### Expression dependencies

We use the [`expression/deps-workspace`](expression/deps-workspace) crate to manage dependencies for compiling expressions at runtime.
This ensures that it is compatible with Geo Engine when linking against it.

It is important to keep the dependencies in sync with the Geo Engine dependencies.
You can verify this by running the [`check-expression-deps.rs`](.scripts/check-expression-deps.rs) script located in the [`.scripts`](.scripts) directory.

```bash
chmod +x .scripts/check-expression-deps.rs
./.scripts/check-expression-deps.rs
```

To update the expression dependencies, you can use the [`update-expression-deps.rs`](.scripts/update-expression-deps.rs) script located in the [`.scripts`](.scripts) directory.
This script helps to keep the dependencies in sync and up to date.
Run it with:

```bash
chmod +x .scripts/update-expression-deps.rs

./.scripts/update-expression-deps.rs
```

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
