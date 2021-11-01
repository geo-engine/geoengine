# Geo Engine

[![CI](https://github.com/geo-engine/geoengine/actions/workflows/ci.yml/badge.svg)](https://github.com/geo-engine/geoengine/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/geo-engine/geoengine/badge.svg?branch=master)](https://coveralls.io/github/geo-engine/geoengine?branch=master)

This workspace contains the Geo Engine crates.

## Development

- While Geo Engine should build on Linux and Windows environments, we currently only support Ubuntu Linux 20.04 LTS.
- You need a recent Rust environment with a Rust nightly compiler. We recommend rustup to manage Rust `https://rustup.rs/`.
- Geo Engine uses OpenCL and therefore requires a functional OpenCL environment. On Linux you can use POCL to run OpenCL on CPUs.

### Ubuntu Users
We suggest to add the [UbuntuGIS PPA](https://wiki.ubuntu.com/UbuntuGIS) for a recent version of GDAL.
```
# Add ppa
add-apt-repository ppa:ubuntugis/ppa
# Update
apt update
```

### Dependencies

```
# Build essentials
apt install build-essential
# lld linker
apt install clang lld
# GDAL (>= 3.2.1)
apt install libgdal-dev gdal-bin
# OpenCL
apt install ocl-icd-opencl-dev
# (optional) OpenCL POCL
apt install pocl-opencl-icd
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

### Docker

#### Dev Container

Build:

`docker build --file=dev.Dockerfile --tag=geoengine:0.0.1 .`

Execute:

`docker container run --detach --name geoengine --publish 127.0.0.1:3030:8080 geoengine:0.0.1`

Inspect the container:

`docker exec -it geoengine bash`

## Troubleshooting

### Running out of memory map areas

Problem: When running Geo Engine or its tests you encounter one of the following (or similar) errors:
- `Cannot allocate Memory (OS Error 12)`
- `Os { code: 11, kind: WouldBlock, message: "Resource temporarily unavailable" }`

Solution: Adjust the system's `max_map_count` parameter:

`sudo sysctl -w vm.max_map_count=262144`

## Contributing

We are grateful for any contributions.
Please make sure to read our [contributing guide](CONTRIBUTING.md).
