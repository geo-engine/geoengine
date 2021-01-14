# geo engine

![CI](https://github.com/geo-engine/geoengine/workflows/CI/badge.svg)

This workspace contains the geo engine crates.

# Development

- While geo engine should build on Linux and Windows environments, we currently only support Ubuntu Linux 20.04 LTS 
- You need a recent Rust environment with a Rust nightly compiler. We recommend rustup to manage Rust `https://rustup.rs/`
- geo engine uses OpenCL and therefore requires a functional OpenCL environment. On Linux you can use POCL to run OpenCL on CPUs.

## Dependencies
 ```
# Build essentials
sudo apt build-essential
# GDAL
sudo apt install libgdal-dev gdal-bin 
# OpenCL
sudo apt install ocl-icd-opencl-dev
# (optional) OpenCL POCL
sudo apt install  pocl-opencl-icd
```

## Lints
Please run Clippy with 
`cargo clippy --all-targets --all-features`
before creating a pull request.

## Testing
Please provide tests with all new features and run
`cargo test`
before creating a pull request.

Edit `Settings-test.toml` for environment specific test parameters.

## Benchmarks
For performance-critical features, we aim to provide benchmarks in the `benches` directory.
If you plan on optimizing a feature of Geo Engine, please confirm it this way.

# Deployment
Deploy an instance using `cargo run --package geoengine-services --bin main --release`

## Features
The PostgreSQL storage backend can optionally be enabled using `--features postgres` in the `cargo` command.

## Configuration
Copy `Settings-default.toml` to `Settings.toml` and edit per your requirements.

## Docker

### Dev Container

Build with `sudo docker build --file=dev.Dockerfile --tag=geoengine:0.0.1 .`.
Execute with `sudo docker container run --detach --name geoengine --publish 127.0.0.1:3030:8080 geoengine:0.0.1`.
Inspect the container with `sudo docker exec -it geoengine bash`.
