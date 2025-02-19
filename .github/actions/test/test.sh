#!/bin/bash

echo "[Checking with Rustfmt]"
cargo fmt --all -- --check

echo "[Checking with Clippy]"
cargo clippy --all-targets --locked -- -D warnings

echo "[Checking with SQLFluff]"
pipx run sqlfluff==3.3.0 lint

echo "[Verifying expression dependencies workspace]"
rustup toolchain install nightly
chmod +x ./.scripts/check-expression-deps.rs
./.scripts/check-expression-deps.rs

echo "[Running tests]"
cargo test --locked --verbose
