#!/bin/bash

function print_headline() {
    local BOLD_WHITE_ON_CYAN="\e[1;46;37m"
    local BOLD_CYAN="\e[1;49;36m"
    local RESET_COLOR="\e[0m"
    printf "${BOLD_WHITE_ON_CYAN} ▶ ${BOLD_CYAN} $1 ${RESET_COLOR}\n" >&2
}

print_headline "Checking with Rustfmt"
cargo fmt --all -- --check

print_headline "Checking with Clippy"
cargo clippy --all-targets --locked -- -D warnings

print_headline "Checking with SQLFluff"
pipx run sqlfluff==3.3.0 lint

print_headline "Verifying expression dependencies workspace"
rustup toolchain install nightly
chmod +x ./.scripts/check-expression-deps.rs
./.scripts/check-expression-deps.rs

print_headline "Running tests"
cargo test --locked --verbose
