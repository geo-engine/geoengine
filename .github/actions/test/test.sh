#!/bin/bash

function print_headline() {
    local BOLD_WHITE_ON_CYAN="\e[1;46;37m"
    local BOLD_CYAN="\e[1;49;36m"
    local RESET_COLOR="\e[0m"
    printf "${BOLD_WHITE_ON_CYAN} ▶ ${BOLD_CYAN} $1 ${RESET_COLOR}\n" >&2
}

print_headline "Install cargo-llvm-cov"
cargo install --locked cargo-llvm-cov

print_headline "Run Tests & Generate Code Coverage"
service postgresql start
cargo llvm-cov \
    --locked \
    --all-features \
    --profile ci \
    --lcov \
    --output-path lcov.info \
    || exit 1

print_headline "Run Doctests"
# cf. https://github.com/taiki-e/cargo-llvm-cov/issues/2
cargo test --doc --all-features --profile ci --locked || exit 1
