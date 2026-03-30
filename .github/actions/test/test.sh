#!/bin/bash

# Ensure pipx binaries are on PATH
export PATH="/root/.local/bin:$PATH"

function print_headline() {
    local BOLD_WHITE_ON_CYAN="\e[1;46;37m"
    local BOLD_CYAN="\e[1;49;36m"
    local RESET_COLOR="\e[0m"
    printf "${BOLD_WHITE_ON_CYAN} ▶ ${BOLD_CYAN} $1 ${RESET_COLOR}\n" >&2
}

print_headline "Install cargo-llvm-cov"
just install

print_headline "Run Tests & Generate Code Coverage"
just test-coverage || exit 1

print_headline "Run Doctests"
# cf. https://github.com/taiki-e/cargo-llvm-cov/issues/2
just test-doc || exit 1
