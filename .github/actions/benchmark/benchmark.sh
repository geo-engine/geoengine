#!/bin/bash

function print_headline() {
    local BOLD_WHITE_ON_CYAN="\e[1;46;37m"
    local BOLD_CYAN="\e[1;49;36m"
    local RESET_COLOR="\e[0m"
    printf "${BOLD_WHITE_ON_CYAN} ▶ ${BOLD_CYAN} $1 ${RESET_COLOR}\n" >&2
}

print_headline "Install codespeed"
rustup show
cargo install cargo-binstall
cargo binstall cargo-codspeed

print_headline "Build benchmarks"
cargo codspeed build -m simulation

print_headline "Run Benchmarks"
service postgresql start
cargo codspeed run -m simulation
