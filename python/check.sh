#!/bin/bash

set -e # exit on error

set -T

trap '! [[ "$BASH_COMMAND" =~ ^(echo|printf) ]] &&
      printf "+ %s\n" "$BASH_COMMAND"' DEBUG

function echoerr() {
    echo "$@" 1>&2;
}

echoerr "Check code style"

python3 -m ruff format --check

echoerr "Check code with linter"

python3 -m ruff check

echoerr "Check code with type checker"

python3 -m mypy geoengine
python3 -m mypy tests

echoerr "Running tests"

pytest

echoerr "Running examples"

python3 test_all_notebooks.py
