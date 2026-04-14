#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

podman build -f $SCRIPT_DIR/Dockerfile --tag geoengine-interactive-ml:latest .
