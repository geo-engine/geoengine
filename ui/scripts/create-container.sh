#!/bin/sh

# Build a container for the given project and push it to quay.io.
# Note: this script requires the geoengione-container repository to be cloned in the same directory as the geoengine-ui repository.

podman build \
            --tag "geoengine-ui:${TAG}" \
            --build-arg GEOENGINE_UI_PROJECT=${UI_PROJECT} \
            -f ../container/geoengine-ui/Dockerfile \
            ..

podman login -u="${QUAY_IO_USER}" -p="${QUAY_IO_TOKEN}" quay.io

podman push geoengine-ui:${TAG} quay.io/geoengine/geoengine-ui:${TAG}
