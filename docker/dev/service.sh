#!/bin/bash

# Allow Job Control to bring processes to the background and back
set -m

# Go to dir where config files are
cd /app || exit

# `/sbin/setuser geoengine` runs the given command as the user `geoengine`.
exec /sbin/setuser geoengine /usr/bin/geoengine >>/var/log/geoengine.log 2>&1 &

# TODO: make requests to setup system

# Bring service to foreground
fg
