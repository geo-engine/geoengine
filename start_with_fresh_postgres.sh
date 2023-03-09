#!/bin/bash

sudo -u postgres psql -d geoengine -c "drop schema public cascade; create schema public authorization geoengine; create extension postgis;" && cargo run --features pro