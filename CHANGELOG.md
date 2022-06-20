# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Added `Measurement`s to vector data workflows
  - https://github.com/geo-engine/geoengine/pull/557
  - The `VectorResultDescriptor`'s field `columns` now has two fields: `data_type` and `measurement`.
  - Operators were adapted to propagate `Measurement`s if possible.
