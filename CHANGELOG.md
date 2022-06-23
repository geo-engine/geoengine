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

- The `Expression` operator now uses temporal alignment for 3-8 sources.

  - https://github.com/geo-engine/geoengine/pull/559
  - Added a `RasterArrayTimeAdapter` to temporally align an array of raster sources

- EBV Provider now handles non-regular data

  - https://github.com/geo-engine/geoengine/pull/564
  - **breaking**: since metadata changed to cover two variants of time defintions, overview metadata is no longer valid
