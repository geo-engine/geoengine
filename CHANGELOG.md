# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added a `ClassHistogram` plot operator for creating histograms of categorical data

  - https://github.com/geo-engine/geoengine/pull/560
  - Works for Raster and Vector data

- (`ebv`) Added data range computations when generating EBV overviews

  - https://github.com/geo-engine/geoengine/pull/565
  - Metadata has new field `dataRange`, which is optional

### Changed

- Changed the temporal reference of regular raster time series from a start date to a valid time interval `dataTime`. Before and after the `dataTime`, only one loading info with nodata will be produced instead of lots of nodata-chunks with the original interval of the time series.

  - https://github.com/geo-engine/geoengine/pull/569
  - **breaking** The json dataset definition now has a `dataTime` field, instead of "start".

- Added `Measurement`s to vector data workflows

  - https://github.com/geo-engine/geoengine/pull/557
  - **breaking**:The `VectorResultDescriptor`'s field `columns` now has two fields: `data_type` and `measurement`.
  - Operators were adapted to propagate `Measurement`s if possible.

- The `Expression` operator now uses temporal alignment for 3-8 sources.

  - https://github.com/geo-engine/geoengine/pull/559
  - Added a `RasterArrayTimeAdapter` to temporally align an array of raster sources

- (`ebv`) EBV Provider now handles non-regular data

  - https://github.com/geo-engine/geoengine/pull/564
  - **breaking**: since metadata changed to cover two variants of time defintions, overview metadata is no longer valid
