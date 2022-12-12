# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added a handler (`/layers/{provider}/{layer}/workflowId`) to register a layer by id and return the resulting workflow id.

  - https://github.com/geo-engine/geoengine/pull/692

- Added a new operator `XGBoost`. This operator allows to use a pretrained model in order to make predictions based on some set of raster tile data.

  - https://github.com/geo-engine/geoengine/pull/639

- Added a handler (`/available`) to the API to check if the service is available.

  - https://github.com/geo-engine/geoengine/pull/681

- Added spatial resolution to the `RasterResultDescriptor` that tracks the native resolution of the raster data through raster operations.

  - https://github.com/geo-engine/geoengine/pull/618

- Added traits and methods for updating / mapping the pixels of grid types. Also added a trait for creating grid types using a function for each pixels location.

  - https://github.com/geo-engine/geoengine/pull/561

- Added a layers API that allows browsing datasets, stored layers and external data in a uniform fashion

  - https://github.com/geo-engine/geoengine/pull/554

- Added a `ClassHistogram` plot operator for creating histograms of categorical data

  - https://github.com/geo-engine/geoengine/pull/560
  - Works for Raster and Vector data

- (`ebv`) Added data range computations when generating EBV overviews

  - https://github.com/geo-engine/geoengine/pull/565
  - Metadata has new field `dataRange`, which is optional

- Added download of workflow metadata (description, result descriptor, citations) as zip file

  - https://github.com/geo-engine/geoengine-python/pull/65

- Added Open ID Connect authentication

  - https://github.com/geo-engine/geoengine/pull/587

- Added statistics for vector data

  - https://github.com/geo-engine/geoengine/pull/614

- Added a `RasterKernel` operator

  - https://github.com/geo-engine/geoengine/pull/644

- Added a `RasterScale` operator

  - https://github.com/geo-engine/geoengine/pull/610

- Added method to create dataset from the result of the workflow via a task

  - https://github.com/geo-engine/geoengine/pull/675

### Changed

- The Interpolation operator's input resolution can now be set to `native` to take the best available resolution, if it is known.

  - https://github.com/geo-engine/geoengine/pull/618

- Removed the custom Geo Bon EBV portal handlers. Instead, the EBV hierarchy is now browsed through the layer collection API.

  - https://github.com/geo-engine/geoengine/pull/581

- Changed the temporal reference of regular raster time series from a start date to a valid time interval `dataTime`. Before and after the `dataTime`, only one loading info with nodata will be produced instead of lots of nodata-chunks with the original interval of the time series.

  - https://github.com/geo-engine/geoengine/pull/569
  - **breaking** The json dataset definition now has a `dataTime` field, instead of "start".

- No-data pixels in a Raster are now represented by a validity mask.

  - https://github.com/geo-engine/geoengine/pull/561
  - `MaskedGrid` replaces `Grid` in `GridOrEmpty` / `RasterTile`
  - GeoTIFF files created by the engine contain the validity mask if not specified otherwise.

- Refactored dataset ids and external provders

  - https://github.com/geo-engine/geoengine/pull/554
  - **breaking** the parameters of the source operators changed which makes old workflow jsons incompatible
  - **breaking** the id of datasets changed which makes old dataset definition jsons incompatible

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

- `DateTime` now serializes to ISO 8601 format

  - https://github.com/geo-engine/geoengine/pull/590
  - Uses the same format now as `Display` or `ToString`

- Changed `Statistics` input and output format

  - https://github.com/geo-engine/geoengine/pull/614
  - The input parameters now include a field `column_names` to select vector columns/alias raster inputs
  - The output was changed to a map from column names/raster aliases to the respective statistics

- The Expression uses a Pratt Parser instead of the previously used PrecClimber from `pest.rs`.

  - https://github.com/geo-engine/geoengine/pull/641

- The `Settings-default.toml` now contains an entry `gdal.allowed_drivers` that specifies all allowed drivers for GDAL.

  - https://github.com/geo-engine/geoengine/pull/659
