---
title: Raster and Vector Data
sidebar:
    order: 5
---

Geo Engine processes two kinds of data: rasters and vectors.
Processing graphs can also produce [plots](/docs/plots/), which summarize raster or vector data as charts or statistics.

## Rasters

A raster is a time series of gridded data.
Its [result descriptor](/docs/the-geo-engine/processing-graphs#result-descriptor) describes it with:

- **Data type.**
  All pixels of a raster have the same [raster data type](/docs/datatypes/rasterdatatype): `U8`, `U16`, `U32`, `U64`, `I8`, `I16`, `I32`, `I64`, `F32` or `F64`.
  [`RasterTypeConversion`](/docs/operators/rastertypeconversion) changes it.
- **Bands.**
  A raster has one or more bands, each with a name and a [measurement](/docs/datatypes/measurement) that says what the values mean: unitless, continuous with a unit, or a classification with named classes.
  See [RasterBandDescriptor](/docs/datatypes/rasterbanddescriptor).
  [`RasterStacker`](/docs/operators/rasterstacker) combines rasters into one with more bands, and [`BandFilter`](/docs/operators/bandfilter) selects bands.
- **Spatial grid.**
  The grid is defined by a geo transform (origin coordinate and pixel size) and the pixel bounds of the data.
  The pixel size is the resolution of the raster.
  A grid is either `source`, i.e., the grid of the original data, or `derived`, i.e., computed by an operator, e.g., by a reprojection.
- **Time.**
  The time bounds and whether the time steps are regular or irregular; see [Time](/docs/the-geo-engine/time).
- **Spatial reference**; see [Spatial References](/docs/the-geo-engine/spatial-references).

Pixels can be _no data_, i.e., have no valid value.
Operators propagate no data, e.g., an [`Expression`](/docs/operators/expression) returns no data if one of its inputs is no data, unless configured otherwise.

### Tiles

Geo Engine splits rasters into tiles of a fixed size.
A GDAL dataset can set its own tile size with `tileSize`; otherwise it uses `[raster.tiling_specification]` from the server configuration, 512 × 512 pixels by default.
Tiles are the unit in which rasters are loaded, processed, cached and streamed.
The tile grid starts at the origin of the raster's spatial grid, so tiles of rasters with the same grid and tile size line up with each other.
[`ReTile`](/docs/operators/retile) moves a raster onto another tile size or origin without resampling.
See [Queries and Streams](/docs/the-geo-engine/queries-and-streams#raster-streams) for the order in which tiles arrive.

### Resolution

Operators that combine several rasters require compatible grids.
[`RasterStacker`](/docs/operators/rasterstacker) moves inputs with the same resolution onto a common grid if their pixels are aligned.
To change the resolution of a raster, use [`Downsampling`](/docs/operators/downsampling) for a coarser and [`Interpolation`](/docs/operators/interpolation) for a finer resolution.
The OGC services resample the final result to the requested resolution automatically.

## Vectors

Vector data are collections of features.
Every feature has a geometry, a [time interval](/docs/the-geo-engine/time) and values for the columns of the collection.
The result descriptor describes vector data with:

- **Data type**, i.e., the geometry type: `MultiPoint`, `MultiLineString`, `MultiPolygon`, or `Data` for features without geometry.
  Geo Engine stores all geometries as multi-geometries, so a single point is a `MultiPoint` with one coordinate.
- **Columns.**
  Each column has a feature data type (`int`, `float`, `text`, `bool`, `dateTime` or `category`) and a [measurement](/docs/datatypes/measurement).
- **Spatial reference**, and optionally the time interval and bounding box of all features.

Features arrive in chunks; see [Queries and Streams](/docs/the-geo-engine/queries-and-streams#vector-streams).

## Combining rasters and vectors

Several operators convert between rasters and vectors:

- [`RasterVectorJoin`](/docs/operators/rastervectorjoin) adds raster values as columns to vector features.
- [`Rasterization`](/docs/operators/rasterization) creates a raster from points, as a grid or a density map.
- [`PointInPolygonFilter`](/docs/operators/pointinpolygonfilter) keeps the points that lie in a set of polygons.
