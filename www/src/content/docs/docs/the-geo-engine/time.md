---
title: Time
sidebar:
    order: 4
---

Time is never optional in Geo Engine.
Every raster tile and every feature has a time interval, and every query has one, too.
Data without a time reference gets a time interval that covers all time.

## Time intervals

A [time interval](/docs/datatypes/timeinterval) is half-open: `[start, end)`.
The start is included, the end is not.
For example, the interval for the year 2014 is `[2014-01-01T00:00:00Z, 2015-01-01T00:00:00Z)`.
Two consecutive intervals therefore share a boundary without overlapping.

An interval whose start equals its end is a single [time instance](/docs/datatypes/timeinstance).
WMS requests often use such an instance to ask for the data valid at one point in time.

Geo Engine stores time as milliseconds since the Unix epoch in UTC.
The API accepts ISO 8601 strings and millisecond timestamps.

## Querying in time

A query returns all data whose time interval intersects the query's time interval.
For rasters, this means the stream contains one time slice for every raster time step that intersects the query, in ascending order and without gaps (see [Queries and Streams](/docs/the-geo-engine/queries-and-streams#raster-streams)).
The time slices keep their own intervals, so a query for one day of a monthly raster returns a tile valid for the whole month.

## Regular and irregular rasters

The result descriptor of a raster describes its time dimension (see [Processing Graphs](/docs/the-geo-engine/processing-graphs#result-descriptor)):

- **Regular** rasters have time steps of a fixed [time step](/docs/datatypes/timestep), e.g., one month, counted from an origin.
- **Irregular** rasters have time slices of varying length, e.g., satellite scenes at irregular acquisition dates.

It also states the time bounds of the data, if they are known.

## Operators that work with time

Several operators change or use the time dimension:

- [`TemporalRasterAggregation`](/docs/operators/temporalrasteraggregation) aggregates raster time slices into longer steps, e.g., monthly means from daily data.
- [`TimeShift`](/docs/operators/timeshift) moves the time of the data, e.g., to compare a year with the previous one.
- [`TimeProjection`](/docs/operators/timeprojection) projects the time intervals of vector features to a coarser granularity, e.g., from days to years.
- [`RasterVectorJoin`](/docs/operators/rastervectorjoin) aggregates raster values over the time interval of each feature.

## Time in the OGC services

OGC requests carry the time in their `time` (WMS, WFS, WCS) or `datetime` (OGC API Tiles) parameter.
If a WMS, WFS or WCS request has no time, the server uses a default from `[ogc.default_time]` in the configuration.
OGC API Tiles requests must always contain a `datetime`.
See [OGC Services](/docs/api/ogc).
