---
title: Overview
sidebar:
    order: 0
---

Geo Engine is a server for processing and visualizing geospatial data.
It reads raster and vector data, combines and transforms them in _processing graphs_, and delivers the results as maps, files, data frames or plots.
Every piece of data has a time dimension, so time series are first-class citizens.
Results are computed on demand and streamed, which means Geo Engine can work on datasets much larger than the available memory.

## Who uses Geo Engine and how

All clients talk to the same server through its [REST API](/docs/api/rest-api).

| Who                          | Interface                                                                                    | Typical tasks                                                                   |
| ---------------------------- | -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| Data scientists              | [`geoengine` Python library](/docs/python/), often in Jupyter                                | Build processing graphs, load results into GeoPandas or xarray, train ML models |
| Domain users                 | Web apps: `gis` for exploring and analysing data, `eodyssey`, and domain-specific dashboards | Browse layers, combine data with operators, view time series on a map           |
| Administrators               | `manager` web app                                                                            | Manage datasets, layers, data connectors, users, roles and permissions          |
| GIS software (e.g., QGIS)    | [OGC services](/docs/api/ogc): WMS, WFS, WCS, OGC API Tiles                                  | Display and download Geo Engine results in existing tools                       |
| Developers of other software | [REST API](/docs/api/rest-api) with generated clients for Python, Rust and TypeScript        | Integrate Geo Engine into their own applications                                |

## Where the data comes from

Geo Engine reads files through [GDAL/OGR](https://gdal.org/), so it supports most raster and vector formats.
Data enters Geo Engine in three ways:

- **Internal datasets** are files that the server knows about, e.g., GeoTIFFs or GeoPackages on a volume.
  See [Datasets](/docs/the-geo-engine/datasets).
- **Uploads** are files that users send to the server and turn into their own datasets.
- **Data connectors** connect to other systems, such as STAC catalogs, GBIF or Pangaea, and make their data available without copying it.
  See [Data Connectors](/docs/the-geo-engine/data-connectors).

[Layers and layer collections](/docs/the-geo-engine/layers) organize all of this data for browsing.

## Next steps

- Follow the [Quickstart](/docs/getting-started/quickstart) to run a first query.
- Read [Processing Graphs](/docs/the-geo-engine/processing-graphs) and [Queries and Streams](/docs/the-geo-engine/queries-and-streams) to understand how Geo Engine computes results.
