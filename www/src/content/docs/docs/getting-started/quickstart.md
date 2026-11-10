---
title: Quickstart
sidebar:
    order: 1
---

This guide runs a local Geo Engine server, registers a processing graph and queries its result in three ways: with `curl`, with the Python library and as a WMS map.

## Run a local server

Geo Engine is developed in the [Geo Engine monorepo](https://github.com/geo-engine/geoengine).
To run the server from source, you need:

- the Rust toolchain,
- GDAL (≥ 3.8.4) and PROJ (≥ 9.4) with their development headers,
- PostgreSQL with the PostGIS extension, with a user and database `geoengine` (password `geoengine`),
- [`just`](https://just.systems/) as the command runner.

The [development container](https://quay.io/repository/geoengine/devcontainer) comes with all dependencies pre-installed.

Start the server from the repository root:

```bash
just backend run
```

The server listens on `http://localhost:3030` and serves the API under `/api`.
When it creates its database schema, it loads the example datasets from `geoengine/test_data/`, among them the raster dataset `ndvi` and the vector dataset `ne_10m_ports`.
An initial administrator `admin@localhost` with password `adminadmin` is created as well.
Both come from `geoengine/Settings-default.toml`, which you can override in a `Settings.toml` next to it or with environment variables like `GEOENGINE__WEB__BIND_ADDRESS`.

You can browse and try all endpoints in the Swagger UI at `http://localhost:3030/api/swagger-ui/`.

## Create a session

Every request needs a session.
For a first test, an anonymous session is enough:

```bash
curl -X POST http://localhost:3030/api/anonymous
```

The response contains the session `id`.
Pass it as a bearer token in all further requests:

```bash
export TOKEN=<session id>
```

See [Users and Permissions](/docs/the-geo-engine/users-and-permissions) for registered and OIDC users.

## Register a processing graph

A [processing graph](/docs/the-geo-engine/processing-graphs) describes what to compute.
The simplest one loads a dataset with a source operator:

```bash
curl -X POST http://localhost:3030/api/processingGraphs \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"type": "Raster", "operator": {"type": "GdalSource", "params": {"data": "ndvi"}}}'
```

The response is the id of the processing graph:

```json
{"id": "<processing graph id>"}
```

Registering does not compute anything yet.
Ask for the metadata of the result to see its data type, spatial reference, spatial grid, time and bands:

```bash
curl http://localhost:3030/api/processingGraphs/<processing graph id>/metadata \
    -H "Authorization: Bearer $TOKEN"
```

## Query the result as a map

The WMS endpoint renders the result as a PNG.
Only now does Geo Engine load and process the data, and only for the requested area and time:

```bash
curl -o ndvi.png -H "Authorization: Bearer $TOKEN" \
    "http://localhost:3030/api/wms/<processing graph id>?service=WMS&version=1.3.0&request=GetMap&layers=<processing graph id>&styles=&width=720&height=360&crs=EPSG:4326&bbox=-90,-180,90,180&format=image/png&time=2014-04-01T12:00:00.000Z"
```

WMS 1.3.0 uses latitude/longitude order for `EPSG:4326`, so the bounding box is `minLat,minLon,maxLat,maxLon`.
See [OGC Services](/docs/api/ogc) for all services and parameters, and for how to use them in QGIS.

## Use the Python library

The [`geoengine` Python library](/docs/python/) wraps the API.
Install it with `pip install geoengine`:

```python
from datetime import datetime

import geoengine as ge

ge.initialize("http://localhost:3030/api")  # creates an anonymous session

ports = ge.register_processing_graph(
    {
        "type": "Vector",
        "operator": {"type": "OgrSource", "params": {"data": "ne_10m_ports"}},
    }
)

time = datetime.fromisoformat("2014-04-01T12:00:00+00:00")

df = ports.get_dataframe(
    ge.QueryRectangle(
        ge.BoundingBox2D(-180.0, -90.0, 180.0, 90.0),
        ge.TimeInterval(time, time),
    )
)

df.head()
```

`get_dataframe` returns a GeoPandas data frame.
For rasters, `get_xarray` returns an xarray `DataArray`, and `raster_stream` and `vector_stream` iterate over the result tile by tile or chunk by chunk.

## Use the web app

The `gis` web app lets you browse layers and build processing graphs interactively.
To run it against your local server, start it from the repository root:

```bash
just ui run gis
```
