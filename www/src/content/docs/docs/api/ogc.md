---
title: OGC Services
sidebar:
    order: 2
---

Geo Engine offers standard OGC services, so that GIS software like QGIS can display and download its results.
WMS, WFS and WCS serve a [processing graph](/docs/the-geo-engine/processing-graphs) by its id, while OGC API Tiles serves a [layer](/docs/the-geo-engine/layers).

| Service                         | Endpoint                               | Data   | Result  |
| ------------------------------- | -------------------------------------- | ------ | ------- |
| [WMS 1.3.0](#wms)               | `/api/wms/{processingGraph}`           | Raster | PNG     |
| [WFS 2.0.0](#wfs)               | `/api/wfs/{processingGraph}`           | Vector | GeoJSON |
| [WCS 1.1.0, 1.1.1](#wcs)        | `/api/wcs/{processingGraph}`           | Raster | GeoTIFF |
| [OGC API Tiles](#ogc-api-tiles) | `/api/ogc/{dataConnectorId}/{layerId}` | Raster | PNG     |

## Authentication

All OGC requests need a session, sent as a bearer token in the `Authorization` header, just like REST requests:

```http
Authorization: Bearer <session id>
```

See [REST API](/docs/api/rest-api#authentication) for how to get a session.
The CORS headers of the OGC services are configured in `[ogc.cors]`.

## Common parameters

- **Processing graph.**
  The endpoint path contains the processing graph id, and the `layers` (WMS), `typeNames` (WFS) or `identifier` (WCS) parameter repeats it.
- **Time.**
  The `time` parameter takes a time instance or an interval separated by `/` in ISO 8601, e.g., `2014-04-01T12:00:00.000Z` or `2014-01-01T00:00:00Z/2015-01-01T00:00:00Z`.
  If it is missing, the server uses `[ogc.default_time]`, which can be overridden per service with `[wms.default_time]`, `[wfs.default_time]` and `[wcs.default_time]`.
  See [Time](/docs/the-geo-engine/time).
- **CRS.**
  The service reprojects the result to the requested CRS.
  Note that WMS 1.3.0 and WCS 1.1 use latitude/longitude axis order for `EPSG:4326`.
- **Timeouts.**
  `request_timeout_seconds` in `[wms]`, `[wfs]` and `[wcs]` limits how long a request may run.

## WMS

The Web Map Service renders raster processing graphs as PNG images.
It supports the `GetCapabilities` and `GetMap` requests.

```http
GET /api/wms/{id}?service=WMS&version=1.3.0&request=GetMap&layers={id}&styles=&crs=EPSG:4326&bbox=-90,-180,90,180&width=720&height=360&format=image/png&time=2014-04-01T12:00:00.000Z
```

The `styles` parameter selects the colors.
Leave it empty for a default gradient on the first band, from black at `0` to white at `255`, or pass `custom:` followed by a raster colorizer in JSON:

```text
custom:{"type":"singleBand","band":0,"bandColorizer":{"type":"linearGradient","breakpoints":[{"value":0,"color":[0,0,0,255]},{"value":255,"color":[255,255,255,255]}],"noDataColor":[0,0,0,0],"overColor":[255,255,255,255],"underColor":[0,0,0,255]}}
```

See [Colorizer](/docs/datatypes/colorizer) for all colorizer types.
Images are limited to `[wms] max_image_width` × `max_image_height` pixels (4096 × 4096 by default).
Set `exceptions=application/json` to get errors as JSON instead of XML.

## WFS

The Web Feature Service returns the features of vector processing graphs as GeoJSON.
It supports the `GetCapabilities` and `GetFeature` requests.

```http
GET /api/wfs/{id}?service=WFS&version=2.0.0&request=GetFeature&typeNames={id}&bbox=-90,-180,90,180&srsName=EPSG:4326&time=2014-04-01T12:00:00.000Z
```

`srsName` is required.
The time interval of each feature is in its `when` member, next to `geometry` and `properties`.

## WCS

The Web Coverage Service downloads raster processing graphs as GeoTIFF.
It supports the `GetCapabilities`, `DescribeCoverage` and `GetCoverage` requests.

```http
GET /api/wcs/{id}?service=WCS&version=1.1.1&request=GetCoverage&identifier={id}&format=image/tiff&boundingbox=-90,-180,90,180,urn:ogc:def:crs:EPSG::4326&gridbasecrs=urn:ogc:def:crs:EPSG::4326&gridoffsets=-0.1,0.1&time=2014-04-01T12:00:00.000Z
```

`gridoffsets` sets the resolution of the result.
A response consists of at most `[wcs] tile_limit` tiles; for larger areas, request several coverages or use the [raster stream](/docs/the-geo-engine/queries-and-streams).

## OGC API Tiles

OGC API Tiles serves raster layers as map tiles.
Unlike the other services, it is addressed by a layer, i.e., the id of its data connector (`dataConnectorId`) and its `layerId`.
The layer's symbology decides the colors.

The landing page of a layer is `/api/ogc/{dataConnectorId}/{layerId}/`.
It links to the collection of the layer, its tile sets and the tile matrix sets:

| Tile matrix set     | Description                                                                        |
| ------------------- | ---------------------------------------------------------------------------------- |
| `WebMercatorQuad`   | The standard Web Mercator tiling that web maps use                                 |
| `CustomWebMercator` | Tiles that match Geo Engine's internal tiling, for layers in EPSG:3857             |
| `Custom`            | Tiles that match Geo Engine's internal tiling in the layer's own spatial reference |

A tile is requested by its tile matrix set, zoom level, row and column:

```http
GET /api/ogc/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles/WebMercatorQuad/{tileMatrix}/{tileRow}/{tileCol}?datetime=2014-04-01T12:00:00Z
```

The `datetime` parameter is required.
It takes a date-time or an interval, half-bounded intervals use `..`.

## Using Geo Engine in QGIS

QGIS can add Geo Engine results as WMS, WFS or WCS layers.
Since the services require a bearer token, the connection needs an authorization header:

1. Get a session id, e.g., with `ge.get_session()` in Python or `POST /api/login`.
2. In QGIS, open _Settings → Options → Authentication_ and add an authentication configuration of the type _API Header_ with the header `Authorization` and the value `Bearer <session id>`.
3. In the _Data Source Manager_, add a new WMS/WMTS, WFS or WCS connection with the URL of the service, e.g., `http://localhost:3030/api/wms/<processing graph id>`, and select the authentication configuration.
4. Connect and add the layer that has the id of the processing graph.

The session expires after 30 days, after which you need to update the header value.
