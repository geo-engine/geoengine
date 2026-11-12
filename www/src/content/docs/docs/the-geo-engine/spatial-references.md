---
title: Spatial References
sidebar:
    order: 6
---

Every raster and vector result in Geo Engine has a spatial reference system (SRS), e.g., `EPSG:4326` for WGS 84 latitude/longitude or `EPSG:32632` for UTM zone 32N.
The SRS is part of the [result descriptor](/docs/the-geo-engine/processing-graphs#result-descriptor).
Spatial references are written as `AUTHORITY:CODE`.
Geo Engine supports the `EPSG`, `SR-ORG`, `IAU2000` and `ESRI` authorities.

`GET /spatialReferenceSpecification/{srsString}` returns the name, PROJ string, axis order and area of use of a spatial reference.

## Reprojection is explicit

Operators with several inputs require that all inputs have the same spatial reference.
Geo Engine never reprojects inputs implicitly.
If you want to combine data in different spatial references, add a [`Reprojection`](/docs/operators/reprojection) operator to the processing graph:

```json
{
    "type": "Reprojection",
    "params": {
        "targetSpatialReference": "EPSG:4326"
    },
    "sources": {
        "source": {
            "type": "GdalSource",
            "params": {
                "data": "dataset-in-utm"
            }
        }
    }
}
```

This makes the cost and the effects of a reprojection visible: reprojecting a raster resamples its pixels onto a new grid, and reprojecting vectors transforms every coordinate.
The [`Reprojection`](/docs/operators/reprojection) reference describes how the operator derives the target grid of a raster.

When a processing graph contains inputs with different spatial references and no reprojection, Geo Engine reports an error when it prepares the processing graph, not during a query.

## Reprojection of results

The OGC services are an exception to this rule.
A WMS, WCS, WFS or OGC API Tiles request names the CRS of the response, and the service reprojects the final result to it.
Raster results are also resampled to the requested grid.
This lets clients like QGIS show a processing graph in the CRS of their map without changing the processing graph.
