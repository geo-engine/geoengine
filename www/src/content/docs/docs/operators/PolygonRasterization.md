---
title: Polygon Rasterization
---

The `PolygonRasterization` operator creates a raster from a polygon vector source.

A pixel is covered by a polygon if its center lies inside the polygon (holes are respected).
Pixels that are not covered by any polygon are NO DATA.

The value of the covered pixels is

- a constant (`1` by default),
- the number of polygons that cover the pixel (`count`), or
- the value of a numeric column (`attribute`), aggregated over all polygons that cover the pixel.
  Features with a null value are skipped, unless `ignoreNoData` is `false`, which makes all pixels they cover NO DATA.
  `first`, `last`, `min` and `max` keep the measurement of the column, e.g., its classes.
  `sum`, `mean` and `medianEstimate` create new values, so a classification becomes unitless.
  The median is exact for up to five values and estimated by the P² algorithm for more.

The time slices of the output raster are

- `featureTimes` (default): bounded by the start and end times of the features.
  Each slice contains exactly the features that are valid during it, and slices without features are NO DATA.
  This needs to scan the times of all features of the source for each query, so prefer `regular` for large data.
- `regular`: slices of a fixed time step, each containing all features whose time intersects it.
- `ignore`: one slice that is valid forever and contains all features.

## Clipping a raster with polygons

Rasterize the polygons on the grid of the raster and mask the raster with the NO DATA pixels of the result,
using a `RasterStacker` and an `Expression` like `if B IS NODATA { NODATA } else { A }`.
Both rasters need

- the same resolution and origin modulo the resolution, e.g., the default origin `(0, 0)` for most grids,
- the same data type, i.e., set `outputType` to the data type of the raster, and
- the same regular time steps, i.e., use the `regular` time of the raster, or slices that the stacker can align.

## Errors

If the source is not a polygon source, the column does not exist or is not numeric,
or the constant value cannot be represented by the output type, an error will be thrown.

## Parameters

| Name              | Type                      | Description                                                                                                                                                   | Examples                                                                                                                          |
| ----------------- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| spatialResolution | SpatialResolution         | Spatial resolution of the output grid in x/y direction.                                                                                                       | `{"x":10,"y":10}`                                                                                                                 |
| originCoordinate  | Coordinate2D              | Origin coordinate to which the raster grid is aligned.<br><br>If omitted, the grid is aligned to `(0, 0)`.                                                    | `{"x":0,"y":0}`                                                                                                                   |
| value             | PolygonRasterizationValue | The value that a polygon burns into the pixels it covers.<br><br>If omitted, each covered pixel gets the value `1`.                                           | `{"value":1}`<br>`{"column":"class","ignoreNoData":true}`<br>`{"type":"attribute","column":"landCoverClass","aggregation":"max"}` |
| outputType        | RasterDataType or null    |                                                                                                                                                               | `"U8"`                                                                                                                            |
| outputMeasurement | Measurement or null       |                                                                                                                                                               |                                                                                                                                   |
| time              | PolygonRasterizationTime  | How the time slices of the output raster are derived from the features.<br><br>If omitted, the slices are bounded by the start and end times of the features. | `{"type":"regular","step":{"granularity":"years","step":1}}`                                                                      |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| vector | VectorOperator | An operator that produces vector data. |

## Examples

```json
{
    "type": "PolygonRasterization",
    "params": {
        "spatialResolution": {
            "x": 0.1,
            "y": 0.1
        },
        "value": {
            "type": "constant",
            "value": 1
        }
    },
    "sources": {
        "vector": {
            "type": "OgrSource",
            "params": {
                "data": "germany"
            }
        }
    }
}
```

```json
{
    "type": "PolygonRasterization",
    "params": {
        "spatialResolution": {
            "x": 10,
            "y": 10
        },
        "originCoordinate": {
            "x": 0,
            "y": 0
        },
        "value": {
            "type": "attribute",
            "column": "landCoverClass",
            "aggregation": "max"
        },
        "outputType": "U8",
        "time": {
            "type": "regular",
            "step": {
                "granularity": "years",
                "step": 1
            }
        }
    },
    "sources": {
        "vector": {
            "type": "OgrSource",
            "params": {
                "data": "land_cover"
            }
        }
    }
}
```

```json
{
    "type": "Expression",
    "params": {
        "expression": "if B IS NODATA { NODATA } else { A }",
        "outputType": "U8",
        "mapNoData": true
    },
    "sources": {
        "raster": {
            "type": "RasterStacker",
            "params": {
                "renameBands": {
                    "type": "default"
                }
            },
            "sources": {
                "rasters": [
                    {
                        "type": "GdalSource",
                        "params": {
                            "data": "ndvi"
                        }
                    },
                    {
                        "type": "PolygonRasterization",
                        "params": {
                            "spatialResolution": {
                                "x": 0.1,
                                "y": 0.1
                            },
                            "outputType": "U8",
                            "time": {
                                "type": "regular",
                                "step": {
                                    "granularity": "months",
                                    "step": 1
                                }
                            }
                        },
                        "sources": {
                            "vector": {
                                "type": "OgrSource",
                                "params": {
                                    "data": "germany"
                                }
                            }
                        }
                    }
                ]
            }
        }
    }
}
```
