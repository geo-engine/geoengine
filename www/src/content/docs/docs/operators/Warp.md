---
title: Warp
---

The `Warp` operator puts a raster onto another grid in one step: another spatial reference,
another resolution and another grid origin, in any combination.

For each output pixel, the operator uses the value of the input pixel under the center of the
output pixel. The output resolution can be coarser or finer than the input resolution, also
coarser in one axis and finer in the other.

Geo Engine uses `Warp` to answer queries, e.g., WMS requests, in a spatial reference or at a
resolution that differs from the processing graph's.

## Errors

The operator returns an error if the input has no spatial reference or if the input data
cannot be reprojected to the target spatial reference.

## Parameters

| Name                   | Type                      | Description                                                                      | Examples                |
| ---------------------- | ------------------------- | -------------------------------------------------------------------------------- | ----------------------- |
| targetSpatialReference | string,null               | Target spatial reference system. Defaults to the spatial reference of the input. | `"EPSG:3857"`           |
| outputResolution       | SpatialResolution or null |                                                                                  | `{"x":20000,"y":10000}` |
| outputOriginReference  | Coordinate2D or null      |                                                                                  |                         |
| method                 | WarpMethod                | Resampling method.                                                               |                         |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "Warp",
    "params": {
        "targetSpatialReference": "EPSG:3857",
        "outputResolution": {
            "x": 20000,
            "y": 10000
        }
    },
    "sources": {
        "raster": {
            "type": "GdalSource",
            "params": {
                "data": "ndvi"
            }
        }
    }
}
```
