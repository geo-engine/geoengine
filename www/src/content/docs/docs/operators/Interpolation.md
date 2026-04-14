---
title: Interpolation
---

The `Interpolation` operator increases raster resolution by interpolating values of an input raster.

If queried with a resolution that is coarser than the input resolution,
interpolation is not applicable and an error is returned.

## Inputs

The `Interpolation` operator expects exactly one _raster_ input.

## Parameters

| Name                  | Type                    | Description               | Examples |
| --------------------- | ----------------------- | ------------------------- | -------- |
| interpolation         | InterpolationMethod     | Interpolation method.     |          |
| outputOriginReference | null or Coordinate2D    |                           |          |
| outputResolution      | InterpolationResolution | Target output resolution. |          |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "Interpolation",
    "params": {
        "interpolation": "nearestNeighbor",
        "outputResolution": {
            "type": "fraction",
            "x": 2,
            "y": 2
        }
    },
    "sources": {
        "raster": {
            "type": "MultiBandGdalSource",
            "params": {
                "data": "sentinel-2-l2a_EPSG32632_U8_20"
            }
        }
    }
}
```
