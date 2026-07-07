---
title: Downsampling
---

The `Downsampling` operator decreases raster resolution by sampling values of an input raster.

If queried with a resolution that is finer than the input resolution,
downsampling is not applicable and an error is returned.

## Inputs

The `Downsampling` operator expects exactly one _raster_ input.

## Parameters

| Name                  | Type                   | Description                                                    | Examples |
| --------------------- | ---------------------- | -------------------------------------------------------------- | -------- |
| samplingMethod        | DownsamplingMethod     | Downsampling method.                                           |          |
| outputResolution      | DownsamplingResolution | Target output resolution.                                      |          |
| outputOriginReference | null or Coordinate2D   | Optional reference point used to align the output grid origin. |          |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "Downsampling",
    "params": {
        "samplingMethod": "nearestNeighbor",
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
