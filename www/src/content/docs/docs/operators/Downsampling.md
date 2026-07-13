---
title: Downsampling
---

The `Downsampling` operator decreases raster resolution by sampling values of an input raster.

If queried with a resolution that is finer than the input resolution,
downsampling is not applicable and an error is returned.

## Inputs

The `Downsampling` operator expects exactly one _raster_ input.

## Resolution

The target resolution can be specified either as an explicit `Resolution` (in pixel units)
or as a `Fraction` that scales the input resolution.

```rust,ignore
// Scale the input resolution by a factor of 2 in both x and y directions
DownsamplingResolution::Fraction(Fraction { x: 2.0, y: 2.0 })
```

```rust,ignore
// Use an explicit resolution of 200×200 pixel units
DownsamplingResolution::Resolution(SpatialResolution { x: 200.0, y: 200.0 })
```

## Parameters

| Name                  | Type                   | Description               | Examples |
| --------------------- | ---------------------- | ------------------------- | -------- |
| samplingMethod        | DownsamplingMethod     | Downsampling method.      |          |
| outputResolution      | DownsamplingResolution | Target output resolution. |          |
| outputOriginReference | null or Coordinate2D   |                           |          |

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
