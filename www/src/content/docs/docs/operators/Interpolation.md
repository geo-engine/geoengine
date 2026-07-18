---
title: Interpolation
---

The `Interpolation` operator increases raster resolution by interpolating values of an input raster.

If queried with a resolution that is coarser than the input resolution,
interpolation is not applicable and an error is returned.

## Inputs

The `Interpolation` operator expects exactly one _raster_ input.

## Resolution

The target resolution can be specified either as an explicit `Resolution` (in pixel units)
or as a `Fraction` that scales the input resolution.

```rust,ignore
// Scale the input resolution by a factor of 2 in both x and y directions
InterpolationResolution::Fraction(Fraction { x: 2.0, y: 2.0 })
```

```rust,ignore
// Use an explicit resolution of 50×50 pixel units
InterpolationResolution::Resolution(SpatialResolution { x: 50.0, y: 50.0 })
```

## Parameters

| Name                  | Type                    | Description               | Examples |
| --------------------- | ----------------------- | ------------------------- | -------- |
| interpolation         | InterpolationMethod     | Interpolation method.     |          |
| outputResolution      | InterpolationResolution | Target output resolution. |          |
| outputOriginReference | null or Coordinate2D    |                           |          |

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
