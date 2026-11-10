---
title: ReTile
---

The `ReTile` operator re-tiles raster data onto a different tile grid without resampling.

Each output pixel maps to exactly one input pixel (identity). The output origin and tile
size can be overridden via the parameters; by default the source's own geo-transform origin
and the tiling specification's tile size are used.

## Inputs

The `ReTile` operator expects exactly one _raster_ input.

## Parameters

| Name     | Type                 | Description                                                                                     | Examples         |
| -------- | -------------------- | ----------------------------------------------------------------------------------------------- | ---------------- |
| tileSize | TileSize or null     | Override the output tile size.<br>If `None`, the configured default tile size is used.          | `[512,512]`      |
| origin   | Coordinate2D or null | Override the output tiling origin.<br>If `None`, the source's own geo-transform origin is used. | `{"x":2,"y":-2}` |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "ReTile",
    "params": {
        "tileSize": [512, 512],
        "origin": {
            "x": 2,
            "y": -2
        }
    },
    "sources": {
        "raster": {
            "type": "GdalSource",
            "params": {
                "data": "example"
            }
        }
    }
}
```
