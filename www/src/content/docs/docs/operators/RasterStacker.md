---
title: Raster Stacker
---

The `RasterStacker` stacks all of its inputs into a single raster time series.
It queries all inputs and combines them by band, space, and then time.

The output raster has as many bands as the sum of all input bands.
Tiles are automatically temporally aligned.

All inputs must have the same data type and spatial reference.

## Inputs

The `RasterStacker` operator expects multiple raster inputs.

## Parameters

| Name        | Type        | Description                                                                                                                                                                                                                              | Examples |
| ----------- | ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| renameBands | RenameBands | Strategy for deriving output band names.<br><br>- `default`: appends ` (n)` with the smallest `n` that avoids a conflict.<br>- `suffix`: appends one suffix per input.<br>- `rename`: explicitly provides names for all resulting bands. |          |

## Sources

| Name    | Type  | Description |
| ------- | ----- | ----------- |
| rasters | array |             |

## Examples

```json
{
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
                    "data": "example-a"
                }
            },
            {
                "type": "GdalSource",
                "params": {
                    "data": "example-b"
                }
            }
        ]
    }
}
```
