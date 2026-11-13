---
title: AddTileOverlap
---

The `AddTileOverlap` operator equips every output tile with an overlap (halo) around its core region.

For each tile, neighboring data is fetched so that convolutions or other neighborhood
computations have input beyond the tile boundary. Regions beyond the dataset extent are
no-data. Coverage and queries remain defined by the tile cores.

## Inputs

The `AddTileOverlap` operator expects exactly one _raster_ input without overlap.

## Parameters

| Name    | Type  | Description                                                                | Examples  |
| ------- | ----- | -------------------------------------------------------------------------- | --------- |
| overlap | array | Overlap halo in pixels `[rows, columns]` added on every side of each tile. | `[16,16]` |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "AddTileOverlap",
    "params": {
        "overlap": [16, 16]
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
