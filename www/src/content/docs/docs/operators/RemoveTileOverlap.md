---
title: RemoveTileOverlap
---

The `RemoveTileOverlap` operator crops the overlap halo from all tiles of its input raster.

It is the inverse of `AddTileOverlap`: each tile shrinks symmetrically while its core region
and georeference stay untouched. Removing all overlap restores plain tiles that every
operator accepts. Use it after ML segmentation to crop model output back to cores.

## Inputs

The `RemoveTileOverlap` operator expects exactly one _raster_ input.

## Parameters

| Name   | Type       | Description                                                                                               | Examples  |
| ------ | ---------- | --------------------------------------------------------------------------------------------------------- | --------- |
| amount | array,null | Halo cropped from every side in pixels `[rows, columns]`.<br>If `None`, all available overlap is removed. | `[16,16]` |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "RemoveTileOverlap",
    "params": {},
    "sources": {
        "raster": {
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
    }
}
```
