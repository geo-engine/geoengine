---
title: Band Filter
---

The `BandFilter` operator selects bands from a raster source by band names or band indices.

It removes all non-selected bands while preserving the original order of remaining bands.

## Inputs

The `BandFilter` operator expects exactly one _raster_ input.

## Errors

The operator returns an error if no bands are selected or if selected band names/indices
cannot be mapped to existing input bands.

## Parameters

| Name  | Type               | Description                                                                        | Examples |
| ----- | ------------------ | ---------------------------------------------------------------------------------- | -------- |
| bands | BandsByNameOrIndex | Selected bands either by names (e.g. `["nir", "red"]`) or indices (e.g. `[0, 2]`). |          |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "BandFilter",
    "params": {
        "bands": ["nir", "red"]
    },
    "sources": {
        "raster": {
            "type": "MultiBandGdalSource",
            "params": {
                "data": "sentinel-2-l2a_EPSG32632_U16_10"
            }
        }
    }
}
```
