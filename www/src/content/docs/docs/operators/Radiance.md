---
title: Radiance
---

The `Radiance` operator converts raw raster values to radiance.

## Parameters

No parameters.

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "Radiance",
    "params": {},
    "sources": {
        "raster": {
            "type": "GdalSource",
            "params": {
                "data": "msg_raw"
            }
        }
    }
}
```
