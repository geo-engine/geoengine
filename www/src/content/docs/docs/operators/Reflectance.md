---
title: Reflectance
---

The `Reflectance` operator converts radiance values to reflectance.

## Parameters

| Name            | Type         | Description | Examples |
| --------------- | ------------ | ----------- | -------- |
| solarCorrection | boolean      |             |          |
| forceHRV        | boolean      |             |          |
| forceSatellite  | integer,null |             |          |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "Reflectance",
    "params": {
        "solarCorrection": true,
        "forceHRV": false
    },
    "sources": {
        "raster": {
            "type": "GdalSource",
            "params": {
                "data": "msg"
            }
        }
    }
}
```
