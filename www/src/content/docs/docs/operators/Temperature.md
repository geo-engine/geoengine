---
title: Temperature
---

The `Temperature` operator converts raw raster values to temperature.

## Parameters

| Name           | Type         | Description | Examples |
| -------------- | ------------ | ----------- | -------- |
| forceSatellite | integer,null |             |          |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "Temperature",
    "params": {
        "forceSatellite": 8
    },
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
