---
title: Multi Band GDAL Source
---

The [`MultiBandGdalSource`] is a source operator that reads multi-band raster data using GDAL.

## Parameters

| Name          | Type         | Description                                                                                                                                 | Examples |
| ------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| data          | string       | Dataset name or identifier to be loaded.                                                                                                    | `"ndvi"` |
| overviewLevel | integer,null | _Optional_: overview level to use.<br><br>If not provided, the data source will determine the resolution, i.e., uses its native resolution. | `3`      |

## Examples

```json
{
    "type": "MultiBandGdalSource",
    "params": {
        "data": "sentinel-2-l2a_EPSG32632_U16_10",
        "overviewLevel": null
    }
}
```
