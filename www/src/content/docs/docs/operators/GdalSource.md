---
title: GDAL Source
---

The [`GdalSource`] is a source operator that reads raster data using GDAL.
The counterpart for vector data is the [`OgrSource`].

## Errors

If the given dataset does not exist or is not readable, an error is thrown.

## Parameters

| Name          | Type         | Description                                                                                                                                 | Examples |
| ------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| data          | string       | Dataset name or identifier to be loaded.                                                                                                    | `"ndvi"` |
| overviewLevel | integer,null | _Optional_: overview level to use.<br><br>If not provided, the data source will determine the resolution, i.e., uses its native resolution. | `3`      |

## Examples

```json
{
    "type": "GdalSource",
    "params": {
        "data": "ndvi",
        "overviewLevel": null
    }
}
```
