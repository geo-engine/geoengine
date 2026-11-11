---
title: Multidimensional GDAL Source
---

The [`MdGdalSource`] is a source operator that reads multidimensional (netCDF/Zarr) arrays
through GDAL, emitting one 2D raster tile per z slice.

## Errors

If the given dataset does not exist, is not readable, or the requested bands are not
part of the dataset, an error is thrown.

## Parameters

| Name | Type   | Description                              | Examples            |
| ---- | ------ | ---------------------------------------- | ------------------- |
| data | string | Dataset name or identifier to be loaded. | `"era5-timeseries"` |

## Examples

```json
{
    "type": "MdGdalSource",
    "params": {
        "data": "era5-timeseries"
    }
}
```
