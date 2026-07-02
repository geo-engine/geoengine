---
title: OGR Source
---

The [`OgrSource`] is a source operator that reads vector data using OGR (part of GDAL).
The counterpart for raster data is the [`GdalSource`].

## Errors

If the given dataset does not exist or is not readable, an error is thrown.

## Parameters

| Name                | Type       | Description                                                                          | Examples |
| ------------------- | ---------- | ------------------------------------------------------------------------------------ | -------- |
| data                | string     | Dataset name or identifier to be loaded.                                             | `"ndvi"` |
| attributeProjection | array,null | _Optional_: list of attributes to include. When `None`, all attributes are included. |          |

## Examples

```json
{
    "type": "OgrSource",
    "params": {
        "data": "ndvi"
    }
}
```
