---
title: Raster Type Conversion
---

The `RasterTypeConversion` operator changes the data type of raster pixels.

Applying this conversion may cause precision loss.
For example, converting `F32` value `3.1` to `U8` results in `3`.

If a value is outside of the range of the target data type,
it is clipped to the valid range of that type.
For example, converting `F32` value `300.0` to `U8` results in `255`.

## Inputs

The `RasterTypeConversion` operator expects exactly one _raster_ input.

## Parameters

| Name           | Type           | Description              | Examples |
| -------------- | -------------- | ------------------------ | -------- |
| outputDataType | RasterDataType | Output raster data type. |          |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "RasterTypeConversion",
    "params": {
        "outputDataType": "U16"
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
