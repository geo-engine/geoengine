---
title: Bandwise Expression
---

The `BandwiseExpression` operator performs a pixel-wise mathematical expression on each band of a raster source.
For more details on the expression syntax, see the `Expression` operator.
Note that in the `BandwiseExpression` operator it is only possible to map one pixel value to another and not reference any other pixels or bands.
The variable name for the pixel value is `x`.

## Errors

The parsing of the expression can fail if there are, for example, syntax errors.

## Parameters

| Name       | Type           | Description                                                                                                          | Examples                     |
| ---------- | -------------- | -------------------------------------------------------------------------------------------------------------------- | ---------------------------- |
| expression | string         | Scalar expression applied to each band value.                                                                        | `"x * 2.0"`<br>`"ln(1 / x)"` |
| outputType | RasterDataType | Output raster data type for the computed band values.                                                                |                              |
| mapNoData  | boolean        | Whether NO DATA values should be mapped with the expression.<br>Otherwise, they are mapped automatically to NO DATA. | `false`<br>`true`            |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "BandwiseExpression",
    "params": {
        "expression": "x * 2.0",
        "outputType": "F64",
        "mapNoData": false
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
