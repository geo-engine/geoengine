---
title: ONNX
---

The `Onnx` operator applies a machine learning model to a raster stack.

## Parameters

| Name  | Type        | Description | Examples |
| ----- | ----------- | ----------- | -------- |
| model | MlModelName |             |          |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "Onnx",
    "params": {
        "model": "my-model"
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
