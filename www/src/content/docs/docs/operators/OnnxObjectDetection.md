---
title: OnnxObjectDetection
---

The `OnnxObjectDetection` operator runs an object-detection model on a raster
and emits the detections as polygons with a `class` and a `score` column.

## Inputs

The `OnnxObjectDetection` operator expects exactly one _raster_ input. Its
pixel size must match the model's input shape and its spatial resolution must
match the resolution the model was trained at; neither is resampled.

## Outputs

The output features carry the columns `class` (category) and `score` (float),
and the time interval of the source tile that produced them.

## Parameters

| Name               | Type            | Description                                                                                                                         | Examples               |
| ------------------ | --------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ---------------------- |
| model              | MlModelName     | The object-detection model to run.                                                                                                  | `"my-detection-model"` |
| expectedResolution | number          | The spatial resolution (in linear units per pixel) the model was trained at.                                                        | `0.1`                  |
| resolutionEpsilon  | number          | Relative tolerance for the resolution check. A source is accepted when<br>`\|actual - expected\| / expected <= resolution_epsilon`. | `0.01`                 |
| layout             | DetectionLayout | How the model's raw output tensor is interpreted.                                                                                   |                        |
| numClasses         | integer         | Number of object classes the model can predict.                                                                                     | `80`                   |
| confThreshold      | number          | Confidence threshold applied to decoded detections.                                                                                 |                        |
| iouThreshold       | number          | `IoU` threshold for non-maximum suppression.                                                                                        |                        |
| classNames         | array           | Optional human-readable class labels, indexed by class id.                                                                          |                        |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "OnnxObjectDetection",
    "params": {
        "model": "my-detection-model",
        "expectedResolution": 0.1,
        "resolutionEpsilon": 0.01,
        "numClasses": 80
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
