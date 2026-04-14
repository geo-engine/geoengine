---
title: Temporal Raster Aggregation
---

The `TemporalRasterAggregation` operator aggregates a raster time series into uniform time windows.
The output starts with the first window that contains the query start and contains all windows
that overlap the query interval.

Pixel values are computed by aggregating all input rasters that contribute to the current window.

## Inputs

The `TemporalRasterAggregation` operator expects exactly one _raster_ input.

## Errors

If the aggregation method is `first`, `last`, or `mean` and the input raster has no NO DATA value,
an error is returned.

## Parameters

| Name            | Type                   | Description                                                                                                                                                                                      | Examples |
| --------------- | ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------- |
| aggregation     | Aggregation            | Aggregation method for values within each time window.<br><br>Encountering NO DATA makes the aggregation result NO DATA unless<br>`ignoreNoData` is `true` for the selected aggregation variant. |          |
| outputType      | null or RasterDataType |                                                                                                                                                                                                  |          |
| window          | TimeStep               | Window size and granularity for the output time series.                                                                                                                                          |          |
| windowReference | null or TimeInstance   |                                                                                                                                                                                                  |          |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "TemporalRasterAggregation",
    "params": {
        "aggregation": {
            "type": "mean",
            "ignoreNoData": true
        },
        "window": {
            "granularity": "months",
            "step": 1
        }
    },
    "sources": {
        "raster": {
            "type": "Expression",
            "params": {
                "expression": "(A - B) / (A + B)",
                "outputType": "F32",
                "outputBand": {
                    "name": "NDVI",
                    "measurement": {
                        "type": "unitless"
                    }
                },
                "mapNoData": false
            },
            "sources": {
                "raster": {
                    "type": "GdalSource",
                    "params": {
                        "data": "ndvi"
                    }
                }
            }
        }
    }
}
```
