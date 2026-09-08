---
title: MeanRasterPixelValuesOverTime
---

The `MeanRasterPixelValuesOverTime` is a _plot operator_ that computes a time series plot of mean raster values.

For each time step in the raster time series, it computes one mean value.
The output is a plot in Vega-Lite specification.

For instance, you want to plot the mean temperature of a monthly raster time series.
Then, you can use this operator to generate a time series plot.

## Parameters

| Name         | Type                                  | Description                                                                                         | Examples |
| ------------ | ------------------------------------- | --------------------------------------------------------------------------------------------------- | -------- |
| timePosition | MeanRasterPixelValuesOverTimePosition | Where should the x-axis (time) tick be positioned? At either time start, time end or in the center. |          |
| area         | boolean                               | Whether to fill the area under the curve. Defaults to `true`.                                       | `false`  |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "MeanRasterPixelValuesOverTime",
    "params": {
        "timePosition": "start",
        "area": true
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
```
