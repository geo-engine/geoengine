---
title: MeanRasterPixelValuesOverTime
---

The `MeanRasterPixelValuesOverTime` is a _plot operator_ that computes a time series plot of mean raster values.

For each time step in the raster time series, it computes one mean value per band.
The output is a plot in Vega-Lite specification.
For a single band, it is an area or line plot.
For multiple bands, it is a line plot with one line per band.

For instance, you want to plot the mean temperature of a monthly raster time series.
Then, you can use this operator to generate a time series plot.

## Errors

The operator returns an error if one of the selected bands (`attributes`) does not exist.

## Notes

If the selected bands have different [`Measurement`](/docs/datatypes/measurement)s, the y-axis has no unit.

## Parameters

| Name         | Type                                  | Description                                                                                                                        | Examples                             |
| ------------ | ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------ |
| timePosition | MeanRasterPixelValuesOverTimePosition | Where should the x-axis (time) tick be positioned? At either time start, time end or in the center.                                | `"start"`                            |
| area         | boolean                               | Whether to fill the area under the curve. Defaults to `true`.<br>Only applies to a single band, multiple bands are drawn as lines. | `false`<br>`true`                    |
| attributes   | AttributeSelection                    | _Optional_: The bands to compute the means for.<br>Defaults to `all`.                                                              | `{"type":"names","values":["ndvi"]}` |

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
