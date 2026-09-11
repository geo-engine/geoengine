---
title: ScatterPlot
---

The `ScatterPlot` is a _plot operator_ that computes a scatter plot over two attributes of a vector dataset.
Thereby, the operator considers all data in the given query rectangle.

In case of more than `500` points to plot, the representation changes from a regular scatter plot
to a 2D Histogram with buckets determined from the underlying data.

## Errors

The operator returns an error if one of the selected columns does not exist or is not numeric.

## Notes

If your dataset contains `infinite` or `NAN` values, they are ignored for the computation. Moreover, if
your dataset contains more than `10.000` values, the buckets of the histogram are generated based on
those `10.000` values. Later values outside those bounds are ignored.

## Parameters

| Name    | Type   | Description                                                 | Examples        |
| ------- | ------ | ----------------------------------------------------------- | --------------- |
| columnX | string | The name of the attribute making up the x-axis of the plot. | `"temperature"` |
| columnY | string | The name of the attribute making up the y-axis of the plot. | `"humidity"`    |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| vector | VectorOperator | An operator that produces vector data. |

## Examples

```json
{
    "type": "ScatterPlot",
    "params": {
        "columnX": "temperature",
        "columnY": "humidity"
    },
    "sources": {
        "vector": {
            "type": "OgrSource",
            "params": {
                "data": "stations"
            }
        }
    }
}
```
