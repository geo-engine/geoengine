---
title: Histogram
---

The `Histogram` is a _plot operator_ that computes a histogram plot either over attributes of a vector dataset or values of a raster source.
The output is a plot in [Vega-Lite](https://vega.github.io/vega-lite/) specification.

For instance, you want to plot the data distribution of numeric attributes of a feature collection.
Then you can use a histogram with a suitable number of buckets to visualize and assess this.

## Errors

The operator returns an error if the selected column (`columnName`) does not exist or is not numeric.

## Notes

If `bounds` or `buckets` are not defined, the operator will determine these values by itself which requires processing the data twice.

If the `buckets` parameter is set to `squareRootChoiceRule`, the operator estimates it using the square root of the number of elements in the data.

## Parameters

| Name        | Type             | Description                                                                                                                  | Examples        |
| ----------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------- | --------------- |
| columnName  | string           | Name of the (numeric) vector attribute or raster band to compute the histogram on.                                           | `"temperature"` |
| bounds      | HistogramBounds  | If `data`, it computes the bounds of the underlying data.<br>If `{ "min": ..., "max": ... }`, one can specify custom bounds. |                 |
| buckets     | HistogramBuckets | The number of buckets. The value can be specified or calculated.                                                             |                 |
| interactive | boolean          | Flag, if the histogram should have user interactions for a range selection. It is `false` by default.                        | `true`          |

## Sources

| Name   | Type                         | Description                                                         |
| ------ | ---------------------------- | ------------------------------------------------------------------- |
| vector | SingleRasterOrVectorOperator | It is either a set of `RasterOperator` or a single `VectorOperator` |

## Examples

```json
{
    "type": "Histogram",
    "params": {
        "columnName": "foobar",
        "bounds": {
            "min": 5,
            "max": 10
        },
        "buckets": {
            "type": "number",
            "value": 15
        },
        "interactive": false
    },
    "sources": {
        "vector": {
            "type": "OgrSource",
            "params": {
                "data": "ndvi"
            }
        }
    }
}
```
