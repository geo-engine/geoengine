---
title: ClassHistogram
---

The `ClassHistogram` is a _plot operator_ that computes a histogram plot either over categorical attributes of a vector dataset or categorical values of a raster band.
The output is a plot in [Vega-Lite](https://vega.github.io/vega-lite/) specification.

For instance, you want to plot the frequencies of the classes of a categorical attribute of a feature collection.
Then you can use a class histogram to visualize and assess this.

## Errors

The operator returns an error if…

- the selected column or band (`columnName`) does not exist or is not numeric,
- the source is a vector and the property `columnName` is not set, or
- the input [`Measurement`](/docs/datatypes/measurement) is not categorical.

## Notes

The operator only uses values of the categorical [`Measurement`](/docs/datatypes/measurement).
It ignores missing or no-data values and values that are not covered by the [`Measurement`](/docs/datatypes/measurement).

## Parameters

| Name       | Type        | Description                                                                                                                                                           | Examples  |
| ---------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| columnName | string,null | The name of the attribute or raster band making up the x-axis of the histogram.<br>Must be set for vector sources. For raster sources, it defaults to the first band. | `"class"` |

## Sources

| Name   | Type                         | Description                                                         |
| ------ | ---------------------------- | ------------------------------------------------------------------- |
| source | SingleRasterOrVectorOperator | It is either a single `RasterOperator` or a single `VectorOperator` |

## Examples

```json
{
    "type": "ClassHistogram",
    "params": {
        "columnName": "class"
    },
    "sources": {
        "source": {
            "type": "OgrSource",
            "params": {
                "data": "landcover"
            }
        }
    }
}
```
