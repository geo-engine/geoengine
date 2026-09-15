---
title: FeatureAttributeValuesOverTime
---

The `FeatureAttributeValuesOverTime` is a _plot operator_ that computes a multi-line plot for feature attribute values over time.
For distinguishing features, the data requires an id column.
The output is a plot in Vega-Lite specification.

```mermaid
xychart-beta
    title "Selected Regional Trends"
    x-axis ["Jan 05", "Feb 02", "Mar 02", "Mar 30"]
    y-axis "Value" 0 --> 250
    line "Papenburg" [118, 201, 198, 225]
    line "Wismar" [185, 158, 177, 213]
    line "Frederikshavn" [46, 50, 103, 124]
    line "Helsingor" [44, 110, 112, 128]
```

For instance, you want to plot the NDVI values of a feature collection of trees.
Then, you can use a multi-line plot to visualize the trees by their id.

## Errors

The operator returns an error if the selected columns ( `idColumn` and `valueColumn`) do not exist or `valueColumn` is not numeric.

## Notes

The operator processes a maximum of `20` different ids.
After recognizing more than `20` different ids, the operator ignores the rest.

## Parameters

| Name        | Type   | Description                                                | Examples        |
| ----------- | ------ | ---------------------------------------------------------- | --------------- |
| idColumn    | string | The column name of the `id` attribute (one line per `id`.) | `"id"`          |
| valueColumn | string | The column name of the `value` attribute (y-axis values).  | `"temperature"` |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| vector | VectorOperator | An operator that produces vector data. |

## Examples

```json
{
    "type": "FeatureAttributeValuesOverTime",
    "params": {
        "idColumn": "id",
        "valueColumn": "temperature"
    },
    "sources": {
        "vector": {
            "type": "OgrSource",
            "params": {
                "data": "temperatures"
            }
        }
    }
}
```
