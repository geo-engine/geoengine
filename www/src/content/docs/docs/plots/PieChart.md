---
title: PieChart
---

The `PieChart` is a _plot operator_ that computes a pie chart for a given vector dataset.
Moreover, the operator considers all data in the given query rectangle.

There are multiple variants on how to compute the slices of the pie chart.
In addition, it is possible to compute a donut chart instead of a standard pie chart.

## Errors

The operator returns an error in the following cases.

- The `attribute` for the given `columnName` does not exist.
- The number of slices is too large: If the number of slices is greater than `32`, the operator returns an error.

## Notes

If the attribute has a [`Measurement`](/docs/datatypes/measurement) of type `Classification`, the operator uses the class name instead of the raw value.

## Parameters

| Name       | Type    | Description                                      | Examples |
| ---------- | ------- | ------------------------------------------------ | -------- |
| type       | string  |                                                  |          |
| columnName | string  | The names of the attribute to generate pies for. | `"name"` |
| donut      | boolean | Whether to render the chart as a donut.          |          |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| vector | VectorOperator | An operator that produces vector data. |

## Examples

```json
{
    "type": "PieChart",
    "params": {
        "type": "count",
        "columnName": "class",
        "donut": false
    },
    "sources": {
        "vector": {
            "type": "OgrSource",
            "params": {
                "data": "land_use"
            }
        }
    }
}
```
