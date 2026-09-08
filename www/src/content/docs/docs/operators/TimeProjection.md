---
title: Time Projection
---

The `TimeProjection` projects vector dataset timestamps to new granularities and ranges.
The output is a new vector dataset with the same geometry and attributes as the input.
However, each time step is projected to a new time range.
Moreover, the [`QueryRectangle`'s](/docs/datatypes/queryrectangle) temporal extent is enlarged as well to include the projected time range.

An example usage scenario is to transform snapshot observations into yearly time slices.
For instance, animal occurrences are observed at a daily granularity.
If you want to aggregate the data to a yearly granularity, you can use the `TimeProjection` operator.
This will change the validity of each element in the dataset to the full year where it was observed.
This is, for instance, useful when you want to combine it with raster time series and use different temporal semantics than the originally recorded validities.

## Errors

If the `step` is negative, an error is thrown.

## Parameters

| Name          | Type                 | Description                                        | Examples |
| ------------- | -------------------- | -------------------------------------------------- | -------- |
| step          | TimeStep             | Time granularity and size for the projection step. |          |
| stepReference | null or TimeInstance |                                                    |          |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| vector | VectorOperator | An operator that produces vector data. |

## Examples

```json
{
    "type": "TimeProjection",
    "params": {
        "step": {
            "granularity": "years",
            "step": 1
        }
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
