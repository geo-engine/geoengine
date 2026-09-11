---
title: Line Simplification
---

The `LineSimplification` operator allows simplifying `FeatureCollection`s of (multi-)lines or (multi-)polygons by removing vertices.
Users can select a simplification algorithm and specify an `epsilon` for parametrization.
Alternatively, they can omit the `epsilon`, which results in the epsilon being automatically determined by the query's spatial resolution.

## Errors

- If `epsilon` is set but <= 0, an error is thrown.
- If the input is not a `MultiPolygon` or `MultiLineString`, an error is thrown.

## Parameters

| Name      | Type                        | Description                                                      | Examples     |
| --------- | --------------------------- | ---------------------------------------------------------------- | ------------ |
| algorithm | LineSimplificationAlgorithm | Simplification algorithm used to reduce the geometry complexity. |              |
| epsilon   | number                      | Distance threshold for the simplification algorithm.             | `0.5`<br>`1` |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| vector | VectorOperator | An operator that produces vector data. |

## Examples

```json
{
    "type": "LineSimplification",
    "params": {
        "algorithm": "douglasPeucker",
        "epsilon": 0.5
    },
    "sources": {
        "vector": {
            "type": "OgrSource",
            "params": {
                "data": "roads"
            }
        }
    }
}
```
