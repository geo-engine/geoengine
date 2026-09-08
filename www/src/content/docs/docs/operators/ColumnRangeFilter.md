---
title: Column Range Filter
---

The `ColumnRangeFilter` operator allows filtering `FeatureCollection`s.
Users can define one or more data ranges for a column in the data table that is then filtered.
The filter can be used for numerical as well as textual columns.
Each range is inclusive, i.e. `[start, end]` includes both the `start` and the `end` values.

## Errors

If the value in the `column` parameter is not a column of the feature collection, an error is thrown.

## Parameters

| Name      | Type    | Description                                                                                                                        | Examples                                      |
| --------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| column    | string  | Column name to filter on.                                                                                                          | `"temperature"`<br>`"population"`<br>`"name"` |
| ranges    | array   | One or more inclusive ranges for the selected column values.<br>Numeric ranges use `[start, end]`; string ranges use `["a", "k"]`. |                                               |
| keepNulls | boolean | Whether rows whose column value is NULL should be retained.                                                                        | `true`<br>`false`                             |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| vector | VectorOperator | An operator that produces vector data. |

## Examples

```json
{
    "type": "ColumnRangeFilter",
    "params": {
        "column": "temperature",
        "ranges": [
            {
                "type": "float",
                "start": 0,
                "end": 50
            }
        ],
        "keepNulls": true
    },
    "sources": {
        "vector": {
            "type": "OgrSource",
            "params": {
                "data": "example"
            }
        }
    }
}
```
