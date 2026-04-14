---
title: Mock Point Source
---

The [`MockPointSource`] is a source operator that provides mock vector point data for testing and development purposes.

## Parameters

| Name          | Type                | Description                                                                          | Examples |
| ------------- | ------------------- | ------------------------------------------------------------------------------------ | -------- |
| points        | array               | Points to be output by the mock point source.<br>                                    |          |
| spatialBounds | SpatialBoundsDerive | Defines how the spatial bounds of the source are derived.<br><br>Defaults to `None`. |          |

## Examples

```json
{
    "type": "MockPointSource",
    "params": {
        "points": [
            {
                "x": 1,
                "y": 2
            },
            {
                "x": 3,
                "y": 4
            }
        ],
        "spatialBounds": {
            "type": "derive"
        }
    }
}
```
