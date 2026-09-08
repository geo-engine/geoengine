---
title: Point In Polygon Filter
---

The `PointInPolygonFilter` operator filters point features of a (multi-)point collection with polygons.
In more detail, the points of each feature are checked against the polygons of the other collection.
If one or more point is included in any polygon's ring, the feature is included in the output.

For instance, you can filter tree features inside the polygons of a forest.
All features, that weren't inside any forest polygon, are considered either part of another forest or outliers and are thus removed.

## Errors

If the `points` vector input is not a (multi-)point feature collection, an error is thrown.

If the `polygons` vector input is not a (multi-)polygon feature collection, an error is thrown.

## Parameters

No parameters.

## Sources

| Name     | Type           | Description                                                      |
| -------- | -------------- | ---------------------------------------------------------------- |
| points   | VectorOperator | Point collection that is checked against the polygon collection. |
| polygons | VectorOperator | Polygon collection used as the filter mask.                      |

## Examples

```json
{
    "type": "PointInPolygonFilter",
    "params": {},
    "sources": {
        "points": {
            "type": "OgrSource",
            "params": {
                "data": "places",
                "attributeProjection": ["name", "population"]
            }
        },
        "polygons": {
            "type": "OgrSource",
            "params": {
                "data": "germany_outline"
            }
        }
    }
}
```
