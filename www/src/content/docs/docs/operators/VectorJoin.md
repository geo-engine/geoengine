---
title: Vector Join
---

The `VectorJoin` operator allows combining multiple vector inputs into a single feature collection.
There are multiple join variants defined, which are described below.

For instance, you want to join tabular data to a point collection of buildings.
The point collection contains the geolocation of the buildings and their id.
The attribute data collection has the building id and the height information.
Combining the two feature collections leads to a single point collection with geolocation and height information.

## Errors

If the value in the `left` parameter is not a column of the left feature collection, an error is thrown.

If the value in the `right` parameter is not a column of the right feature collection, an error is thrown.

### `EquiGeoToData`

If the left input is not a geo data collection, an error is thrown.

If the right input is not a (non-geo) data collection, an error is thrown.

## Parameters

| Name              | Type        | Description                                                      | Examples                |
| ----------------- | ----------- | ---------------------------------------------------------------- | ----------------------- |
| type              | string      |                                                                  |                         |
| leftColumn        | string      | Column name of the left input used in the join.                  | `"id"`                  |
| rightColumn       | string      | Column name of the right input used in the join.                 | `"id"`                  |
| rightColumnSuffix | string,null | Optional suffix added to right-side columns to avoid collisions. | `"_other"`<br>`"right"` |

## Sources

| Name  | Type           | Description                            |
| ----- | -------------- | -------------------------------------- |
| left  | VectorOperator | An operator that produces vector data. |
| right | VectorOperator | An operator that produces vector data. |

## Examples

```json
{
    "type": "VectorJoin",
    "params": {
        "type": "EquiGeoToData",
        "leftColumn": "id",
        "rightColumn": "id",
        "rightColumnSuffix": "_other"
    },
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
