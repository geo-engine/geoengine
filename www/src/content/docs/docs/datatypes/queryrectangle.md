---
title: QueryRectangle
---

A query rectangle defines a multi-dimensional spatial query in Geo Engine.
It consists of three parts:

- a two-dimensional spatial bounds (and extent plus its spatial reference system),
- a time interval,
- a spatial resolution.

The spatial bounds behave differently for raster, vector, or plot queries.
For raster queries, the spatial bounds define a spatial partition.
This means the lower right corner of the spatial bounds is not included in the query.
For vector queries, the spatial bounds define a bounding box, i.e., a rectangle where all bounds are included.
Plot queries behave like vector queries.

# Example JSON

```json
{
    "spatial_bounds": {
        "upper_left_coordinate": {
            "x": 10.0,
            "y": 20.0
        },
        "lower_right_coordinate": {
            "x": 70.0,
            "y": 80.0
        }
    },
    "time_interval": {
        "start": "2010-01-01T00:00:00Z",
        "end": "2011-01-01T00:00:00Z"
    },
    "spatial_resolution": {
        "x": 1.0,
        "y": 1.0
    }
}
```
