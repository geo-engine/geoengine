---
title: Visual Point Clustering
---

The `VisualPointClustering` is a clustering operator for point collections that removes clutter and preserves the spatial structure of the input.
The output is a point collection with a count and radius attribute.
The operator utilizes the input resolution of the query to determine when points, being displayed as circles, would overlap.
Moreover, it allows aggregating non-geo attributes to preserve the other columns of the input.
For more information on the algorithm, cf. the paper [Beilschmidt, C. et al.: A Linear-Time Algorithm for the Aggregation and Visualization of Big Spatial Point Data. SIGSPATIAL/GIS 2017: 73:1-73:4](https://doi.org/10.1145/3139958.3140037).

An exemplary use case for this operator is the visualization of point data in an online map application.
There, you can use this operator as the final step of the workflow to cluster the points and display them as circles.
These circles then pose a decluttered view of the data, e.g., via a WFS endpoint.

## Errors

If the source value `vector` is not a point collection, an error is thrown.

If multiple columns in `columnAggregates` have the same names, an error is thrown.

## Parameters

| Name             | Type   | Description                                                                            | Examples     |
| ---------------- | ------ | -------------------------------------------------------------------------------------- | ------------ |
| minRadiusPx      | number | Minimum circle radius in pixels.                                                       | `8`<br>`10`  |
| deltaPx          | number | Minimum circle-to-circle distance in pixels.                                           | `1`          |
| resolution       | number | Spatial resolution used during clustering.                                             | `0.5`<br>`1` |
| radiusColumn     | string | Column name used to store the cluster radius.                                          | `"__radius"` |
| countColumn      | string | Column name used to store the number of clustered points.                              | `"__count"`  |
| columnAggregates | object | Map of source columns to aggregation definitions used for clustered output attributes. |              |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| vector | VectorOperator | An operator that produces vector data. |

## Examples

```json
{
    "type": "VisualPointClustering",
    "params": {
        "minRadiusPx": 8,
        "deltaPx": 1,
        "radiusColumn": "__radius",
        "countColumn": "__count",
        "columnAggregates": {
            "mean_population": {
                "columnName": "population",
                "aggregateType": "MeanNumber",
                "measurement": {
                    "type": "unitless"
                }
            },
            "sample_names": {
                "columnName": "name",
                "aggregateType": "StringSample"
            }
        }
    },
    "sources": {
        "vector": {
            "type": "OgrSource",
            "params": {
                "data": "places",
                "attributeProjection": ["name", "population"]
            }
        }
    }
}
```
