---
title: Neighborhood Aggregate
---

The `NeighborhoodAggregate` operator computes an aggregate function for a pixel and its neighborhood.
The operator can be defined as a neighborhood matrix with either weights or predefined shapes and an aggregate function.
For each time step in the raster time series, the operator computes the aggregate for each pixel and its neighborhood.

## Types

There are several types of neighborhoods. They define a matrix of weights. The rows and columns of this matrix must be odd.

### `WeightsMatrix`

The weights matrix is defined as an n × m matrix of floating-point values. It is applied to the pixel and its neighborhood to serve as the input for the aggregate function.

### Rectangle

The rectangle neighborhood is defined by its shape n × m. The result is a weights matrix with all weights set to `1.0`.

### `AggregateFunction`

The aggregate function computes a single value from a set of values. Supported functions are `sum` and `standardDeviation`.

## Errors

If the neighborhood rows or columns are not positive or odd, an error is thrown.

## Parameters

| Name              | Type                        | Description                                                                                                              | Examples |
| ----------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------ | -------- |
| neighborhood      | NeighborhoodKernel          | Neighborhood definition applied around each pixel.<br>This can be a rectangular neighborhood or a custom weights matrix. |          |
| aggregateFunction | NeighborhoodAggregateMethod | Aggregate function applied across the neighborhood values.                                                               |          |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "NeighborhoodAggregate",
    "params": {
        "neighborhood": {
            "type": "rectangle",
            "dimensions": [3, 3]
        },
        "aggregateFunction": "sum"
    },
    "sources": {
        "raster": {
            "type": "GdalSource",
            "params": {
                "data": "example"
            }
        }
    }
}
```
