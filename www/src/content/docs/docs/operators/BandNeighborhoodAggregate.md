---
title: Band Neighborhood Aggregate
---

The `BandNeighborhoodAggregate` operator performs a pixel-wise aggregate function over neighboring bands.
The output is a raster time series with the same number of bands as the input raster.
The pixel values are replaced by the result of the aggregate function.
This allows, for example, the computation of a moving average over the bands of a raster time series.

## Types

The following describes the types used in the parameters.

### NeighborhoodAggregate

There are several types of neighborhood aggregate functions.

#### Average

This aggregate function computes the average of the neighboring bands.
The `windowSize` parameter defines the number of bands to consider for the average and must be an odd number.
For the borders, the window is reduced to the available bands.

#### `FirstDerivative`

This aggregate function computes an approximation of the first derivative of the neighboring bands using the central difference method.
To compute the distance between neighboring bands, a `bandDistance` parameter is required.

## Errors

The operation fails if there are not enough bands in the input raster to compute the aggregate function or the number of bands does not match the requirements of the aggregate function.

## Parameters

| Name      | Type                            | Description                                                                                                                                                                                                                          | Examples |
| --------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------- |
| aggregate | BandNeighborhoodAggregateMethod | The aggregation method to apply to neighboring bands.<br><br>The average method computes the mean over the current band window, while the<br>first-derivative method approximates the derivative using the configured band distance. |          |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "BandNeighborhoodAggregate",
    "params": {
        "aggregate": {
            "type": "average",
            "windowSize": 3
        }
    },
    "sources": {
        "raster": {
            "type": "GdalSource",
            "params": {
                "data": "sentinel-2"
            }
        }
    }
}
```
