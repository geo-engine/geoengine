---
title: Reprojection
---

The `Reprojection` operator reprojects data from one spatial reference system to another.
It accepts exactly one input which can either be a raster or a vector data stream.
The operator produces all data that, after reprojection, is contained in the query rectangle.

## Data Type Specifics

The concrete behavior depends on the data type.

### Vector Data

The operator reprojects all coordinates of the features individually.
The result contains all features that, after reprojection, are intersected by the query rectangle.

### Raster Data

To create tiles in the target projection, the operator loads corresponding tiles in the source projection.
For each output pixel, the value of the nearest input pixel is used.

If parts of a tile are outside of the source extent after projection, the operator produces NO DATA values.

## Inputs

The `Reprojection` operator expects exactly one _raster_ or _vector_ input.

## Errors

The operator returns an error if the target projection is unknown or if input data cannot be reprojected.

## Parameters

| Name                   | Type                       | Description                                                                                                                                                                                             | Examples       |
| ---------------------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| targetSpatialReference | string                     | Target spatial reference system.                                                                                                                                                                        | `"EPSG:32632"` |
| deriveOutSpec          | DeriveOutRasterSpecsSource | Controls how raster output bounds are derived.<br><br>The default `projectionBounds` usually keeps a projection-aligned target grid,<br>while `dataBounds` derives it directly from source data bounds. |                |

## Sources

| Name   | Type                         | Description                                                         |
| ------ | ---------------------------- | ------------------------------------------------------------------- |
| source | SingleRasterOrVectorOperator | It is either a set of `RasterOperator` or a single `VectorOperator` |

## Examples

```json
{
    "type": "Reprojection",
    "params": {
        "deriveOutSpec": "projectionBounds",
        "targetSpatialReference": "EPSG:32632"
    },
    "sources": {
        "source": {
            "type": "MockPointSource",
            "params": {
                "points": [
                    {
                        "x": 8.77069,
                        "y": 50.80904
                    }
                ],
                "spatialBounds": {
                    "type": "none"
                }
            }
        }
    }
}
```
