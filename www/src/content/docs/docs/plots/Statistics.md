---
title: Statistics
---

The `Statistics` operator is a _plot operator_ that computes count statistics over

- a selection of numerical columns of a single vector dataset, or
- multiple raster datasets.

The output is a JSON description.

For instance, you want to get an overview of a raster data source.
Then, you can use this operator to get basic count statistics.

## Vector Data

In the case of vector data, the operator generates one statistic for each of the selected numerical attributes.
The operator returns an error if one of the selected attributes is not numeric.

## Raster Data

For raster data, the operator generates one statistic for each input raster.

## Inputs

The operator consumes exactly one _vector_ or multiple _raster_ operators.

| Parameter | Type                                 |
| --------- | ------------------------------------ |
| `source`  | `MultipleRasterOrSingleVectorSource` |

## Errors

The operator returns an error in the following cases.

- Vector data: The `attribute` for one of the given `columnNames` is not numeric.
- Vector data: The `attribute` for one of the given `columnNames` does not exist.
- Raster data: The length of the `columnNames` parameter does not match the number of input rasters.

### Example Output

```json
{
    "A": {
        "valueCount": 6,
        "validCount": 6,
        "min": 1.0,
        "max": 6.0,
        "mean": 3.5,
        "stddev": 1.707,
        "percentiles": [
            {
                "percentile": 0.25,
                "value": 2.0
            },
            {
                "percentile": 0.5,
                "value": 3.5
            },
            {
                "percentile": 0.75,
                "value": 5.0
            }
        ]
    }
}
```

## Parameters

| Name        | Type  | Description                                                                                                                                                                                                                                                                                                                                                                      | Examples |
| ----------- | ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| columnNames | array | # Vector data<br>The names of the attributes to generate statistics for.<br><br># Raster data<br>_Optional_: An alias for each input source.<br>The operator will automatically name the rasters `Raster-1`, `Raster-2`, … if this parameter is empty.<br>If aliases are given, the number of aliases must match the number of input rasters.<br>Otherwise an error is returned. |          |
| percentiles | array | The percentiles to compute for each attribute.                                                                                                                                                                                                                                                                                                                                   |          |

## Sources

| Name   | Type                                 | Description                                                         |
| ------ | ------------------------------------ | ------------------------------------------------------------------- |
| source | MultipleRasterOrSingleVectorOperator | It is either a set of `RasterOperator` or a single `VectorOperator` |

## Examples

```json
{
    "type": "Statistics",
    "params": {
        "columnNames": ["A"],
        "percentiles": [0.25, 0.5, 0.75]
    },
    "sources": {
        "source": [
            {
                "type": "GdalSource",
                "params": {
                    "data": "ndvi"
                }
            }
        ]
    }
}
```
