---
title: BoxPlot
---

The `BoxPlot` is a _plot operator_ that computes a box plot over

- a selection of numerical columns of a single vector dataset, or
- multiple raster datasets.

Thereby, the operator considers all data in the given query rectangle.

The boxes of the plot span the 1st and 3rd quartile and highlight the median.
The whiskers indicate the minimum and maximum values of the corresponding attribute or raster.

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

## Notes

If your dataset contains `infinite` or `NAN` values, they are ignored for the computation.
Moreover, if your dataset contains more than `10.000`values (which is likely for rasters),
the median and quartiles are estimated using the P^2 algorithm described in:

R. Jain and I. Chlamtac, The P^2 algorithm for dynamic calculation of quantiles and
histograms without storing observations, Communications of the ACM,
Volume 28 (October), Number 10, 1985, p. 1076-1085.
<https://www.cse.wustl.edu/~jain/papers/ftp/psqr.pdf>

## Parameters

| Name        | Type  | Description                                                                                                                                                                                                                                                                                                                                                                   | Examples |
| ----------- | ----- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| columnNames | array | ## Vector Data<br>The names of the attributes to generate boxes for.<br><br>## Raster Data<br>_Optional_: An alias for each input source.<br>The operator will automatically name the boxes `Raster-1`, `Raster-2`, ... if this parameter is empty.<br>If aliases are given, the number of aliases must match the number of input rasters.<br>Otherwise an error is returned. |          |

## Sources

| Name   | Type                                 | Description                                                         |
| ------ | ------------------------------------ | ------------------------------------------------------------------- |
| source | MultipleRasterOrSingleVectorOperator | It is either a set of `RasterOperator` or a single `VectorOperator` |

## Examples

```json
{
    "type": "BoxPlot",
    "params": {
        "columnNames": ["x", "y"]
    },
    "sources": {
        "source": {
            "type": "OgrSource",
            "params": {
                "data": "ndvi"
            }
        }
    }
}
```

```json
{
    "type": "BoxPlot",
    "params": {
        "columnNames": ["A", "B"]
    },
    "sources": {
        "source": [
            {
                "type": "GdalSource",
                "params": {
                    "data": "ndvi"
                }
            },
            {
                "type": "GdalSource",
                "params": {
                    "data": "temperature"
                }
            }
        ]
    }
}
```
