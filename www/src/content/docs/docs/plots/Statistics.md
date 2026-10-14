---
title: Statistics
---

The `Statistics` operator is a _plot operator_ that computes count statistics over

- a selection of numerical columns of a single vector dataset, or
- a selection of bands of a single raster dataset.

The output is a Vega-Lite table with one row per attribute or band.
The raw statistics of each row are in the `data.values` of the Vega-Lite spec in `vegaSpec`,
with the attribute or band in `name`.

For instance, you want to get an overview of a raster data source.
Then, you can use this operator to get basic count statistics.

## Vector Data

In the case of vector data, the operator generates one statistic for each of the selected numerical attributes.
The operator returns an error if one of the selected attributes is not numeric.

## Raster Data

For raster data, the operator generates one statistic for each of the selected bands.
It only considers the pixels that intersect the query rectangle.

## Errors

The operator returns an error in the following cases.

- Vector data: One of the selected `attributes` is not numeric.
- Vector data: One of the selected `attributes` does not exist.
- Raster data: One of the selected `attributes` (bands) does not exist.

### Example Output

The `data.values` of the Vega-Lite spec:

```json
[
    {
        "name": "ndvi",
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
]
```

## Parameters

| Name        | Type               | Description                                                                                                                                                                                           | Examples                             |
| ----------- | ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------ |
| attributes  | AttributeSelection | _Optional_: The attributes to generate statistics for.<br>For vector data, these are numeric columns, for raster data, these are bands.<br>Defaults to `all`, i.e., all numeric columns or all bands. | `{"type":"names","values":["ndvi"]}` |
| percentiles | array              | The percentiles to compute for each attribute.                                                                                                                                                        | `[0.25,0.5,0.75]`                    |

## Sources

| Name   | Type                         | Description                                                         |
| ------ | ---------------------------- | ------------------------------------------------------------------- |
| source | SingleRasterOrVectorOperator | It is either a single `RasterOperator` or a single `VectorOperator` |

## Examples

```json
{
    "type": "Statistics",
    "params": {
        "attributes": {
            "type": "names",
            "values": ["ndvi"]
        },
        "percentiles": [0.25, 0.5, 0.75]
    },
    "sources": {
        "source": {
            "type": "GdalSource",
            "params": {
                "data": "ndvi"
            }
        }
    }
}
```
