
# Statistics

The `Statistics` operator is a _plot operator_ that computes count statistics over  - a selection of numerical columns of a single vector dataset, or - multiple raster datasets.  The output is a JSON description.  For instance, you want to get an overview of a raster data source. Then, you can use this operator to get basic count statistics.  ## Vector Data  In the case of vector data, the operator generates one statistic for each of the selected numerical attributes. The operator returns an error if one of the selected attributes is not numeric.  ## Raster Data  For raster data, the operator generates one statistic for each input raster.  ## Inputs  The operator consumes exactly one _vector_ or multiple _raster_ operators.  | Parameter | Type                                 | | --------- | ------------------------------------ | | `source`  | `MultipleRasterOrSingleVectorSource` |  ## Errors  The operator returns an error in the following cases.  - Vector data: The `attribute` for one of the given `columnNames` is not numeric. - Vector data: The `attribute` for one of the given `columnNames` does not exist. - Raster data: The length of the `columnNames` parameter does not match the number of input rasters.  ### Example Output  ```json {   \"A\": {     \"valueCount\": 6,     \"validCount\": 6,     \"min\": 1.0,     \"max\": 6.0,     \"mean\": 3.5,     \"stddev\": 1.707,     \"percentiles\": [       {         \"percentile\": 0.25,         \"value\": 2.0       },       {         \"percentile\": 0.5,         \"value\": 3.5       },       {         \"percentile\": 0.75,         \"value\": 5.0       }     ]   } } ``` 

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [StatisticsParameters](StatisticsParameters.md)
`sources` | [MultipleRasterOrSingleVectorSource](MultipleRasterOrSingleVectorSource.md)

## Example

```typescript
import type { Statistics } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies Statistics

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Statistics
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


