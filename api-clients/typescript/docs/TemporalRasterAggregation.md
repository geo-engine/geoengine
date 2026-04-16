
# TemporalRasterAggregation

The `TemporalRasterAggregation` operator aggregates a raster time series into uniform time windows. The output starts with the first window that contains the query start and contains all windows that overlap the query interval.  Pixel values are computed by aggregating all input rasters that contribute to the current window.  ## Inputs  The `TemporalRasterAggregation` operator expects exactly one _raster_ input.  ## Errors  If the aggregation method is `first`, `last`, or `mean` and the input raster has no NO DATA value, an error is returned.

## Properties

Name | Type
------------ | -------------
`params` | [TemporalRasterAggregationParameters](TemporalRasterAggregationParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)
`type` | string

## Example

```typescript
import type { TemporalRasterAggregation } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "params": null,
  "sources": null,
  "type": null,
} satisfies TemporalRasterAggregation

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TemporalRasterAggregation
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


