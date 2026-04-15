
# RasterOperator

An operator that produces raster data.

## Properties

Name | Type
------------ | -------------
`params` | [TemporalRasterAggregationParameters](TemporalRasterAggregationParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)
`type` | string

## Example

```typescript
import type { RasterOperator } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "params": null,
  "sources": null,
  "type": null,
} satisfies RasterOperator

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RasterOperator
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


