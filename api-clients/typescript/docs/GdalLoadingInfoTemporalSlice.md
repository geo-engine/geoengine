
# GdalLoadingInfoTemporalSlice

one temporal slice of the dataset that requires reading from exactly one Gdal dataset

## Properties

Name | Type
------------ | -------------
`time` | [TimeInterval](TimeInterval.md)
`params` | [GdalDatasetParameters](GdalDatasetParameters.md)
`cacheTtl` | number

## Example

```typescript
import type { GdalLoadingInfoTemporalSlice } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "time": null,
  "params": null,
  "cacheTtl": null,
} satisfies GdalLoadingInfoTemporalSlice

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GdalLoadingInfoTemporalSlice
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


