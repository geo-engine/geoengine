
# GdalSource

The [`GdalSource`] is a source operator that reads raster data using GDAL. The counterpart for vector data is the [`OgrSource`].  ## Errors  If the given dataset does not exist or is not readable, an error is thrown. 

## Properties

Name | Type
------------ | -------------
`params` | [GdalSourceParameters](GdalSourceParameters.md)
`type` | string

## Example

```typescript
import type { GdalSource } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "params": null,
  "type": null,
} satisfies GdalSource

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GdalSource
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


