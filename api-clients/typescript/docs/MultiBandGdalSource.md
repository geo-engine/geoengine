
# MultiBandGdalSource

The [`MultiBandGdalSource`] is a source operator that reads multi-band raster data using GDAL.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [GdalSourceParameters](GdalSourceParameters.md)

## Example

```typescript
import type { MultiBandGdalSource } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
} satisfies MultiBandGdalSource

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MultiBandGdalSource
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


