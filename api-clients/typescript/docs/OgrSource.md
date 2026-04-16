
# OgrSource

The [`OgrSource`] is a source operator that reads vector data using OGR (part of GDAL). The counterpart for raster data is the [`GdalSource`].  ## Errors  If the given dataset does not exist or is not readable, an error is thrown. 

## Properties

Name | Type
------------ | -------------
`params` | [OgrSourceParameters](OgrSourceParameters.md)
`type` | string

## Example

```typescript
import type { OgrSource } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "params": null,
  "type": null,
} satisfies OgrSource

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as OgrSource
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


