
# MdGdalSource

The [`MdGdalSource`] is a source operator that reads multidimensional (netCDF/Zarr) arrays through GDAL, emitting one 2D raster tile per z slice.  ## Errors  If the given dataset does not exist, is not readable, or the requested bands are not part of the dataset, an error is thrown. 

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [MdGdalSourceParameters](MdGdalSourceParameters.md)

## Example

```typescript
import type { MdGdalSource } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
} satisfies MdGdalSource

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MdGdalSource
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


