
# StacProviderDataset


## Properties

Name | Type
------------ | -------------
`name` | string
`description` | string
`dataType` | [RasterDataType](RasterDataType.md)
`resolution` | [SpatialResolution](SpatialResolution.md)
`projection` | string
`spatialGrid` | [SpatialGridDescriptor](SpatialGridDescriptor.md)
`bands` | [Array&lt;StacProviderDatasetBand&gt;](StacProviderDatasetBand.md)

## Example

```typescript
import type { StacProviderDataset } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "name": null,
  "description": null,
  "dataType": null,
  "resolution": null,
  "projection": null,
  "spatialGrid": null,
  "bands": null,
} satisfies StacProviderDataset

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as StacProviderDataset
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


