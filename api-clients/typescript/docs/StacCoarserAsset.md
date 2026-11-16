
# StacCoarserAsset

A coarser asset that holds the same band as a dataset band, e.g. the 60 m file of a band whose dataset has a resolution of 10 m.

## Properties

Name | Type
------------ | -------------
`resolution` | [SpatialResolution](SpatialResolution.md)
`assetBand` | [StacAssetBand](StacAssetBand.md)

## Example

```typescript
import type { StacCoarserAsset } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "resolution": null,
  "assetBand": null,
} satisfies StacCoarserAsset

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as StacCoarserAsset
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


