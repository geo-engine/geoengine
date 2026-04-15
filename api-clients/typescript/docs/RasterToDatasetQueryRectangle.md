
# RasterToDatasetQueryRectangle

A spatio-temporal rectangle with a specified resolution

## Properties

Name | Type
------------ | -------------
`spatialBounds` | [SpatialPartition2D](SpatialPartition2D.md)
`timeInterval` | [TimeInterval](TimeInterval.md)

## Example

```typescript
import type { RasterToDatasetQueryRectangle } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "spatialBounds": null,
  "timeInterval": null,
} satisfies RasterToDatasetQueryRectangle

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RasterToDatasetQueryRectangle
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


