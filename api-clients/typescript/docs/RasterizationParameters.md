
# RasterizationParameters


## Properties

Name | Type
------------ | -------------
`spatialResolution` | [SpatialResolution](SpatialResolution.md)
`originCoordinate` | [Coordinate2D](Coordinate2D.md)
`densityParams` | [DensityParams](DensityParams.md)

## Example

```typescript
import type { RasterizationParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "spatialResolution": null,
  "originCoordinate": null,
  "densityParams": null,
} satisfies RasterizationParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RasterizationParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


