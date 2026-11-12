
# WarpParameters

Parameters for the `Warp` operator.

## Properties

Name | Type
------------ | -------------
`targetSpatialReference` | string
`outputResolution` | [SpatialResolution](SpatialResolution.md)
`outputOriginReference` | [Coordinate2D](Coordinate2D.md)
`method` | [WarpMethod](WarpMethod.md)

## Example

```typescript
import type { WarpParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "targetSpatialReference": null,
  "outputResolution": null,
  "outputOriginReference": null,
  "method": null,
} satisfies WarpParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as WarpParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


