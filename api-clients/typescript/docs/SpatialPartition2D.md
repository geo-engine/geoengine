
# SpatialPartition2D

A partition of space that include the upper left but excludes the lower right coordinate

## Properties

Name | Type
------------ | -------------
`lowerRightCoordinate` | [Coordinate2D](Coordinate2D.md)
`upperLeftCoordinate` | [Coordinate2D](Coordinate2D.md)

## Example

```typescript
import type { SpatialPartition2D } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "lowerRightCoordinate": null,
  "upperLeftCoordinate": null,
} satisfies SpatialPartition2D

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as SpatialPartition2D
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


