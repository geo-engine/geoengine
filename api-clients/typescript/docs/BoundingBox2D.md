
# BoundingBox2D

A bounding box that includes all border points. Note: may degenerate to a point!

## Properties

Name | Type
------------ | -------------
`lowerLeftCoordinate` | [Coordinate2D](Coordinate2D.md)
`upperRightCoordinate` | [Coordinate2D](Coordinate2D.md)

## Example

```typescript
import type { BoundingBox2D } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "lowerLeftCoordinate": null,
  "upperRightCoordinate": null,
} satisfies BoundingBox2D

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as BoundingBox2D
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


