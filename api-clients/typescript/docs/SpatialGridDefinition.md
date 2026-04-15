
# SpatialGridDefinition


## Properties

Name | Type
------------ | -------------
`geoTransform` | [GeoTransform](GeoTransform.md)
`gridBounds` | [GridBoundingBox2D](GridBoundingBox2D.md)

## Example

```typescript
import type { SpatialGridDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "geoTransform": null,
  "gridBounds": null,
} satisfies SpatialGridDefinition

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as SpatialGridDefinition
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


