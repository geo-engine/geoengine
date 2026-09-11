
# PointInPolygonFilterSource


## Properties

Name | Type
------------ | -------------
`points` | [VectorOperator](VectorOperator.md)
`polygons` | [VectorOperator](VectorOperator.md)

## Example

```typescript
import type { PointInPolygonFilterSource } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "points": null,
  "polygons": null,
} satisfies PointInPolygonFilterSource

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as PointInPolygonFilterSource
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


