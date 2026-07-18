
# STRectangle


## Properties

Name | Type
------------ | -------------
`spatialReference` | string
`boundingBox` | [BoundingBox2D](BoundingBox2D.md)
`timeInterval` | [TimeInterval](TimeInterval.md)

## Example

```typescript
import type { STRectangle } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "spatialReference": null,
  "boundingBox": null,
  "timeInterval": null,
} satisfies STRectangle

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as STRectangle
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


