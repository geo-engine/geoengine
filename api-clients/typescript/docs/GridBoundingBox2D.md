
# GridBoundingBox2D


## Properties

Name | Type
------------ | -------------
`topLeftIdx` | [GridIdx2D](GridIdx2D.md)
`bottomRightIdx` | [GridIdx2D](GridIdx2D.md)

## Example

```typescript
import type { GridBoundingBox2D } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "topLeftIdx": null,
  "bottomRightIdx": null,
} satisfies GridBoundingBox2D

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GridBoundingBox2D
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


