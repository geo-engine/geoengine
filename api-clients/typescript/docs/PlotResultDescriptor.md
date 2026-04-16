
# PlotResultDescriptor

A `ResultDescriptor` for plot queries

## Properties

Name | Type
------------ | -------------
`bbox` | [BoundingBox2D](BoundingBox2D.md)
`spatialReference` | string
`time` | [TimeInterval](TimeInterval.md)

## Example

```typescript
import type { PlotResultDescriptor } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "bbox": null,
  "spatialReference": null,
  "time": null,
} satisfies PlotResultDescriptor

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as PlotResultDescriptor
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


