
# MultiPolygon


## Properties

Name | Type
------------ | -------------
`polygons` | Array&lt;Array&lt;Array&lt;Coordinate2D&gt;&gt;&gt;

## Example

```typescript
import type { MultiPolygon } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "polygons": null,
} satisfies MultiPolygon

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MultiPolygon
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


