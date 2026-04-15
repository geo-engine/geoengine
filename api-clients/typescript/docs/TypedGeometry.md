
# TypedGeometry


## Properties

Name | Type
------------ | -------------
`data` | any
`multiPoint` | [MultiPoint](MultiPoint.md)
`multiLineString` | [MultiLineString](MultiLineString.md)
`multiPolygon` | [MultiPolygon](MultiPolygon.md)

## Example

```typescript
import type { TypedGeometry } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "data": null,
  "multiPoint": null,
  "multiLineString": null,
  "multiPolygon": null,
} satisfies TypedGeometry

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TypedGeometry
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


