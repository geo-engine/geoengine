
# PointInPolygonFilter

The `PointInPolygonFilter` operator filters point features of a (multi-)point collection with polygons. In more detail, the points of each feature are checked against the polygons of the other collection. If one or more point is included in any polygon\'s ring, the feature is included in the output.  For instance, you can filter tree features inside the polygons of a forest. All features, that weren\'t inside any forest polygon, are considered either part of another forest or outliers and are thus removed.  ## Errors  If the `points` vector input is not a (multi-)point feature collection, an error is thrown.  If the `polygons` vector input is not a (multi-)polygon feature collection, an error is thrown. 

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | object
`sources` | [PointInPolygonFilterSource](PointInPolygonFilterSource.md)

## Example

```typescript
import type { PointInPolygonFilter } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies PointInPolygonFilter

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as PointInPolygonFilter
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


