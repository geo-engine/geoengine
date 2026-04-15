
# SpatialReferenceSpecification

The specification of a spatial reference, where extent and axis labels are given in natural order (x, y) = (east, north)

## Properties

Name | Type
------------ | -------------
`axisLabels` | Array&lt;string&gt;
`axisOrder` | [AxisOrder](AxisOrder.md)
`extent` | [BoundingBox2D](BoundingBox2D.md)
`name` | string
`projString` | string
`spatialReference` | string

## Example

```typescript
import type { SpatialReferenceSpecification } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "axisLabels": null,
  "axisOrder": null,
  "extent": null,
  "name": null,
  "projString": null,
  "spatialReference": null,
} satisfies SpatialReferenceSpecification

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as SpatialReferenceSpecification
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


