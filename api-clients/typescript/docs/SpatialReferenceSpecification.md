
# SpatialReferenceSpecification

The specification of a spatial reference, where extent and axis labels are given in natural order (x, y) = (east, north)

## Properties

Name | Type
------------ | -------------
`name` | string
`spatialReference` | string
`projString` | string
`extent` | [BoundingBox2D](BoundingBox2D.md)
`axisLabels` | Array&lt;string&gt;
`axisOrder` | [AxisOrder](AxisOrder.md)

## Example

```typescript
import type { SpatialReferenceSpecification } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "name": null,
  "spatialReference": null,
  "projString": null,
  "extent": null,
  "axisLabels": null,
  "axisOrder": null,
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


