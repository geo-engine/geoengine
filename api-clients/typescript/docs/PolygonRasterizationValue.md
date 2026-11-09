
# PolygonRasterizationValue

The value that a polygon burns into the pixels it covers.

## Properties

Name | Type
------------ | -------------
`type` | string
`value` | number
`column` | string
`aggregation` | [PolygonRasterizationAggregation](PolygonRasterizationAggregation.md)
`ignoreNoData` | boolean

## Example

```typescript
import type { PolygonRasterizationValue } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "value": null,
  "column": null,
  "aggregation": null,
  "ignoreNoData": null,
} satisfies PolygonRasterizationValue

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as PolygonRasterizationValue
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


