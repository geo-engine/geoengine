
# PolygonRasterizationValueAttribute

Each pixel gets the value of a numeric column, aggregated over all polygons that cover it.

## Properties

Name | Type
------------ | -------------
`type` | string
`column` | string
`aggregation` | [PolygonRasterizationAggregation](PolygonRasterizationAggregation.md)
`ignoreNoData` | boolean

## Example

```typescript
import type { PolygonRasterizationValueAttribute } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "column": null,
  "aggregation": null,
  "ignoreNoData": null,
} satisfies PolygonRasterizationValueAttribute

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as PolygonRasterizationValueAttribute
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


