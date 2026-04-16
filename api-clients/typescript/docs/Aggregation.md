
# Aggregation

Aggregation methods for `TemporalRasterAggregation`.  Available variants are `min`, `max`, `first`, `last`, `mean`, `sum`, `count`, and `percentileEstimate`. Encountering NO DATA makes the aggregation result NO DATA unless `ignoreNoData` is `true`.

## Properties

Name | Type
------------ | -------------
`ignoreNoData` | boolean
`type` | string
`percentile` | number

## Example

```typescript
import type { Aggregation } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "ignoreNoData": null,
  "type": null,
  "percentile": null,
} satisfies Aggregation

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Aggregation
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


