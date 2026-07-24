
# VectorExpressionParameters

Parameters for the `VectorExpression` operator.

## Properties

Name | Type
------------ | -------------
`inputColumns` | Array&lt;string&gt;
`expression` | string
`outputColumn` | [OutputColumn](OutputColumn.md)
`geometryColumnName` | string
`outputMeasurement` | [Measurement](Measurement.md)

## Example

```typescript
import type { VectorExpressionParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "inputColumns": null,
  "expression": null,
  "outputColumn": null,
  "geometryColumnName": null,
  "outputMeasurement": null,
} satisfies VectorExpressionParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as VectorExpressionParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


