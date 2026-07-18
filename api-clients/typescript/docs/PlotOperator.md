
# PlotOperator

An operator that produces plot data.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [StatisticsParameters](StatisticsParameters.md)
`sources` | [MultipleRasterOrSingleVectorSource](MultipleRasterOrSingleVectorSource.md)

## Example

```typescript
import type { PlotOperator } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies PlotOperator

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as PlotOperator
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


