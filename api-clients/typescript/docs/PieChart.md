
# PieChart

The `PieChart` is a _plot operator_ that computes a pie chart for a given vector dataset. Moreover, the operator considers all data in the given query rectangle.  There are multiple variants on how to compute the slices of the pie chart. In addition, it is possible to compute a donut chart instead of a standard pie chart.  ## Errors  The operator returns an error in the following cases.  - The `attribute` for the given `columnName` does not exist. - The number of slices is too large: If the number of slices is greater than `32`, the operator returns an error.  ## Notes  If the attribute has a [`Measurement`](../datatypes/measurement) of type `Classification`, the operator uses the class name instead of the raw value. 

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [PieChartParameters](PieChartParameters.md)
`sources` | [SingleVectorSource](SingleVectorSource.md)

## Example

```typescript
import type { PieChart } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies PieChart

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as PieChart
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


