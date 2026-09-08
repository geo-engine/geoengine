
# ScatterPlotParameters

The parameter spec for `ScatterPlot`.

## Properties

Name | Type
------------ | -------------
`columnX` | string
`columnY` | string

## Example

```typescript
import type { ScatterPlotParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "columnX": temperature,
  "columnY": humidity,
} satisfies ScatterPlotParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ScatterPlotParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


