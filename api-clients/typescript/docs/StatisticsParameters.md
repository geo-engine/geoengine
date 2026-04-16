
# StatisticsParameters

The parameter spec for `Statistics`

## Properties

Name | Type
------------ | -------------
`columnNames` | Array&lt;string&gt;
`percentiles` | Array&lt;number&gt;

## Example

```typescript
import type { StatisticsParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "columnNames": null,
  "percentiles": null,
} satisfies StatisticsParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as StatisticsParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


