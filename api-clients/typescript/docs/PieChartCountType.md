
# PieChartCountType


## Properties

Name | Type
------------ | -------------
`type` | string
`columnName` | string
`donut` | boolean

## Example

```typescript
import type { PieChartCountType } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "columnName": name,
  "donut": null,
} satisfies PieChartCountType

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as PieChartCountType
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


