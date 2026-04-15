
# TypedOperator

Operator outputs are distinguished by their data type. There are `raster`, `vector` and `plot` operators.

## Properties

Name | Type
------------ | -------------
`operator` | [PlotOperator](PlotOperator.md)
`type` | string

## Example

```typescript
import type { TypedOperator } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "operator": null,
  "type": null,
} satisfies TypedOperator

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TypedOperator
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


