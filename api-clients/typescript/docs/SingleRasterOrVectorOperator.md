
# SingleRasterOrVectorOperator

It is either a set of `RasterOperator` or a single `VectorOperator`

## Properties

Name | Type
------------ | -------------
`params` | [ReprojectionParameters](ReprojectionParameters.md)
`sources` | [SingleRasterOrVectorSource](SingleRasterOrVectorSource.md)
`type` | string

## Example

```typescript
import type { SingleRasterOrVectorOperator } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "params": null,
  "sources": null,
  "type": null,
} satisfies SingleRasterOrVectorOperator

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as SingleRasterOrVectorOperator
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


