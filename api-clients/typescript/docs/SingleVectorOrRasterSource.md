
# SingleVectorOrRasterSource

A single vector or raster operator as source for this operator, keyed as \"vector\" in JSON.

## Properties

Name | Type
------------ | -------------
`vector` | [SingleRasterOrVectorOperator](SingleRasterOrVectorOperator.md)

## Example

```typescript
import type { SingleVectorOrRasterSource } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "vector": null,
} satisfies SingleVectorOrRasterSource

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as SingleVectorOrRasterSource
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


