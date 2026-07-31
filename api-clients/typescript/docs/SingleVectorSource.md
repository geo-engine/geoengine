
# SingleVectorSource

A single vector operator as a source for this operator.

## Properties

Name | Type
------------ | -------------
`vector` | [VectorOperator](VectorOperator.md)

## Example

```typescript
import type { SingleVectorSource } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "vector": null,
} satisfies SingleVectorSource

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as SingleVectorSource
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


