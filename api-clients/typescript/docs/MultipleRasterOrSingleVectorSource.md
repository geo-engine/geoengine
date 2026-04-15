
# MultipleRasterOrSingleVectorSource

Either one or more raster operators or a single vector operator as source for this operator.

## Properties

Name | Type
------------ | -------------
`source` | [MultipleRasterOrSingleVectorOperator](MultipleRasterOrSingleVectorOperator.md)

## Example

```typescript
import type { MultipleRasterOrSingleVectorSource } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "source": null,
} satisfies MultipleRasterOrSingleVectorSource

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MultipleRasterOrSingleVectorSource
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


