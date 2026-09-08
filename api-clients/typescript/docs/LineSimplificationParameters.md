
# LineSimplificationParameters


## Properties

Name | Type
------------ | -------------
`algorithm` | [LineSimplificationAlgorithm](LineSimplificationAlgorithm.md)
`epsilon` | number

## Example

```typescript
import type { LineSimplificationParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "algorithm": null,
  "epsilon": null,
} satisfies LineSimplificationParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as LineSimplificationParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


