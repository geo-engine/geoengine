
# MockPointSource

The [`MockPointSource`] is a source operator that provides mock vector point data for testing and development purposes. 

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [MockPointSourceParameters](MockPointSourceParameters.md)

## Example

```typescript
import type { MockPointSource } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
} satisfies MockPointSource

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MockPointSource
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


