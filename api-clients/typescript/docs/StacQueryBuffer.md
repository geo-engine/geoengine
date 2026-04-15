
# StacQueryBuffer

A struct that represents buffers to apply to stac requests

## Properties

Name | Type
------------ | -------------
`endSeconds` | number
`startSeconds` | number

## Example

```typescript
import type { StacQueryBuffer } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "endSeconds": null,
  "startSeconds": null,
} satisfies StacQueryBuffer

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as StacQueryBuffer
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


