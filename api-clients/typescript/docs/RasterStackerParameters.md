
# RasterStackerParameters

Parameters for the `RasterStacker` operator.

## Properties

Name | Type
------------ | -------------
`renameBands` | [RenameBands](RenameBands.md)

## Example

```typescript
import type { RasterStackerParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "renameBands": null,
} satisfies RasterStackerParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RasterStackerParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


