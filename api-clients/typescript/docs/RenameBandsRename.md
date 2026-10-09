
# RenameBandsRename

A new name for each band, to be used instead of the original band names.

## Properties

Name | Type
------------ | -------------
`type` | string
`values` | Array&lt;string&gt;

## Example

```typescript
import type { RenameBandsRename } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "values": null,
} satisfies RenameBandsRename

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RenameBandsRename
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


