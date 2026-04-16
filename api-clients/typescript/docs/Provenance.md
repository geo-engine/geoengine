
# Provenance


## Properties

Name | Type
------------ | -------------
`citation` | string
`license` | string
`uri` | string

## Example

```typescript
import type { Provenance } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "citation": null,
  "license": null,
  "uri": null,
} satisfies Provenance

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Provenance
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


