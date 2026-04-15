
# ProvenanceEntry


## Properties

Name | Type
------------ | -------------
`data` | [Array&lt;DataId&gt;](DataId.md)
`provenance` | [Provenance](Provenance.md)

## Example

```typescript
import type { ProvenanceEntry } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "data": null,
  "provenance": null,
} satisfies ProvenanceEntry

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ProvenanceEntry
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


