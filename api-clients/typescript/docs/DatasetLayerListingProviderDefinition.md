
# DatasetLayerListingProviderDefinition


## Properties

Name | Type
------------ | -------------
`collections` | [Array&lt;DatasetLayerListingCollection&gt;](DatasetLayerListingCollection.md)
`description` | string
`id` | string
`name` | string
`priority` | number
`type` | string

## Example

```typescript
import type { DatasetLayerListingProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "collections": null,
  "description": null,
  "id": null,
  "name": null,
  "priority": null,
  "type": null,
} satisfies DatasetLayerListingProviderDefinition

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as DatasetLayerListingProviderDefinition
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


