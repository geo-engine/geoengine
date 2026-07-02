
# DatasetLayerListingProviderDefinition


## Properties

Name | Type
------------ | -------------
`type` | string
`id` | string
`name` | string
`description` | string
`priority` | number
`collections` | [Array&lt;DatasetLayerListingCollection&gt;](DatasetLayerListingCollection.md)

## Example

```typescript
import type { DatasetLayerListingProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "id": null,
  "name": null,
  "description": null,
  "priority": null,
  "collections": null,
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


