
# LayerCollection


## Properties

Name | Type
------------ | -------------
`id` | [ProviderLayerCollectionId](ProviderLayerCollectionId.md)
`name` | string
`description` | string
`items` | [Array&lt;CollectionItem&gt;](CollectionItem.md)
`entryLabel` | string
`properties` | Array&lt;Array&lt;string&gt;&gt;

## Example

```typescript
import type { LayerCollection } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "id": null,
  "name": null,
  "description": null,
  "items": null,
  "entryLabel": null,
  "properties": null,
} satisfies LayerCollection

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as LayerCollection
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


