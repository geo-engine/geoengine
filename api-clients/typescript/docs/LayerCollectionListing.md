
# LayerCollectionListing


## Properties

Name | Type
------------ | -------------
`description` | string
`id` | [ProviderLayerCollectionId](ProviderLayerCollectionId.md)
`name` | string
`properties` | Array&lt;Array&lt;string&gt;&gt;
`type` | string

## Example

```typescript
import type { LayerCollectionListing } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "description": null,
  "id": null,
  "name": null,
  "properties": null,
  "type": null,
} satisfies LayerCollectionListing

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as LayerCollectionListing
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


