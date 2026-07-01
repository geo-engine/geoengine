
# CollectionItem


## Properties

Name | Type
------------ | -------------
`type` | string
`id` | [ProviderLayerId](ProviderLayerId.md)
`name` | string
`description` | string
`properties` | Array&lt;Array&lt;string&gt;&gt;

## Example

```typescript
import type { CollectionItem } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "id": null,
  "name": null,
  "description": null,
  "properties": null,
} satisfies CollectionItem

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as CollectionItem
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


