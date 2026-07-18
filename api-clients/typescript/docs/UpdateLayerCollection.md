
# UpdateLayerCollection


## Properties

Name | Type
------------ | -------------
`name` | string
`description` | string
`properties` | Array&lt;Array&lt;string&gt;&gt;

## Example

```typescript
import type { UpdateLayerCollection } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "name": Example Collection,
  "description": A description for an example collection,
  "properties": null,
} satisfies UpdateLayerCollection

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as UpdateLayerCollection
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


