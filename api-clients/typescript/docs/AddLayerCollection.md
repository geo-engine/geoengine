
# AddLayerCollection


## Properties

Name | Type
------------ | -------------
`description` | string
`name` | string
`properties` | Array&lt;Array&lt;string&gt;&gt;

## Example

```typescript
import type { AddLayerCollection } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "description": A description for an example collection,
  "name": Example Collection,
  "properties": null,
} satisfies AddLayerCollection

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as AddLayerCollection
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


