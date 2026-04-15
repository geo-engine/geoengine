
# GeoJson


## Properties

Name | Type
------------ | -------------
`features` | Array&lt;any&gt;
`type` | [CollectionType](CollectionType.md)

## Example

```typescript
import type { GeoJson } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "features": null,
  "type": null,
} satisfies GeoJson

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GeoJson
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


