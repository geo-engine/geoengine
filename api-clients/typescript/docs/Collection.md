
# Collection

A body of resources that belong or are used together. An aggregate, set, or group of related resources.

## Properties

Name | Type
------------ | -------------
`id` | string
`title` | string
`description` | string
`keywords` | Array&lt;string&gt;
`attribution` | string
`extent` | [Extent](Extent.md)
`itemType` | string
`crs` | Array&lt;string&gt;
`storageCrs` | string
`storageCrsCoordinateEpoch` | number
`links` | [Array&lt;Link&gt;](Link.md)

## Example

```typescript
import type { Collection } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "id": null,
  "title": null,
  "description": null,
  "keywords": null,
  "attribution": null,
  "extent": null,
  "itemType": null,
  "crs": null,
  "storageCrs": null,
  "storageCrsCoordinateEpoch": null,
  "links": null,
} satisfies Collection

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Collection
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


