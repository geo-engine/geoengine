
# TileMatrixSetItem

A minimal tile matrix set element for use within a list of tile matrix sets linking to a full definition.

## Properties

Name | Type
------------ | -------------
`id` | [TileMatrixSetId](TileMatrixSetId.md)
`title` | string
`uri` | string
`crs` | [TilesCrs](TilesCrs.md)
`links` | [Array&lt;Link&gt;](Link.md)

## Example

```typescript
import type { TileMatrixSetItem } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "id": null,
  "title": null,
  "uri": null,
  "crs": null,
  "links": null,
} satisfies TileMatrixSetItem

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TileMatrixSetItem
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


