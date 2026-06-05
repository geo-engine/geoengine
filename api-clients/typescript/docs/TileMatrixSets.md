
# TileMatrixSets

A definition of a tile matrix set following the Tile Matrix Set standard. For tileset metadata, such a description (in `tileMatrixSet` property) is only required for offline use, as an alternative to a link with a `http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme` relation type.

## Properties

Name | Type
------------ | -------------
`tileMatrixSets` | [Array&lt;TileMatrixSetItem&gt;](TileMatrixSetItem.md)

## Example

```typescript
import type { TileMatrixSets } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "tileMatrixSets": null,
} satisfies TileMatrixSets

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TileMatrixSets
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


