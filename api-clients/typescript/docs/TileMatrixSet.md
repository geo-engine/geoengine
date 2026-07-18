
# TileMatrixSet

A definition of a tile matrix set following the Tile Matrix Set standard. For tileset metadata, such a description (in `tileMatrixSet` property) is only required for offline use, as an alternative to a link with a `http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme` relation type.

## Properties

Name | Type
------------ | -------------
`id` | [TileMatrixSetId](TileMatrixSetId.md)
`title` | string
`description` | string
`keywords` | Array&lt;string&gt;
`uri` | string
`crs` | [TilesCrs](TilesCrs.md)
`orderedAxes` | Array&lt;string&gt;
`wellKnownScaleSet` | string
`boundingBox` | [BoundingBox2D](BoundingBox2D.md)
`tileMatrices` | [Array&lt;TileMatrix&gt;](TileMatrix.md)

## Example

```typescript
import type { TileMatrixSet } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "id": null,
  "title": null,
  "description": null,
  "keywords": null,
  "uri": null,
  "crs": null,
  "orderedAxes": null,
  "wellKnownScaleSet": null,
  "boundingBox": null,
  "tileMatrices": null,
} satisfies TileMatrixSet

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TileMatrixSet
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


