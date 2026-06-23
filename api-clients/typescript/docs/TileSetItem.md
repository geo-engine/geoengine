
# TileSetItem

A minimal tileset element for use within a list of tilesets linking to full description of those tilesets.

## Properties

Name | Type
------------ | -------------
`title` | string
`dataType` | [GeospatialDataDataType](GeospatialDataDataType.md)
`crs` | [TilesCrs](TilesCrs.md)
`tileMatrixSetURI` | string
`links` | [Array&lt;Link&gt;](Link.md)

## Example

```typescript
import type { TileSetItem } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "title": null,
  "dataType": null,
  "crs": null,
  "tileMatrixSetURI": null,
  "links": null,
} satisfies TileSetItem

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TileSetItem
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


