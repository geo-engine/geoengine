
# TileSet

A resource describing a tileset based on the OGC TileSet Metadata Standard. At least one of the \'TileMatrixSet\',  or a link with \'rel\' tiling-scheme\"

## Properties

Name | Type
------------ | -------------
`title` | string
`description` | string
`keywords` | Array&lt;string&gt;
`dataType` | [GeospatialDataDataType](GeospatialDataDataType.md)
`tileMatrixSetURI` | string
`tileMatrixSetLimits` | [Array&lt;TileMatrixLimits&gt;](TileMatrixLimits.md)
`crs` | [TilesCrs](TilesCrs.md)
`epoch` | number
`links` | [Array&lt;Link&gt;](Link.md)
`layers` | [Array&lt;GeospatialData&gt;](GeospatialData.md)
`boundingBox` | [BoundingBox2D](BoundingBox2D.md)
`centerPoint` | [TilePoint](TilePoint.md)
`style` | [Style](Style.md)
`attribution` | string
`license` | string
`accessConstraints` | [AccessConstraints](AccessConstraints.md)
`version` | string
`created` | Date
`updated` | Date
`pointOfContact` | string
`mediaTypes` | Array&lt;string&gt;

## Example

```typescript
import type { TileSet } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "title": null,
  "description": null,
  "keywords": null,
  "dataType": null,
  "tileMatrixSetURI": null,
  "tileMatrixSetLimits": null,
  "crs": null,
  "epoch": null,
  "links": null,
  "layers": null,
  "boundingBox": null,
  "centerPoint": null,
  "style": null,
  "attribution": null,
  "license": null,
  "accessConstraints": null,
  "version": null,
  "created": null,
  "updated": null,
  "pointOfContact": null,
  "mediaTypes": null,
} satisfies TileSet

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TileSet
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


