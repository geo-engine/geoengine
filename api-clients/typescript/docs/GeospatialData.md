
# GeospatialData


## Properties

Name | Type
------------ | -------------
`id` | string
`title` | string
`description` | string
`keywords` | Array&lt;string&gt;
`dataType` | [GeospatialDataDataType](GeospatialDataDataType.md)
`geometryDimension` | [GeometryDimension](GeometryDimension.md)
`featureType` | string
`attribution` | string
`license` | string
`pointOfContact` | string
`publisher` | string
`theme` | string
`crs` | [TilesCrs](TilesCrs.md)
`epoch` | number
`minScaleDenominator` | number
`maxScaleDenominator` | number
`minCellSize` | number
`maxCellSize` | number
`maxTileMatrix` | string
`minTileMatrix` | string
`boundingBox` | [BoundingBox2D](BoundingBox2D.md)
`created` | Date
`updated` | Date
`style` | [Style](Style.md)
`geoDataClasses` | Array&lt;string&gt;
`propertiesSchema` | any
`links` | [Array&lt;Link&gt;](Link.md)

## Example

```typescript
import type { GeospatialData } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "id": null,
  "title": null,
  "description": null,
  "keywords": null,
  "dataType": null,
  "geometryDimension": null,
  "featureType": null,
  "attribution": null,
  "license": null,
  "pointOfContact": null,
  "publisher": null,
  "theme": null,
  "crs": null,
  "epoch": null,
  "minScaleDenominator": null,
  "maxScaleDenominator": null,
  "minCellSize": null,
  "maxCellSize": null,
  "maxTileMatrix": null,
  "minTileMatrix": null,
  "boundingBox": null,
  "created": null,
  "updated": null,
  "style": null,
  "geoDataClasses": null,
  "propertiesSchema": null,
  "links": null,
} satisfies GeospatialData

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GeospatialData
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


