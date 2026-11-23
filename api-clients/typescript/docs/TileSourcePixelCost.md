
# TileSourcePixelCost

Estimated source-file pixel coverage of one OGC tile at this matrix resolution.

## Properties

Name | Type
------------ | -------------
`id` | string
`cellSize` | number
`tileWidth` | number
`tileHeight` | number
`matrixWidth` | number
`matrixHeight` | number
`sourcePixels` | number
`pointOfOrigin` | Array&lt;number&gt;

## Example

```typescript
import type { TileSourcePixelCost } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "id": null,
  "cellSize": null,
  "tileWidth": null,
  "tileHeight": null,
  "matrixWidth": null,
  "matrixHeight": null,
  "sourcePixels": null,
  "pointOfOrigin": null,
} satisfies TileSourcePixelCost

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TileSourcePixelCost
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


