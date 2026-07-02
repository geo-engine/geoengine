
# TileMatrix

A tile matrix, usually corresponding to a particular zoom level of a TileMatrixSet.

## Properties

Name | Type
------------ | -------------
`id` | string
`title` | string
`description` | string
`keywords` | Array&lt;string&gt;
`scaleDenominator` | number
`cellSize` | number
`cornerOfOrigin` | [CornerOfOrigin](CornerOfOrigin.md)
`pointOfOrigin` | Array&lt;number&gt;
`tileWidth` | number
`tileHeight` | number
`matrixWidth` | number
`matrixHeight` | number
`variableMatrixWidths` | [Array&lt;VariableMatrixWidth&gt;](VariableMatrixWidth.md)

## Example

```typescript
import type { TileMatrix } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "id": null,
  "title": null,
  "description": null,
  "keywords": null,
  "scaleDenominator": null,
  "cellSize": null,
  "cornerOfOrigin": null,
  "pointOfOrigin": null,
  "tileWidth": null,
  "tileHeight": null,
  "matrixWidth": null,
  "matrixHeight": null,
  "variableMatrixWidths": null,
} satisfies TileMatrix

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TileMatrix
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


