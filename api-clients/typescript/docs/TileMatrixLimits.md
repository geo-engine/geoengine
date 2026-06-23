
# TileMatrixLimits

A resource describing useful to create an array that describes the limits for a tile set [super::TileMatrixSet] based on the OGC TileSet Metadata Standard

## Properties

Name | Type
------------ | -------------
`tileMatrix` | string
`minTileRow` | number
`maxTileRow` | number
`minTileCol` | number
`maxTileCol` | number

## Example

```typescript
import type { TileMatrixLimits } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "tileMatrix": null,
  "minTileRow": null,
  "maxTileRow": null,
  "minTileCol": null,
  "maxTileCol": null,
} satisfies TileMatrixLimits

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TileMatrixLimits
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


