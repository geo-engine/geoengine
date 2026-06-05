
# TileSetMetadataResponse


## Properties

Name | Type
------------ | -------------
`title` | string
`dataType` | string
`tileMatrixSetId` | string
`links` | [Array&lt;TemplatedTileLink&gt;](TemplatedTileLink.md)

## Example

```typescript
import type { TileSetMetadataResponse } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "title": null,
  "dataType": null,
  "tileMatrixSetId": null,
  "links": null,
} satisfies TileSetMetadataResponse

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TileSetMetadataResponse
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


