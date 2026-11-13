
# AddTileOverlap

The `AddTileOverlap` operator equips every output tile with an overlap (halo) around its core region.  For each tile, neighboring data is fetched so that convolutions or other neighborhood computations have input beyond the tile boundary. Regions beyond the dataset extent are no-data. Coverage and queries remain defined by the tile cores.  ## Inputs  The `AddTileOverlap` operator expects exactly one _raster_ input without overlap.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [AddTileOverlapParameters](AddTileOverlapParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)

## Example

```typescript
import type { AddTileOverlap } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies AddTileOverlap

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as AddTileOverlap
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


