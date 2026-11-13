
# RemoveTileOverlap

The `RemoveTileOverlap` operator crops the overlap halo from all tiles of its input raster.  It is the inverse of `AddTileOverlap`: each tile shrinks symmetrically while its core region and georeference stay untouched. Removing all overlap restores plain tiles that every operator accepts. Use it after ML segmentation to crop model output back to cores.  ## Inputs  The `RemoveTileOverlap` operator expects exactly one _raster_ input.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [RemoveTileOverlapParameters](RemoveTileOverlapParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)

## Example

```typescript
import type { RemoveTileOverlap } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies RemoveTileOverlap

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RemoveTileOverlap
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


