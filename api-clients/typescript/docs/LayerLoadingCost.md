
# LayerLoadingCost

Estimates of source-file pixels and native STAC searches for tiled layer loading. Limits are application policy and are deliberately not part of the model.

## Properties

Name | Type
------------ | -------------
`tileMatrixSet` | string
`crs` | string
`matrices` | [Array&lt;TileSourcePixelCost&gt;](TileSourcePixelCost.md)
`searchGrids` | [Array&lt;StacSearchGrid&gt;](StacSearchGrid.md)

## Example

```typescript
import type { LayerLoadingCost } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "tileMatrixSet": null,
  "crs": null,
  "matrices": null,
  "searchGrids": null,
} satisfies LayerLoadingCost

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as LayerLoadingCost
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


