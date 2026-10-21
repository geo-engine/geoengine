
# ReTile

The `ReTile` operator re-tiles raster data onto a different tile grid without resampling.  Each output pixel maps to exactly one input pixel (identity). The output origin and tile size can be overridden via the parameters; by default the source\'s own geo-transform origin and the tiling specification\'s tile size are used.  ## Inputs  The `ReTile` operator expects exactly one _raster_ input.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [ReTileParameters](ReTileParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)

## Example

```typescript
import type { ReTile } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies ReTile

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ReTile
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


