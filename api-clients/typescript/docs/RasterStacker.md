
# RasterStacker

The `RasterStacker` stacks all of its inputs into a single raster time series. It queries all inputs and combines them by band, space, and then time.  The output raster has as many bands as the sum of all input bands. Tiles are automatically temporally aligned.  All inputs must have the same data type and spatial reference.  ## Inputs  The `RasterStacker` operator expects multiple raster inputs.

## Properties

Name | Type
------------ | -------------
`params` | [RasterStackerParameters](RasterStackerParameters.md)
`sources` | [MultipleRasterSources](MultipleRasterSources.md)
`type` | string

## Example

```typescript
import type { RasterStacker } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "params": null,
  "sources": null,
  "type": null,
} satisfies RasterStacker

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RasterStacker
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


