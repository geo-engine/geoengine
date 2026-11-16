
# Warp

The `Warp` operator puts a raster onto another grid in one step: another spatial reference, another resolution and another grid origin, in any combination.  For each output pixel, the operator uses the value of the input pixel under the center of the output pixel. The output resolution can be coarser or finer than the input resolution, also coarser in one axis and finer in the other.  Geo Engine uses `Warp` to answer queries, e.g., WMS requests, in a spatial reference or at a resolution that differs from the processing graph\'s.  ## Errors  The operator returns an error if the input has no spatial reference or if the input data cannot be reprojected to the target spatial reference.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [WarpParameters](WarpParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)

## Example

```typescript
import type { Warp } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies Warp

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Warp
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


