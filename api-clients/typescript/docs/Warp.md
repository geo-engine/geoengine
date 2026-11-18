
# Warp

The `Warp` operator puts a raster onto another grid in one step: another spatial reference, another resolution and another grid origin, in any combination.  The output resolution can be coarser or finer than the input resolution, also coarser in one axis and finer in the other. By default, each output pixel takes the value of the input pixel under its center. `downsampling` and `upsampling` choose other methods, with the semantics of [`gdalwarp -r`](https://gdal.org/en/stable/programs/gdalwarp.html#cmdoption-gdalwarp-r): `downsampling` applies to outputs that are coarser than the input in at least one axis, `upsampling` to outputs that are finer in both axes. With another spatial reference, coarser and finer are relative to the resolution of the reprojected input, and the footprint of an output pixel is the bounding box of its projected corners, as in GDAL.  Integer outputs of `average` and `bilinear` round half up. `outputDataType` keeps the fractions, e.g. `F32` for the average of `U8` values.  Geo Engine uses `Warp` to answer queries, e.g., WMS requests, in a spatial reference or at a resolution that differs from the processing graph\'s.  ## Errors  The operator returns an error if the input has no spatial reference, if the input data cannot be reprojected to the target spatial reference, or if `mode` is used with a float input or could need more memory than the limit `[query_context] warp_mode_max_memory_per_tile_mb`.

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


