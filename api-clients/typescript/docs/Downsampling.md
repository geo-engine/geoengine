
# Downsampling

The `Downsampling` operator decreases raster resolution by sampling values of an input raster.  If queried with a resolution that is finer than the input resolution, downsampling is not applicable and an error is returned.  ## Inputs  The `Downsampling` operator expects exactly one _raster_ input.  ## Resolution  The target resolution can be specified either as an explicit `Resolution` (in pixel units) or as a `Fraction` that scales the input resolution.  ```rust,ignore // Scale the input resolution by a factor of 2 in both x and y directions DownsamplingResolution::Fraction(Fraction { x: 2.0, y: 2.0 }) ```  ```rust,ignore // Use an explicit resolution of 200×200 pixel units DownsamplingResolution::Resolution(SpatialResolution { x: 200.0, y: 200.0 }) ```

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [DownsamplingParameters](DownsamplingParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)

## Example

```typescript
import type { Downsampling } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies Downsampling

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Downsampling
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


