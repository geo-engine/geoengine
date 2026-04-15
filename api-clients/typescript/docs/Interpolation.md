
# Interpolation

The `Interpolation` operator increases raster resolution by interpolating values of an input raster.  If queried with a resolution that is coarser than the input resolution, interpolation is not applicable and an error is returned.  ## Inputs  The `Interpolation` operator expects exactly one _raster_ input.

## Properties

Name | Type
------------ | -------------
`params` | [InterpolationParameters](InterpolationParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)
`type` | string

## Example

```typescript
import type { Interpolation } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "params": null,
  "sources": null,
  "type": null,
} satisfies Interpolation

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Interpolation
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


