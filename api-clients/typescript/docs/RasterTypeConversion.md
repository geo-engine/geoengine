
# RasterTypeConversion

The `RasterTypeConversion` operator changes the data type of raster pixels.  Applying this conversion may cause precision loss. For example, converting `F32` value `3.1` to `U8` results in `3`.  If a value is outside of the range of the target data type, it is clipped to the valid range of that type. For example, converting `F32` value `300.0` to `U8` results in `255`.  ## Inputs  The `RasterTypeConversion` operator expects exactly one _raster_ input.

## Properties

Name | Type
------------ | -------------
`params` | [RasterTypeConversionParameters](RasterTypeConversionParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)
`type` | string

## Example

```typescript
import type { RasterTypeConversion } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "params": null,
  "sources": null,
  "type": null,
} satisfies RasterTypeConversion

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RasterTypeConversion
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


