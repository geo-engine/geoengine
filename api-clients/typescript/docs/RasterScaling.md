
# RasterScaling

The raster scaling operator scales/unscales the values of a raster by a given slope factor and offset. This allows to shrink and expand the value range of the pixel values needed to store a raster. It also allows to shift values to all-positive values and back. We use the [GDAL](https://gdal.org/index.html) terms of [scale](https://gdal.org/programs/gdal_translate.html#cmdoption-gdal_translate-scale) and [unscale](https://gdal.org/programs/gdal_translate.html#cmdoption-gdal_translate-unscale). Raster data is often scaled to reduce memory/storage consumption. To get the \"real\" raster values the unscale operation is applied. Keep in mind that scaling might reduce the precision of the pixel values. (To actually reduce the size of the raster, use the [raster type conversion operator](/docs/operators/rastertypeconversion) and transform to a smaller datatype after scaling.)  The operator applies the following formulas to every pixel.  For _unscaling_ the formula is: `p_new = p_old * slope + offset`. The key for this mode is `mulSlopeAddOffset`.  For _scaling_ the formula is: `p_new = (p_old - offset) / slope`. The key for this mode is `subOffsetDivSlope`.  `p_old` and `p_new` refer to the old and new pixel value. The slope and offset values are either properties attached to the input raster or a fixed value.  An example for Meteosat Second Generation properties is:  - offset: `msg.calibration_offset` - slope: `msg.calibration_slope`  \\*if no `outputMeasurement` is given, the measurement of the input raster is used.  The `RasterScaling` operator expects exactly one _raster_ input.  | Parameter | Type | | --------- | ----- | | `source` | `SingleRasterSource` |  ## Types  The following describes the types used in the parameters.  ### `SlopeOffsetSelection`  The `SlopeOffsetSelection` type is used to specify a metadata key or a constant value.  | Value | Description | | ----- | ----------- | | `{\"type\": \"auto\"}` * | Use slope and offset from the tiles properties | | `{\"type\": \"constant\", \"value\": number}` | A constant value. | | `{\"type\": \"metadataKey\", \"domain\": string, \"key\": string}` | A metadata key to lookup dynamic values from raster (tile) properties. |  * if set to `\"auto\"`, the operator will use the values from the dedicated (GDAL) raster properties for scale and offset. 

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [RasterScalingParameters](RasterScalingParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)

## Example

```typescript
import type { RasterScaling } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies RasterScaling

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RasterScaling
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


