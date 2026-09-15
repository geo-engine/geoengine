---
title: Raster Scaling
---

The raster scaling operator scales/unscales the values of a raster by a given slope factor and offset.
This allows to shrink and expand the value range of the pixel values needed to store a raster.
It also allows to shift values to all-positive values and back.
We use the [GDAL](https://gdal.org/index.html) terms of [scale](https://gdal.org/programs/gdal_translate.html#cmdoption-gdal_translate-scale) and [unscale](https://gdal.org/programs/gdal_translate.html#cmdoption-gdal_translate-unscale).
Raster data is often scaled to reduce memory/storage consumption.
To get the "real" raster values the unscale operation is applied.
Keep in mind that scaling might reduce the precision of the pixel values.
(To actually reduce the size of the raster, use the [raster type conversion operator](/docs/operators/rastertypeconversion) and transform to a smaller datatype after scaling.)

The operator applies the following formulas to every pixel.

For _unscaling_ the formula is: `p_new = p_old * slope + offset`. The key for this mode is `mulSlopeAddOffset`.

For _scaling_ the formula is: `p_new = (p_old - offset) / slope`. The key for this mode is `subOffsetDivSlope`.

`p_old` and `p_new` refer to the old and new pixel value. The slope and offset values are either properties attached to the input raster or a fixed value.

An example for Meteosat Second Generation properties is:

- offset: `msg.calibration_offset`
- slope: `msg.calibration_slope`

\*if no `outputMeasurement` is given, the measurement of the input raster is used.

## Parameters

| Name              | Type                 | Description                                                    | Examples                                                                                               |
| ----------------- | -------------------- | -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| slope             | SlopeOffsetSelection | The slope (multiplication factor) applied to each pixel value. | `{"domain":"","key":"scale"}`<br>`{"value":0.1}`<br>`{"type":"metadataKey","domain":"","key":"scale"}` |
| offset            | SlopeOffsetSelection | The offset (addition) applied to each pixel value.             | `{"domain":"","key":"scale"}`<br>`{"value":0.1}`<br>`{"type":"constant","value":1}`                    |
| outputMeasurement | null or Measurement  |                                                                | `null`                                                                                                 |
| scalingMode       | ScalingMode          | Selects whether the values are scaled or unscaled.             | `"mulSlopeAddOffset"`                                                                                  |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "RasterScaling",
    "params": {
        "slope": {
            "type": "metadataKey",
            "domain": "",
            "key": "scale"
        },
        "offset": {
            "type": "constant",
            "value": 1
        },
        "outputMeasurement": null,
        "scalingMode": "mulSlopeAddOffset"
    },
    "sources": {
        "raster": {
            "type": "GdalSource",
            "params": {
                "data": "modis-b6"
            }
        }
    }
}
```
