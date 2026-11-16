---
title: Warp
---

The `Warp` operator puts a raster onto another grid in one step: another spatial reference,
another resolution and another grid origin, in any combination.

The output resolution can be coarser or finer than the input resolution, also coarser in one
axis and finer in the other. By default, each output pixel takes the value of the input pixel
under its center. `downsampling` and `upsampling` choose other methods, with the semantics of
[`gdalwarp -r`](https://gdal.org/en/stable/programs/gdalwarp.html#cmdoption-gdalwarp-r):
`downsampling` applies to outputs that are coarser than the input in at least one axis,
`upsampling` to outputs that are finer in both axes. With another spatial reference, coarser
and finer are relative to the resolution of the reprojected input, and the footprint of an
output pixel is the bounding box of its projected corners, as in GDAL.

Integer outputs of `average` and `bilinear` round half up. `outputDataType` keeps the fractions,
e.g. `F32` for the average of `U8` values.

Geo Engine uses `Warp` to answer queries, e.g., WMS requests, in a spatial reference or at a
resolution that differs from the processing graph's.

## Errors

The operator returns an error if the input has no spatial reference or if the input data
cannot be reprojected to the target spatial reference.

## Parameters

| Name                   | Type                      | Description                                                                                                   | Examples            |
| ---------------------- | ------------------------- | ------------------------------------------------------------------------------------------------------------- | ------------------- |
| targetSpatialReference | string,null               | Target spatial reference system. Defaults to the spatial reference of the input.                              | `"EPSG:3857"`       |
| outputResolution       | SpatialResolution or null |                                                                                                               | `{"x":0.5,"y":0.5}` |
| outputOriginReference  | Coordinate2D or null      |                                                                                                               |                     |
| downsampling           | WarpDownsamplingMethod    | How to compute an output pixel that is coarser than the input in at least one axis.<br>Defaults to `nearest`. | `"average"`         |
| upsampling             | WarpUpsamplingMethod      | How to compute an output pixel that is finer than the input in both axes.<br>Defaults to `nearest`.           | `"bilinear"`        |
| outputDataType         | RasterDataType or null    |                                                                                                               | `"F32"`             |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "Warp",
    "params": {
        "targetSpatialReference": "EPSG:3857",
        "outputResolution": {
            "x": 20000,
            "y": 10000
        }
    },
    "sources": {
        "raster": {
            "type": "GdalSource",
            "params": {
                "data": "ndvi"
            }
        }
    }
}
```

```json
{
    "type": "Warp",
    "params": {
        "outputResolution": {
            "x": 0.5,
            "y": 0.5
        },
        "downsampling": "average",
        "upsampling": "bilinear",
        "outputDataType": "F32"
    },
    "sources": {
        "raster": {
            "type": "GdalSource",
            "params": {
                "data": "ndvi"
            }
        }
    }
}
```
