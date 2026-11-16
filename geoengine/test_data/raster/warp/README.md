# Warp reference data

GDAL's results for the resampling methods of the `Warp` operator, to compare with in the operator's tests.

## Inputs

Both inputs are 60 × 60 pixels of 0.1° in EPSG:4326, from 0° to 6° E and 50° to 56° N (Benelux and the North Sea), without overviews:

- `ndvi.tif`: `../modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF`, continuous, with water (255) as no-data
- `landcover.tif`: `../landcover/landcover.tif`, classes, with water (0) as no-data and the spatial reference set to EPSG:4326 (the source uses the Clarke 1866 ellipsoid)

## Expected results

`expected/<input>_<method>_<case>_<data type>.tif` is the result of `gdal raster update -r <method> --et 0` (GDAL 3.12) on an empty target grid.
The target grid is fixed beforehand, as `Warp`'s output grid, and `--et 0` transforms every pixel exactly.
Each `Byte` result with `average` or `bilinear` has a `Float32` counterpart: where the exact result lies halfway between two integers, GDAL's rounding depends on the rounding errors of its coordinates, so the tests accept both neighbours there.

The cases:

- `3x`: 0.3°, three input pixels per output pixel
- `2_5x`: 0.25°, partly covered input pixels
- `2x_shifted`: 0.2° with the origin moved by half an input pixel
- `mixed`: 0.3° wide and 0.05° high
- `2x_finer`, `2_5x_finer_shifted`: 0.05° and 0.04°, on a part of the input; the latter is shifted so that no output pixel center lies on an input pixel edge, where the pixel GDAL takes as the one under the center depends on rounding errors

## Commands

Run in this directory:

```sh
gdal raster clip --bbox 0,50,6,56 ../modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF ndvi.tif --overwrite
gdal raster edit --nodata 255 ndvi.tif
gdal raster clip --bbox 0,50,6,56 ../landcover/landcover.tif landcover.tif --overwrite
gdal raster edit --nodata 0 --crs EPSG:4326 landcover.tif

mkdir -p expected
warp() { # input method case size bbox data_type nodata
    target="expected/$1_$2_$3_$6.tif"
    gdal raster create --size "$4" --bbox "$5" --crs EPSG:4326 --ot "$6" --nodata "$7" --burn "$7" --overwrite "$target"
    gdal raster update -r "$2" --et 0 "$1.tif" "$target"
}
warp ndvi average 3x 20,20 0,50,6,56 Byte 255
warp ndvi average 3x 20,20 0,50,6,56 Float32 -9999
warp ndvi average 2_5x 24,24 0,50,6,56 Float32 -9999
warp ndvi average 2x_shifted 29,29 0.05,50.05,5.85,55.85 Byte 255
warp ndvi average 2x_shifted 29,29 0.05,50.05,5.85,55.85 Float32 -9999
warp ndvi average mixed 20,120 0,50,6,56 Byte 255
warp ndvi average mixed 20,120 0,50,6,56 Float32 -9999
warp ndvi min 2_5x 24,24 0,50,6,56 Byte 255
warp ndvi max 3x 20,20 0,50,6,56 Byte 255
warp ndvi bilinear 2x_finer 40,40 1,51,3,53 Byte 255
warp ndvi bilinear 2x_finer 40,40 1,51,3,53 Float32 -9999
warp ndvi bilinear 2_5x_finer_shifted 49,49 1.01,51.03,2.97,52.99 Float32 -9999
warp landcover mode 3x 20,20 0,50,6,56 Byte 0
warp landcover mode 2_5x 24,24 0,50,6,56 Byte 0
warp landcover mode 2x_shifted 29,29 0.05,50.05,5.85,55.85 Byte 0
warp landcover mode mixed 20,120 0,50,6,56 Byte 0
rm -f ./*.aux.xml expected/*.aux.xml
```
