# Sparse GeoTIFFs

512×256 pixel GeoTIFFs with 256×256 tiles.
Only the left tile is stored in the files (`SPARSE_OK=TRUE`); the right tile is missing and GDAL fills it on read.
The data is the cropped MODIS NDVI from `../modis_ndvi/cropped/`.

| File                    | No-data handling                            |
| ----------------------- | ------------------------------------------- |
| `sparse_nodata.tif`     | no-data value 0                             |
| `sparse_no_nodata.tif`  | none, so the missing tile reads as valid 0s |
| `sparse_mask.tif`       | internal mask, missing for the right tile   |
| `sparse_dense_mask.tif` | internal mask, all valid and fully stored   |

Created with:

```bash
NDVI=../modis_ndvi/cropped/MOD13A2_M_NDVI_2014-04-01_30x30.tif
gdalwarp -te 8 31.8 59.2 57.4 -tr 0.1 0.1 -dstnodata 0 -co TILED=YES -co SPARSE_OK=TRUE $NDVI sparse_nodata.tif
gdal_translate sparse_nodata.tif sparse_no_nodata.tif -a_nodata none -co TILED=YES -co SPARSE_OK=TRUE
gdal_translate sparse_nodata.tif sparse_mask.tif -a_nodata none -mask 1 -co TILED=YES -co SPARSE_OK=TRUE --config GDAL_TIFF_INTERNAL_MASK YES
gdal_create -of GTiff -outsize 512 256 -bands 1 -ot Byte -burn 255 -a_srs EPSG:4326 -a_ullr 8 57.4 59.2 31.8 all_valid.tif
gdalbuildvrt -separate stack.vrt sparse_no_nodata.tif all_valid.tif
gdal_translate stack.vrt sparse_dense_mask.tif -b 1 -mask 2 -co TILED=YES -co SPARSE_OK=TRUE --config GDAL_TIFF_INTERNAL_MASK YES
rm all_valid.tif stack.vrt
```
