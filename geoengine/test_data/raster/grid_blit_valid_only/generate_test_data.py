#!/usr/bin/env python3
"""
Generate test data for the grid_blit_valid_only integration test.

Creates two 4x4 GeoTIFF files with complementary valid/no-data patterns,
and one expected-result GeoTIFF after combining them with grid_blit_valid_only.

File A (left.tif):
    Columns 0,1 = value 100 (valid)
    Columns 2,3 = value 0   (no-data, nodata=0)

File B (right.tif):
    Columns 0,1 = value 0   (no-data, nodata=0)
    Columns 2,3 = value 200 (valid)

Expected output (expected.tif):
    Columns 0,1 = value 100
    Columns 2,3 = value 200
    All pixels valid (no nodata)
"""

import os
import numpy as np
from osgeo import gdal, osr

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

WIDTH = 4
HEIGHT = 4

# Geo transform: origin (0, 4), pixel size (1, -1)
# This gives pixel (0,0) at geo (0,4), pixel (3,3) at geo (3,1)
GT = (0.0, 1.0, 0.0, 4.0, 0.0, -1.0)

# WGS84 projection
srs = osr.SpatialReference()
srs.ImportFromEPSG(4326)
proj = srs.ExportToWkt()


def create_geotiff(filename, data, nodata_value=None):
    """Create a GeoTIFF with the given data array."""
    filepath = os.path.join(OUT_DIR, filename)
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(
        filepath, WIDTH, HEIGHT, 1, gdal.GDT_UInt16, options=["COMPRESS=DEFLATE"]
    )
    ds.SetGeoTransform(GT)
    ds.SetProjection(proj)
    band = ds.GetRasterBand(1)
    if nodata_value is not None:
        band.SetNoDataValue(nodata_value)
    band.WriteArray(data)
    band.FlushCache()
    ds.FlushCache()
    ds = None
    print(f"Created {filepath}")


# File A: left half valid (100), right half nodata (0)
data_a = np.array(
    [
        [100, 100, 0, 0],
        [100, 100, 0, 0],
        [100, 100, 0, 0],
        [100, 100, 0, 0],
    ],
    dtype=np.uint16,
)
create_geotiff("left.tif", data_a, nodata_value=0.0)

# File B: left half nodata (0), right half valid (200)
data_b = np.array(
    [
        [0, 0, 200, 200],
        [0, 0, 200, 200],
        [0, 0, 200, 200],
        [0, 0, 200, 200],
    ],
    dtype=np.uint16,
)
create_geotiff("right.tif", data_b, nodata_value=0.0)

# Expected: all pixels valid, cols 0,1 = 100, cols 2,3 = 200
data_expected = np.array(
    [
        [100, 100, 200, 200],
        [100, 100, 200, 200],
        [100, 100, 200, 200],
        [100, 100, 200, 200],
    ],
    dtype=np.uint16,
)
create_geotiff("expected.tif", data_expected, nodata_value=None)

print("Done.")
