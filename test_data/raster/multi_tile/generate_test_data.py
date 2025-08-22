import os
import numpy as np
from osgeo import gdal, osr
import datetime
import json
import random
import matplotlib.pyplot as plt

# World extent in WGS84
minx, maxx = -180, 180
miny, maxy = -90, 90

# 2x2 tiles
tiles_x, tiles_y = 2, 2

# Tile size in pixels (non-overlapping)
base_tile_width = int(1800 / tiles_x)
base_tile_height = int(900 / tiles_y)

# Overlap fraction
overlap_frac = 0.25

# Adjusted tile size for overlap
tile_width = int(base_tile_width * (1 + overlap_frac))
tile_height = int(base_tile_height * (1 + overlap_frac))

geo_engine_tile_size_px = 512

# Bands
bands = 2

dates = ["2025-01-01", "2025-02-01", "2025-04-01"]

def create_tile_tiff(filename, width, height, data_array, geotransform, projection):
    driver = gdal.GetDriverByName('GTiff')
    ds = driver.Create(
        filename, width, height, 1, gdal.GDT_UInt16,
        options=["COMPRESS=DEFLATE"]
    )
    ds.SetGeoTransform(geotransform)
    ds.SetProjection(projection)
    ds.GetRasterBand(1).WriteArray(data_array)
    ds.FlushCache()
    ds = None


# WGS84 projection
srs = osr.SpatialReference()
srs.ImportFromEPSG(4326)
proj = srs.ExportToWkt()

# Pixel size (deg per pixel)
px_size_x = (maxx - minx) / (base_tile_width * tiles_x)
px_size_y = (miny - maxy) / (base_tile_height * tiles_y)  # negative

# Overlap in pixels
overlap_px_x = int(base_tile_width * overlap_frac)
overlap_px_y = int(base_tile_height * overlap_frac)

loading_info = []
loading_info_rev = []
pixel_values = set()

for date_idx, date in enumerate(dates):
    for band in range(bands):
        # create global raster for each date and band
        global_filename = f"results/z_index/global/{date}_global_b{band}.tif"
        global_width = base_tile_width * tiles_x
        global_height = base_tile_height * tiles_y
        global_gt = (minx, px_size_x, 0, maxy, 0, px_size_y)
        driver = gdal.GetDriverByName('GTiff')

        print(f"Creating global raster: {global_filename} with dimensions {global_width}x{global_height}")
        global_ds = driver.Create(
            global_filename, global_width, global_height, 1, gdal.GDT_UInt16,
            options=["COMPRESS=DEFLATE"]
        )
        global_ds.SetGeoTransform(global_gt)
        global_ds.SetProjection(proj)

        global_rev_filename = f"results/z_index_reversed/global/{date}_global_b{band}.tif"
        global_ds_rev = driver.Create(
            global_rev_filename, global_width, global_height, 1, gdal.GDT_UInt16,
            options=["COMPRESS=DEFLATE"]
        )
        global_ds_rev.SetGeoTransform(global_gt)
        global_ds_rev.SetProjection(proj)

        global_tiles = []

        for i in range(tiles_x):
            for j in range(tiles_y):
                # TODO: compute this like our engine (what is even the difference?)
                # Calculate top-left pixel position in the global raster
                start_px_x = i * (base_tile_width - overlap_px_x)
                start_px_y = j * (base_tile_height - overlap_px_y)

                # Calculate geotransform for this tile
                xmin = minx + start_px_x * px_size_x
                ymax = maxy + start_px_y * px_size_y

                gt = (xmin, px_size_x, 0, ymax, 0, px_size_y)

                value = 10000 + date_idx * 1000 + band * 100 + i * 10 + j
                pixel_values.add(value)
                data = np.full((tile_height, tile_width), value, dtype=np.float32)

                filename = f"data/{date}_tile_x{i}_y{j}_b{band}.tif"                
                create_tile_tiff(filename, tile_width, tile_height, data, gt, proj)

                # Calculate spatial partition
                upper_left_x = xmin
                upper_left_y = ymax
                lower_right_x = xmin + tile_width * px_size_x
                lower_right_y = ymax + tile_height * px_size_y

                # Calculate time in ms since epoch
                def date_to_ms(date_str):
                    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                    dt = dt.replace(tzinfo=datetime.timezone.utc)  # Treat as UTC
                    return int(dt.timestamp() * 1000)

                time_start = date_to_ms(date)

                next_month = (datetime.datetime.strptime(date, "%Y-%m-%d") + datetime.timedelta(days=31)).replace(day=1)
                next_month = next_month.replace(tzinfo=datetime.timezone.utc)  # Treat as UTC
                time_end = int(next_month.timestamp() * 1000)

                meta = {
                    "time": {"start": time_start, "end": time_end},
                    "spatial_partition": {
                        "upperLeftCoordinate": {"x": upper_left_x, "y": upper_left_y},
                        "lowerRightCoordinate": {"x": lower_right_x, "y": lower_right_y}
                    },
                    "band": band,
                    "z_index": i + j,
                    "params": {
                        "filePath": f"test_data/raster/multi_tile/{filename}",
                        "rasterbandChannel": 1,
                        "geoTransform": {
                            "originCoordinate": {"x": xmin, "y": ymax},
                            "xPixelSize": px_size_x,
                            "yPixelSize": px_size_y
                        },
                        "width": tile_width,
                        "height": tile_height,
                        "fileNotFoundHandling": "Error",
                        "noDataValue": 0.0,
                        "propertiesMapping": None,
                        "gdalOpenOptions": None,
                        "gdalConfigOptions": None,
                        "allowAlphabandAsMask": True
                    }
                }

                loading_info.append(meta)

                # copy meta and reverse r_index
                meta_rev = meta.copy()
                meta_rev["z_index"] = 3- meta_rev["z_index"]
                loading_info_rev.append(meta_rev)

                global_tiles.append({"data":  data.astype(np.uint16), "xoff": start_px_x, "yoff": start_px_y})

        for tile in global_tiles:
            global_ds.GetRasterBand(1).WriteArray(
                tile["data"],
                xoff=tile["xoff"],
                yoff=tile["yoff"]
            )
            
        for tile in reversed(global_tiles):
            global_ds_rev.GetRasterBand(1).WriteArray(
                tile["data"],
                xoff=tile["xoff"],
                yoff=tile["yoff"]
            )


        global_ds.FlushCache()
        global_ds = None
        global_ds_rev.FlushCache()
        global_ds_rev = None

with open("metadata/loading_info.json", "w") as f:
    json.dump(loading_info, f, indent=2)

with open("metadata/loading_info_rev.json", "w") as f:
    json.dump(loading_info_rev, f, indent=2)

cmap = plt.get_cmap('tab20')
values = sorted(pixel_values)
n = len(values)

def colormap_color(idx):
    rgba = cmap(idx / max(n - 1, 1))
    return [int(rgba[0] * 255), int(rgba[1] * 255), int(rgba[2] * 255), int(rgba[3] * 255)]

import urllib.parse

colorizer = {
    "type": "singleBand",
    "band": 0,
    "bandColorizer": {
        "type": "palette",
        "colors": {
            str(v): colormap_color(i) for i, v in enumerate(values)
        },
        "noDataColor": [0, 0, 0, 0],
        "defaultColor": [0, 0, 0, 0]
    }
}

colorizer_json = json.dumps(colorizer, separators=(',', ':'))
colorizer_urlencoded = urllib.parse.quote(colorizer_json)

with open("metadata/colorizer_urlencoded.txt", "w") as f:
    f.write(colorizer_urlencoded)