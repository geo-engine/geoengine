from osgeo import gdal
import numpy as np
import os
import glob

# TODO: turn input bands into own tiffs, one tiff for each geoengine tile in the order of the engine production
def create_geoengine_tiles_from_raster(filename, out_dir=".", tile_size_px=512, origin=(0, 0)):
    """
    Create tiles from a raster file like they would be produced by Geo Engine.
    """
    ds = gdal.Open(filename)

    if ds is None:
        raise RuntimeError(f"Could not open {filename}")

    width = ds.RasterXSize
    height = ds.RasterYSize
    bands = ds.RasterCount

    # Get geotransform to map between pixel and geo coordinates
    gt = ds.GetGeoTransform()

    # Compute the pixel coordinate of (0,0) in the dataset
    # x_geo = gt[0] + px * gt[1] + py * gt[2]
    # y_geo = gt[3] + px * gt[4] + py * gt[5]
    # For north-up images, gt[2] and gt[4] are 0
    px0 = int(round((origin[0] - gt[0]) / gt[1]))
    py0 = int(round((origin[1] - gt[3]) / gt[5]))

    min_tile_x = (-(px0)) // tile_size_px
    max_tile_x = (width - px0 - 1) // tile_size_px

    min_tile_y = (-(py0)) // tile_size_px
    max_tile_y = (height - py0 - 1) // tile_size_px

    tile_index = 0
    for tile_y in range(min_tile_y, max_tile_y + 1):
        for tile_x in range(min_tile_x, max_tile_x + 1):
            # Compute pixel window in the dataset for this tile
            px_start = px0 + tile_x * tile_size_px
            py_start = py0 + tile_y * tile_size_px

            px_end = px_start + tile_size_px
            py_end = py_start + tile_size_px

            # Clip to dataset bounds
            read_xoff = max(px_start, 0)
            read_yoff = max(py_start, 0)
            read_xsize = min(px_end, width) - read_xoff
            read_ysize = min(py_end, height) - read_yoff

            # Prepare output array, fill with zeros (no data)
            dtype = gdal.GetDataTypeName(ds.GetRasterBand(1).DataType)
            np_dtype = {
                'Byte': np.uint8,
                'UInt16': np.uint16,
                'Int16': np.int16,
                'UInt32': np.uint32,
                'Int32': np.int32,
                'Float32': np.float32,
                'Float64': np.float64,
            }.get(dtype, np.uint16)
            tile_array = np.zeros((tile_size_px, tile_size_px), dtype=np_dtype)

            if read_xsize > 0 and read_ysize > 0:
                # Read data from source
                data = ds.GetRasterBand(1).ReadAsArray(read_xoff, read_yoff, read_xsize, read_ysize)
                # Place into output array
                x_insert = read_xoff - px_start
                y_insert = read_yoff - py_start
                tile_array[y_insert:y_insert+read_ysize, x_insert:x_insert+read_xsize] = data

            # Write tile to file
            base_name = os.path.splitext(os.path.basename(filename))[0]
            out_filename = f"{out_dir}/{base_name}_tile_{tile_index}.tif"
            driver = gdal.GetDriverByName('GTiff')
            out_ds = driver.Create(
                out_filename, tile_size_px, tile_size_px, 1, ds.GetRasterBand(1).DataType,
                options=["COMPRESS=DEFLATE"]
            )
            # Compute geotransform for this tile
            tile_origin_x = gt[0] + (px0 + tile_x * tile_size_px) * gt[1]
            tile_origin_y = gt[3] + (py0 + tile_y * tile_size_px) * gt[5]
            out_gt = (tile_origin_x, gt[1], 0, tile_origin_y, 0, gt[5])
            out_ds.SetGeoTransform(out_gt)
            out_ds.SetProjection(ds.GetProjection())
            out_ds.GetRasterBand(1).WriteArray(tile_array)
            out_ds.GetRasterBand(1).SetNoDataValue(0)
            out_ds.FlushCache()
            out_ds = None

            tile_index += 1

    ds = None

if __name__ == "__main__":
    input_dir = "results/z_index/global"
    for filepath in glob.glob(os.path.join(input_dir, "*.tif")):
        if os.path.isfile(filepath):
            create_geoengine_tiles_from_raster(filepath, "results/z_index/tiles", tile_size_px=512, origin=(0, 0))

    input_dir = "results/z_index_reversed/global"
    for filepath in glob.glob(os.path.join(input_dir, "*.tif")):
        if os.path.isfile(filepath):
            create_geoengine_tiles_from_raster(filepath, "results/z_index_reversed/tiles", tile_size_px=512, origin=(0, 0))