"""A module that contains classes to write raster data from a Geo Engine raster workflow."""

from datetime import datetime
from typing import cast

import numpy as np
import rasterio as rio

from geoengine.types import (
    GeoTransform,
    QueryRectangle,
    RasterQueryRectangle,
    RasterResultDescriptor,
    TimeInterval,
)
from geoengine.workflow import Workflow

# pylint: disable=too-many-instance-attributes


class RasterWorkflowRioWriter:
    """
    A class to write raster data from a Geo Engine raster workflow to a GDAL dataset.
    It creates a new dataset for each time interval and writes the tiles to the dataset.
    Multiple bands are supported and the bands are written to the dataset in the order of the result descriptor.
    """

    current_dataset: rio.io.DatasetWriter | None = None
    current_time: TimeInterval | None = None
    dataset_geo_transform: GeoTransform | None = None
    dataset_width = None
    dataset_height = None
    dataset_data_type: np.dtype
    print_info = False

    dataset_prefix = None
    workflow: Workflow | None = None
    result_descriptor: RasterResultDescriptor
    no_data_value = 0
    time_format = "%Y-%m-%d_%H-%M-%S"

    gdal_driver = "GTiff"
    rio_kwargs = {"tiled": True, "compress": "DEFLATE", "zlevel": 6}
    tile_size = 512

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def __init__(
        self,
        dataset_prefix,
        workflow: Workflow,
        no_data_value=0,
        data_type: np.dtype | None = None,
        print_info=False,
        rio_kwargs=None,
    ):
        """Create a new RasterWorkflowGdalWriter instance."""
        self.dataset_prefix = dataset_prefix
        self.workflow = workflow
        self.no_data_value = no_data_value
        self.print_info = print_info

        ras_res = cast(RasterResultDescriptor, self.workflow.get_result_descriptor())
        self.result_descriptor = ras_res
        dt = ras_res.data_type.to_np_dtype()
        self.dataset_data_type = dt if data_type is None else data_type
        self.bands = ras_res.bands
        if rio_kwargs:
            for key, value in rio_kwargs.items():
                self.rio_kwargs[key] = value

    def close_current_dataset(self):
        """Close the current dataset"""
        if self.current_dataset:
            del self.current_dataset
            self.current_dataset = None

    # pylint: disable=too-many-locals, too-many-statements

    def create_gdal_geo_transform_width_height(self, query: QueryRectangle):
        """Create the tiling geo transform, width and height for the current query."""

        geo_transform = self.result_descriptor.geo_transform
        valid_bounds = query.spatial_bounds.intersection(self.result_descriptor.spatial_bounds.to_bounding_box())
        grid_bounds = geo_transform.spatial_to_grid_bounds(valid_bounds)

        width = grid_bounds.width
        height = grid_bounds.height

        geo_transform = GeoTransform(
            valid_bounds.xmin, valid_bounds.ymax, geo_transform.x_pixel_size, geo_transform.y_pixel_size
        )

        if self.dataset_geo_transform is None:
            self.dataset_geo_transform = geo_transform
        else:
            assert self.dataset_geo_transform == geo_transform, "Can not change the geo transform of the dataset"

        if self.dataset_width is None:
            self.dataset_width = width
        else:
            assert self.dataset_width == width, "The width of the current dataset does not match the new one"

        if self.dataset_height is None:
            self.dataset_height = height
        else:
            assert self.dataset_height == height, "The height of the current dataset does not match the new one"

    def __create_new_dataset(self, query: RasterQueryRectangle | QueryRectangle):
        """Create a new dataset for the current query."""

        assert self.current_time is not None, "The current time must be set"
        time_formated_start = self.current_time.start.astype(datetime).strftime(self.time_format)
        assert self.dataset_geo_transform is not None, "Dataset GeoTransform not set"
        affine_transform = rio.Affine.from_gdal(*self.dataset_geo_transform.to_gdal())
        if self.print_info:
            print(
                f"Creating dataset {self.dataset_prefix}{time_formated_start}.tif"
                f" with width {self.dataset_width}, height {self.dataset_height}, \
                      geo_transform {self.dataset_geo_transform}"
                f" rio kwargs: {self.rio_kwargs}"
            )
        assert self.bands is not None, "The bands of the ResultDescriptor must be set"

        raster_band_idxs = list(range(0, len(self.bands)))
        if isinstance(query, RasterQueryRectangle):
            raster_band_idxs = query.raster_bands

        dataset_bands = [self.bands[db] for db in raster_band_idxs]

        number_of_bands = len(dataset_bands)
        dataset_data_type = self.dataset_data_type
        file_path = f"{self.dataset_prefix}{time_formated_start}.tif"
        rio_dataset = rio.open(
            file_path,
            "w",
            driver=self.gdal_driver,
            width=self.dataset_width,
            height=self.dataset_height,
            count=number_of_bands,
            crs=query.srs,
            transform=affine_transform,
            dtype=dataset_data_type,
            nodata=self.no_data_value,
            **self.rio_kwargs,
        )

        for i, b in enumerate(dataset_bands, start=1):
            b_n = b.name
            b_m = str(b.measurement)
            rio_dataset.update_tags(i, band_name=b_n, band_measurement=b_m)

        self.current_dataset = rio_dataset

    async def query_and_write(self, query: RasterQueryRectangle, skip_empty_times=True):
        """
        Query the raster workflow and write the resulting tiles to a GDAL dataset per timeslice.

        :param query: The QueryRectangle to write to GDAL dataset(s)
        :param skip_empty_times: Skip timeslices where all pixels are empty/nodata
        """

        self.create_gdal_geo_transform_width_height(query)

        assert self.workflow is not None, "The workflow must be set"
        try:
            async for tile in self.workflow.raster_stream(query):
                if self.current_time != tile.time:
                    self.close_current_dataset()
                    self.current_time = tile.time

                if tile.is_empty() and skip_empty_times:
                    continue

                if self.current_dataset is None:
                    self.__create_new_dataset(query)

                assert self.current_time == tile.time, "The time of the current dataset does not match the tile"
                assert self.dataset_geo_transform is not None, "The geo transform must be set"
                assert self.dataset_height is not None
                assert self.dataset_width is not None

                ul_tile_px_idx = self.dataset_geo_transform.coord_to_pixel_ul(
                    tile.geo_transform.x_min, tile.geo_transform.y_max
                )

                lr_tile_px_x = ul_tile_px_idx.x_idx + self.tile_size
                lr_tile_px_y = ul_tile_px_idx.y_idx + self.tile_size

                assert ul_tile_px_idx.x_idx < self.dataset_width, "The tile must intersect the dataset (a)"
                assert ul_tile_px_idx.y_idx < self.dataset_height, "The tile must intersect the dataset (b)"
                assert lr_tile_px_x > 0, "The tile must intersect the dataset (c)"
                assert lr_tile_px_y > 0, "The tile must intersect the dataset (d)"

                crop_ul_x = 0 if ul_tile_px_idx.x_idx > 0 else -ul_tile_px_idx.x_idx
                crop_ul_y = 0 if ul_tile_px_idx.y_idx > 0 else -ul_tile_px_idx.y_idx
                crop_lr_x = 0 if lr_tile_px_x < self.dataset_width else self.dataset_width - lr_tile_px_x
                crop_lr_y = 0 if lr_tile_px_y < self.dataset_height else self.dataset_height - lr_tile_px_y

                band_index = tile.band + 1
                data = tile.to_numpy_data_array(self.no_data_value)

                data_crop = data[crop_ul_y : self.tile_size + crop_lr_y, crop_ul_x : self.tile_size + crop_lr_x]

                assert self.tile_size == tile.size_x == tile.size_y, "Tile size does not match the expected size"
                window = rio.windows.Window(
                    ul_tile_px_idx.x_idx + crop_ul_x,
                    ul_tile_px_idx.y_idx + crop_ul_y,
                    data_crop.shape[1],
                    data_crop.shape[0],
                )

                assert self.current_dataset is not None, "Dataset must be open."
                self.current_dataset.write(data_crop, window=window, indexes=band_index)
        except Exception as inner_e:
            raise RuntimeError("Exception when waiting for tiles") from inner_e

        finally:
            self.close_current_dataset()
