"""Raster data types"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import cast

import geoengine_openapi_client
import numpy as np
import pyarrow as pa
import xarray as xr

import geoengine.types as gety
from geoengine.util import clamp_datetime_ms_ns


class RasterTile2D:
    """A 2D raster tile as produced by the Geo Engine"""

    size_x: int
    size_y: int
    data: pa.Array
    geo_transform: gety.GeoTransform
    crs: str
    time: gety.TimeInterval
    band: int

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def __init__(
        self,
        shape: tuple[int, int],
        data: pa.Array,
        geo_transform: gety.GeoTransform,
        crs: str,
        time: gety.TimeInterval,
        band: int,
    ):
        """Create a RasterTile2D object"""
        self.size_y, self.size_x = shape
        self.data = data
        self.geo_transform = geo_transform
        self.crs = crs
        self.time = time
        self.band = band

    @property
    def shape(self) -> tuple[int, int]:
        """Return the shape of the raster tile in numpy order (y_size, x_size)"""
        return (self.size_y, self.size_x)

    @property
    def data_type(self) -> pa.DataType:
        """Return the arrow data type of the raster tile"""
        return self.data.type

    @property
    def numpy_data_type(self) -> np.dtype:
        """Return the numpy dtype of the raster tile"""
        return self.data_type.to_pandas_dtype()

    @property
    def has_null_values(self) -> bool:
        """Return whether the raster tile has null values"""
        return self.data.null_count > 0

    @property
    def time_start_ms(self) -> np.datetime64:
        return self.time.start.astype("datetime64[ms]")

    @property
    def time_end_ms(self) -> np.datetime64 | None:
        return None if self.time.end is None else self.time.end.astype("datetime64[ms]")

    @property
    def pixel_size(self) -> tuple[float, float]:
        return (self.geo_transform.x_pixel_size, self.geo_transform.y_pixel_size)

    def to_numpy_data_array(self, fill_null_value=0) -> np.ndarray:
        """
        Return the raster tile as a numpy array.
        Caution: this will not mask nodata values but replace them with the provided value !
        """
        nulled_array = self.data.fill_null(fill_null_value)
        return nulled_array.to_numpy(
            zero_copy_only=True,  # data was already copied when creating the "null filled" array
        ).reshape(self.shape)

    def to_numpy_mask_array(self, nan_is_null=False) -> np.ndarray | None:
        """
        Return the raster tiles mask as a numpy array.
        True means no data, False means data.
        If the raster tile has no null values, None is returned.
        It is possible to specify whether NaN values should be considered as no data when creating the mask.
        """
        numpy_mask = None
        if self.has_null_values:
            numpy_mask = (
                self.data.is_null(
                    nan_is_null=nan_is_null  # nan is not no data
                )
                .to_numpy(
                    zero_copy_only=False  # cannot zero-copy with bools
                )
                .reshape(self.shape)
            )
        return numpy_mask

    def to_numpy_masked_array(self, nan_is_null=False) -> np.ma.MaskedArray:
        """Return the raster tile as a masked numpy array"""
        numpy_data = self.to_numpy_data_array()
        maybe_numpy_mask = self.to_numpy_mask_array(nan_is_null=nan_is_null)

        assert maybe_numpy_mask is None or maybe_numpy_mask.shape == numpy_data.shape

        numpy_mask: np.ndarray | np.ma.MaskType = np.ma.nomask if maybe_numpy_mask is None else maybe_numpy_mask

        numpy_masked_data: np.ma.MaskedArray = np.ma.masked_array(numpy_data, mask=numpy_mask)

        return numpy_masked_data

    def coords_x(self, pixel_center=False) -> np.ndarray:
        """
        Return the x coordinates of the raster tile
        If pixel_center is True, the coordinates will be the center of the pixels.
        Otherwise they will be the upper left edges.
        """
        start = self.geo_transform.x_min

        if pixel_center:
            start += self.geo_transform.x_half_pixel_size

        return np.arange(
            start,
            stop=self.geo_transform.pixel_x_to_coord_x(self.size_x),
            step=self.geo_transform.x_pixel_size,
        )

    def coords_y(self, pixel_center=False) -> np.ndarray:
        """
        Return the y coordinates of the raster tile
        If pixel_center is True, the coordinates will be the center of the pixels.
        Otherwise they will be the upper left edges.
        """
        start = self.geo_transform.y_max

        if pixel_center:
            start += self.geo_transform.y_half_pixel_size

        return np.arange(
            start,
            stop=self.geo_transform.pixel_y_to_coord_y(self.size_y),
            step=self.geo_transform.y_pixel_size,
        )

    def to_xarray(self, clip_with_bounds: gety.SpatialBounds | None = None) -> xr.DataArray:
        """
        Return the raster tile as an xarray.DataArray.

        Note:
            - Xarray does not support masked arrays.
                - Masked pixels are converted to NaNs and the nodata value is set to NaN as well.
            - Xarray uses numpy's datetime64[ns] which only covers the years from 1678 to 2262.
                - Date times that are outside of the defined range are clipped to the limits of the range.
        """

        # clamp the dates to the min and max range
        clamped_date = clamp_datetime_ms_ns(self.time_start_ms)

        array = xr.DataArray(
            self.to_numpy_masked_array(),
            dims=["y", "x"],
            coords={
                "x": self.coords_x(pixel_center=True),
                "y": self.coords_y(pixel_center=True),
                "time": clamped_date,  # TODO: incorporate time end?
                "band": self.band,
            },
        )
        array.rio.write_crs(self.crs, inplace=True)

        if clip_with_bounds is not None:
            array = array.rio.clip_box(*clip_with_bounds.as_bbox_tuple(), auto_expand=True)
            array = cast(xr.DataArray, array)

        return array

    def spatial_partition(self) -> gety.SpatialPartition2D:
        """Return the spatial partition of the raster tile"""
        return gety.SpatialPartition2D(
            self.geo_transform.x_min,
            self.geo_transform.pixel_y_to_coord_y(self.size_y),
            self.geo_transform.pixel_x_to_coord_x(self.size_x),
            self.geo_transform.y_max,
        )

    def spatial_resolution(self) -> gety.SpatialResolution:
        return self.geo_transform.spatial_resolution()

    def is_empty(self) -> bool:
        """Returns true if the tile is empty"""
        num_pixels = self.size_x * self.size_y
        num_nulls = self.data.null_count
        return num_pixels == num_nulls

    @staticmethod
    def from_ge_record_batch(record_batch: pa.RecordBatch) -> RasterTile2D:
        """Create a RasterTile2D from an Arrow record batch recieved from the Geo Engine"""
        metadata = record_batch.schema.metadata
        inner = geoengine_openapi_client.GeoTransform.from_json(metadata[b"geoTransform"])
        assert inner is not None, "Failed to parse geoTransform"
        geo_transform = gety.GeoTransform.from_response(inner)
        x_size = int(metadata[b"xSize"])
        y_size = int(metadata[b"ySize"])
        spatial_reference = metadata[b"spatialReference"].decode("utf-8")
        # We know from the backend that there is only one array a.k.a. one column
        arrow_array = record_batch.column(0)

        inner_time = geoengine_openapi_client.TimeInterval.from_json(metadata[b"time"])
        assert inner_time is not None, "Failed to parse time"
        time = gety.TimeInterval.from_response(inner_time)

        band = int(metadata[b"band"])

        return RasterTile2D(
            (y_size, x_size),
            arrow_array,
            geo_transform,
            spatial_reference,
            time,
            band,
        )


class RasterTileStack2D:
    """A stack of all the bands of a raster tile as produced by the Geo Engine"""

    size_y: int
    size_x: int
    geo_transform: gety.GeoTransform
    crs: str
    time: gety.TimeInterval
    data: list[pa.Array]
    bands: list[int]

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def __init__(
        self,
        tile_shape: tuple[int, int],
        data: list[pa.Array],
        geo_transform: gety.GeoTransform,
        crs: str,
        time: gety.TimeInterval,
        bands: list[int],
    ):
        """Create a RasterTileStack2D object"""
        (self.size_y, self.size_x) = tile_shape
        self.data = data
        self.geo_transform = geo_transform
        self.crs = crs
        self.time = time
        self.bands = bands

    def single_band(self, index: int) -> RasterTile2D:
        """Return a single band from the stack"""
        return RasterTile2D(
            (self.size_y, self.size_x),
            self.data[index],
            self.geo_transform,
            self.crs,
            self.time,
            self.bands[index],
        )

    def to_numpy_masked_array_stack(self) -> np.ma.MaskedArray:
        """Return the raster stack as a 3D masked numpy array"""
        arrays = [self.single_band(i).to_numpy_masked_array() for i in range(0, len(self.data))]
        stack = np.ma.stack(arrays, axis=0)
        return stack

    def to_xarray(self, clip_with_bounds: gety.SpatialBounds | None = None) -> xr.DataArray:
        """Return the raster stack as an xarray.DataArray"""
        arrays = [self.single_band(i).to_xarray(clip_with_bounds) for i in range(0, len(self.data))]
        stack = xr.concat(arrays, dim="band")
        return stack


async def tile_stream_to_stack_stream(raster_stream: AsyncIterator[RasterTile2D]) -> AsyncIterator[RasterTileStack2D]:
    """Convert a stream of raster tiles to stream of stacked tiles"""
    store: list[RasterTile2D] = []
    first_band: int = -1

    async for tile in raster_stream:
        if len(store) == 0:
            first_band = tile.band
            store.append(tile)

        else:
            # check things that should be the same for all tiles
            assert tile.shape == store[0].shape, "Tile shapes do not match"
            # TODO: geo transform should be the same for all tiles
            #       tiles should have a tile position or global pixel position

            # assert tile.geo_transform == store[0].geo_transform, 'Tile geo_transforms do not match'
            assert tile.crs == store[0].crs, "Tile crs do not match"

            if tile.band == first_band:
                assert tile.time.start >= store[0].time.start, "Tile time intervals must be equal or increasing"

                stack = [tile.data for tile in store]
                tile_shape = store[0].shape
                bands = [tile.band for tile in store]
                geo_transforms = store[0].geo_transform
                crs = store[0].crs
                time = store[0].time

                store = [tile]
                yield RasterTileStack2D(tile_shape, stack, geo_transforms, crs, time, bands)

            else:
                assert tile.time == store[0].time, "Time missmatch. " + str(store[0].time) + " != " + str(tile.time)
                store.append(tile)

    if len(store) > 0:
        tile_shape = store[0].shape
        stack = [tile.data for tile in store]
        bands = [tile.band for tile in store]
        geo_transforms = store[0].geo_transform
        crs = store[0].crs
        time = store[0].time

        store = []

        yield RasterTileStack2D(tile_shape, stack, geo_transforms, crs, time, bands)
