"""Tests for raster streaming workflows"""

import asyncio
import json
import unittest
import unittest.mock
from datetime import datetime
from uuid import UUID

import pyarrow as pa
import rioxarray
import websockets.protocol
import xarray as xr

import geoengine as ge
from geoengine.types import RasterBandDescriptor

from . import UrllibMocker


class MockWebsocket:
    """Mock for websockets.client.connect"""

    def __init__(self):
        """Create a mock websocket with some data"""

        self.__tiles = []

        for time in [datetime(2014, 1, 1, 0, 0, 0), datetime(2014, 1, 2, 0, 0, 0)]:
            for tiles in read_data():
                self.__tiles.append(arrow_bytes(tiles, ge.TimeInterval(start=time), 0))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    @property
    def state(self) -> websockets.protocol.State:
        """Mock open impl"""
        return websockets.protocol.State.OPEN if len(self.__tiles) > 0 else websockets.protocol.State.CLOSED

    async def recv(self):
        return self.__tiles.pop()

    async def send(self, *args):
        pass

    async def close(self):
        pass


def read_data() -> list[xr.DataArray]:
    """Slice a raster into 4 parts"""
    whole = rioxarray.open_rasterio("tests/responses/ndvi.tiff")

    if isinstance(whole, list):
        raise TypeError("Expected Dataset not List")

    whole = whole.isel(band=0)

    parts = [
        whole[:4, :4],
        whole[4:, :4],
        whole[:4, 4:],
        whole[4:, 4:],
    ]

    return parts


def arrow_bytes(data: xr.DataArray, time: ge.TimeInterval, band: int) -> bytes:
    """Convert a xarray.DataArray into an Arrow record batch within an IPC file"""

    array = pa.array(data.to_numpy().reshape(-1))
    batch = pa.RecordBatch.from_arrays([array], ["data"])
    schema = batch.schema.with_metadata(
        {
            "geoTransform": json.dumps(
                {
                    "originCoordinate": {
                        "x": data.rio.bounds()[0],
                        "y": data.rio.bounds()[3],
                    },
                    "xPixelSize": 45.0,
                    "yPixelSize": -22.5,
                }
            ),
            "xSize": "4",
            "ySize": "4",
            "spatialReference": "EPSG:4326",
            "time": json.dumps(
                {
                    "start": int(time.start.astype("datetime64[ms]").astype(int)),
                    "end": int(time.start.astype("datetime64[ms]").astype(int)),
                }
            ),
            "band": str(band),
        }
    )

    sink = pa.BufferOutputStream()

    with pa.ipc.new_file(sink, schema) as writer:
        writer.write_batch(batch)

    return sink.getvalue()


class WorkflowRasterStreamTests(unittest.TestCase):
    """Test methods for retrieving raster workflows as data streams"""

    def setUp(self) -> None:
        ge.reset(False)

    def test_streaming_workflow(self):
        with UrllibMocker() as m:
            m.get(
                "http://localhost:3030/session",
                json={
                    "id": "00000000-0000-0000-0000-000000000000",
                },
            )
            ge.initialize("http://localhost:3030", token="no_token")

        with unittest.mock.patch(
            "geoengine.Workflow._Workflow__query_result_descriptor",
            return_value=ge.RasterResultDescriptor(
                "U8",
                [RasterBandDescriptor("band", ge.UnitlessMeasurement())],
                "EPSG:4326",
                spatial_grid=ge.SpatialGridDescriptor(
                    descriptor="source",
                    spatial_grid=ge.SpatialGridDefinition(
                        geo_transform=ge.GeoTransform(x_min=-180.0, y_max=90.0, y_pixel_size=-22.5, x_pixel_size=45.0),
                        grid_bounds=ge.GridBoundingBox2D(
                            top_left_idx=ge.GridIdx2D(x_idx=0, y_idx=0), bottom_right_idx=(7, 7)
                        ),
                    ),
                ),
                time=ge.TimeDescriptor(dimension=ge.IrregularTimeDimension(), bounds=None),
            ),
        ):
            workflow = ge.Workflow(UUID("00000000-0000-0000-0000-000000000000"))

        query_rect = ge.QueryRectangle(
            spatial_bounds=ge.BoundingBox2D(-180.0, -90.0, 180.0, 90.0),
            time_interval=ge.TimeInterval(datetime(2014, 1, 1, 0, 0, 0), datetime(2014, 1, 3, 0, 0, 0)),
        )

        with unittest.mock.patch("websockets.asyncio.client.connect", return_value=MockWebsocket()):

            async def inner1():
                tiles = []

                async for tile in workflow.raster_stream(query_rect):
                    tiles.append(tile)

                assert len(tiles) == 8

            asyncio.run(inner1())

        with unittest.mock.patch("websockets.asyncio.client.connect", return_value=MockWebsocket()):

            async def inner2():
                array = await workflow.raster_stream_into_xarray(query_rect)
                assert array.shape == (2, 1, 8, 8)  # time, band, y, x

                original_array = rioxarray.open_rasterio("tests/responses/ndvi.tiff").isel(band=0, drop=True)

                # Let's check that the output is the same as if we would
                # have read the whole raster with rioxarray

                assert array.isel({"band": 0, "time": 0}, drop=True).equals(original_array)

            asyncio.run(inner2())
