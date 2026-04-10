"""Tests for vector streaming workflows"""

import asyncio
import unittest
import unittest.mock
from datetime import datetime
from uuid import UUID

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import websockets.protocol

import geoengine as ge

from . import UrllibMocker


class MockRequestsGet:
    """Mock for requests.get"""

    def __init__(self, json_data: dict[str, str]):
        self.__json = json_data

    def json(self) -> dict[str, str]:
        return self.__json


class MockWebsocket:
    """Mock for websockets.client.connect"""

    def __init__(self):
        """Create a mock websocket with some data"""

        self.__chunks = []

        chunk_size = 2

        (geos, times, datas) = read_data()

        for i in range(len(geos) // chunk_size):
            self.__chunks.append(
                arrow_bytes(
                    geos[i * chunk_size : (i + 1) * chunk_size],
                    times[i * chunk_size : (i + 1) * chunk_size],
                    datas[i * chunk_size : (i + 1) * chunk_size],
                )
            )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    @property
    def state(self) -> websockets.protocol.State:
        """Mock open impl"""
        return websockets.protocol.State.OPEN if len(self.__chunks) > 0 else websockets.protocol.State.CLOSED

    async def recv(self):
        return self.__chunks.pop(0)

    async def send(self, *args):
        pass

    async def close(self):
        pass


def read_data() -> tuple[list[str], list[list[int]], list[int]]:
    """Output vector data than can be subdivided into chunks"""
    geos = [
        "MULTIPOINT (-69.92356 12.43750)",
        "MULTIPOINT (-58.95141 -34.15333)",
        "MULTIPOINT (-59.00495 -34.09889)",
        "MULTIPOINT (-62.10088 -38.89444)",
        "MULTIPOINT (-62.30053 -38.78306)",
        "MULTIPOINT (-62.25989 -38.79194)",
        "MULTIPOINT (-61.85230 17.12278)",
        "MULTIPOINT (115.73810 -32.04750)",
    ]

    times = [
        [-8334632851200000, 8210298412799999],
        [-8334632851200000, 8210298412799999],
        [-8334632851200000, 8210298412799999],
        [-8334632851200000, 8210298412799999],
        [-8334632851200000, 8210298412799999],
        [-8334632851200000, 8210298412799999],
        [-8334632851200000, 8210298412799999],
        [-8334632851200000, 8210298412799999],
    ]

    datas = [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
    ]

    return (
        geos,
        times,
        datas,
    )


def arrow_bytes(geo: list[str], time: list[list[int]], data: list[int]) -> bytes:
    """Convert lists of vector data into an Arrow record batch within an IPC file"""

    geo_array = pa.array(geo)
    time_array = pa.array(time)
    data_array = pa.array(data)
    batch = pa.RecordBatch.from_arrays([geo_array, time_array, data_array], ["__geometry", "__time", "data"])
    schema = batch.schema.with_metadata(
        {
            "spatialReference": "EPSG:4326",
        }
    )

    sink = pa.BufferOutputStream()

    with pa.ipc.new_file(sink, schema) as writer:
        writer.write_batch(batch)

    return sink.getvalue()


class WorkflowVectorStreamTests(unittest.TestCase):
    """Test methods for retrieving vector workflows as data streams"""

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
            return_value=ge.VectorResultDescriptor(
                spatial_reference="EPSG:4326",
                data_type=ge.VectorDataType.MULTI_POINT,
                columns={
                    "data": ge.VectorColumnInfo(
                        data_type="int",
                        measurement=ge.UnitlessMeasurement,
                    )
                },
            ),
        ):
            workflow = ge.Workflow(UUID("00000000-0000-0000-0000-000000000000"))

        query_rect = ge.QueryRectangle(
            spatial_bounds=ge.BoundingBox2D(-180.0, -90.0, 180.0, 90.0),
            time_interval=ge.TimeInterval(datetime(2014, 4, 1, 0, 0, 0), datetime(2014, 6, 1, 0, 0, 0)),
        )

        with unittest.mock.patch("websockets.asyncio.client.connect", return_value=MockWebsocket()):

            async def inner1():
                chunks = []

                async for chunk in workflow.vector_stream(query_rect):
                    chunks.append(chunk)

                assert len(chunks) == 4

            asyncio.run(inner1())

        with unittest.mock.patch("websockets.asyncio.client.connect", return_value=MockWebsocket()):

            async def inner2():
                data_frame = await workflow.vector_stream_into_geopandas(query_rect)

                # 1x geo + 2x time + 1x data
                assert data_frame.shape == (8, 4)

                (geos, _times, datas) = read_data()

                assert data_frame["geometry"].equals(gpd.GeoSeries.from_wkt(geos))

                # TODO: when we can parse very early and very late times,
                # this should be possible to check against `_times`
                assert pd.isnull(data_frame["time_start"]).all()
                assert pd.isnull(data_frame["time_end"]).all()

                assert np.array_equal(data_frame["data"].tolist(), datas)

            asyncio.run(inner2())
