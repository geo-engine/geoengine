"""
A processing graph representation and methods on processing graphs
"""
# pylint: disable=too-many-lines
# TODO: split into multiple files

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from collections.abc import AsyncIterator
from io import BytesIO
from logging import debug
from os import PathLike
from typing import Any, TypedDict, cast
from uuid import UUID

import geoengine_api_client as geoc
import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import rasterio.io
import requests as req
import rioxarray
import websockets
import websockets.asyncio.client
import xarray as xr
from owslib.util import Authentication, ResponseWrapper
from owslib.wcs import WebCoverageService
from PIL import Image
from typing_extensions import deprecated
from vega import VegaLite

from geoengine import api, backports
from geoengine.auth import get_session
from geoengine.error import (
    GeoEngineException,
    InputException,
    MethodNotCalledOnPlotException,
    MethodNotCalledOnRasterException,
    MethodNotCalledOnVectorException,
    OGCXMLError,
)
from geoengine.processing_graph_builder.operators import Operator as ProcessingGraphBuilderOperator
from geoengine.raster import RasterTile2D
from geoengine.tasks import Task, TaskId
from geoengine.types import (
    ClassificationMeasurement,
    ProvenanceEntry,
    QueryRectangle,
    RasterColorizer,
    RasterQueryRectangle,
    RasterResultDescriptor,
    ResultDescriptor,
    SpatialPartition2D,
    SpatialResolution,
    VectorResultDescriptor,
)
from geoengine.util import eprint

# TODO: Define as recursive type when supported in mypy: https://github.com/python/mypy/issues/731
JsonType = dict[str, Any] | list[Any] | int | str | float | bool | type[None]


class Axis(TypedDict):
    title: str


class Bin(TypedDict):
    binned: bool
    step: float


class Field(TypedDict):
    field: str


class DatasetIds(TypedDict):
    upload: UUID
    dataset: UUID


class Values(TypedDict):
    binStart: float
    binEnd: float
    Frequency: int


class X(TypedDict):
    field: Field
    bin: Bin
    axis: Axis


class X2(TypedDict):
    field: Field


class Y(TypedDict):
    field: Field
    type: str


class Encoding(TypedDict):
    x: X
    x2: X2
    y: Y


VegaSpec = TypedDict("VegaSpec", {"$schema": str, "data": list[Values], "mark": str, "encoding": Encoding})


class ProcessingGraphId:
    """
    A wrapper around a processing graph UUID
    """

    __processing_graph_id: UUID

    def __init__(self, processing_graph_id: UUID | str) -> None:
        """Create a new ProcessingGraphId from an UUID or uuid as str"""

        if not isinstance(processing_graph_id, UUID):
            processing_graph_id = UUID(processing_graph_id)

        self.__processing_graph_id = processing_graph_id

    @classmethod
    def from_response(cls, response: geoc.IdResponse) -> ProcessingGraphId:
        """
        Create a `ProcessingGraphId` from an http response
        """
        return ProcessingGraphId(response.id)

    def __str__(self) -> str:
        return str(self.__processing_graph_id)

    def __repr__(self) -> str:
        return str(self)

    def to_dict(self) -> UUID:
        return self.__processing_graph_id


class RasterStreamProcessing:
    """
    Helper class to process raster stream data
    """

    @classmethod
    def read_arrow_ipc(cls, arrow_ipc: bytes) -> pa.RecordBatch:
        """Read an Arrow IPC file from a byte array"""

        reader = pa.ipc.open_file(arrow_ipc)
        # We know from the backend that there is only one record batch
        record_batch = reader.get_record_batch(0)
        return record_batch

    @classmethod
    def process_bytes(cls, tile_bytes: bytes | None) -> RasterTile2D | None:
        """Process a tile from a byte array"""

        if tile_bytes is None:
            return None

        # process the received data
        record_batch = RasterStreamProcessing.read_arrow_ipc(tile_bytes)
        tile = RasterTile2D.from_ge_record_batch(record_batch)

        return tile

    @classmethod
    def merge_tiles(cls, tiles: list[xr.DataArray]) -> xr.DataArray | None:
        """Merge a list of tiles into a single xarray"""

        if len(tiles) == 0:
            return None

        # group the tiles by band
        tiles_by_band: dict[int, list[xr.DataArray]] = defaultdict(list)
        for tile in tiles:
            band = tile.band.item()  # assuming 'band' is a coordinate with a single value
            tiles_by_band[band].append(tile)

        # build one spatial tile per band
        combined_by_band = []
        for band_tiles in tiles_by_band.values():
            combined = xr.combine_by_coords(band_tiles)
            # `combine_by_coords` always returns a `DataArray` for single variable input arrays.
            # This assertion verifies this for mypy
            assert isinstance(combined, xr.DataArray)
            combined_by_band.append(combined)

        # build one array with all bands and geo coordinates
        combined_tile = xr.concat(combined_by_band, dim="band")

        return combined_tile


class ProcessingGraph:
    """
    Holds a processing graph id and allows querying data
    """

    __processing_graph_id: ProcessingGraphId
    __result_descriptor: ResultDescriptor

    def __init__(self, processing_graph_id: ProcessingGraphId) -> None:
        self.__processing_graph_id = processing_graph_id
        self.__result_descriptor = self.__query_result_descriptor()

    def __str__(self) -> str:
        return str(self.__processing_graph_id)

    def __repr__(self) -> str:
        return repr(self.__processing_graph_id)

    def __query_result_descriptor(self, timeout: int = 60) -> ResultDescriptor:
        """
        Query the metadata of the processing graph result
        """

        session = get_session()

        with geoc.ApiClient(session.configuration) as api_client:
            processing_graphs_api = geoc.ProcessingGraphsApi(api_client)
            response = processing_graphs_api.get_processing_graph_metadata_handler(
                self.__processing_graph_id.to_dict(), _request_timeout=timeout
            )

        debug(response)

        return ResultDescriptor.from_response(response)

    def get_result_descriptor(self) -> ResultDescriptor:
        """
        Return the metadata of the processing graph result
        """

        return self.__result_descriptor

    def definition(self, timeout: int = 60) -> geoc.ProcessingGraph:
        """Return the definition of this processing graph"""

        session = get_session()

        with geoc.ApiClient(session.configuration) as api_client:
            processing_graphs_api = geoc.ProcessingGraphsApi(api_client)
            response = processing_graphs_api.load_processing_graph_handler(
                self.__processing_graph_id.to_dict(), _request_timeout=timeout
            )

        return response

    @deprecated("Use `ProcessingGraph.definition` instead.", category=DeprecationWarning)
    def workflow_definition(self, timeout: int = 60) -> geoc.ProcessingGraph:
        """Deprecated: use `ProcessingGraph.definition` instead."""
        return self.definition(timeout=timeout)

    def get_dataframe(
        self, bbox: QueryRectangle, timeout: int = 3600, resolve_classifications: bool = False
    ) -> gpd.GeoDataFrame:
        """
        Query a processing graph and return the WFS result as a GeoPandas `GeoDataFrame`

        The computation ID from the server response is attached as an attribute.
        Access via: `df.attrs.get('computation_id')`
        """

        if not self.__result_descriptor.is_vector_result():
            raise MethodNotCalledOnVectorException()

        session = get_session()

        with geoc.ApiClient(session.configuration) as api_client:
            wfs_api = geoc.OGCWFSApi(api_client)
            response = wfs_api.wfs_handler_with_http_info(
                processing_graph=self.__processing_graph_id.to_dict(),
                service=geoc.WfsService(geoc.WfsService.WFS),
                request=geoc.WfsRequest(geoc.WfsRequest.GETFEATURE),
                type_names=str(self.__processing_graph_id),
                bbox=bbox.bbox_str,
                version=geoc.WfsVersion(geoc.WfsVersion.ENUM_2_DOT_0_DOT_0),
                time=bbox.time_str,
                srs_name=bbox.srs,
                _request_timeout=timeout,
            )

        # Extract computation ID from response headers
        computation_id = None
        if response.headers:
            computation_id = response.headers.get("x-computation-id")

        def geo_json_with_time_to_geopandas(geo_json):
            """
            GeoJson has no standard for time, so we parse the when field
            separately and attach it to the data frame as columns `start`
            and `end`.
            """

            data = gpd.GeoDataFrame.from_features(geo_json)
            data = data.set_crs(bbox.srs, allow_override=True)

            start = [f["when"]["start"] for f in geo_json["features"]]
            end = [f["when"]["end"] for f in geo_json["features"]]

            # TODO: find a good way to infer BoT/EoT

            data["start"] = gpd.pd.to_datetime(start, errors="coerce")
            data["end"] = gpd.pd.to_datetime(end, errors="coerce")

            return data

        def transform_classifications(data: gpd.GeoDataFrame):
            result_descriptor: VectorResultDescriptor = self.__result_descriptor  # type: ignore
            for column, info in result_descriptor.columns.items():
                if isinstance(info.measurement, ClassificationMeasurement):
                    measurement: ClassificationMeasurement = info.measurement
                    classes = measurement.classes
                    data[column] = data[column].apply(lambda x, classes=classes: classes[x])  # pylint: disable=cell-var-from-loop

            return data

        result = geo_json_with_time_to_geopandas(response.data.to_dict())

        if resolve_classifications:
            result = transform_classifications(result)

        # Attach computation ID to the result
        result.attrs["computation_id"] = computation_id

        return result

    def wms_get_map_as_image(
        self,
        bbox: QueryRectangle,
        raster_colorizer: RasterColorizer,
        # TODO: allow to use width height
        spatial_resolution: SpatialResolution,
    ) -> Image.Image:
        """Return the result of a WMS request as a PIL Image"""

        if not self.__result_descriptor.is_raster_result():
            raise MethodNotCalledOnRasterException()

        session = get_session()

        with geoc.ApiClient(session.configuration) as api_client:
            wms_api = geoc.OGCWMSApi(api_client)
            response = wms_api.wms_handler(
                processing_graph=self.__processing_graph_id.to_dict(),
                version=geoc.WmsVersion(geoc.WmsVersion.ENUM_1_DOT_3_DOT_0),
                service=geoc.WmsService(geoc.WmsService.WMS),
                request=geoc.WmsRequest(geoc.WmsRequest.GETMAP),
                width=int((bbox.spatial_bounds.xmax - bbox.spatial_bounds.xmin) / spatial_resolution.x_resolution),
                height=int((bbox.spatial_bounds.ymax - bbox.spatial_bounds.ymin) / spatial_resolution.y_resolution),  # pylint: disable=line-too-long
                bbox=bbox.bbox_ogc_str,
                format=geoc.WmsResponseFormat(geoc.WmsResponseFormat.IMAGE_SLASH_PNG),
                layers=str(self),
                styles="custom:" + raster_colorizer.to_api_dict().to_json(),
                crs=bbox.srs,
                time=bbox.time_str,
            )

        if OGCXMLError.is_ogc_error(response):
            raise OGCXMLError(response)

        return Image.open(BytesIO(response))

    def plot_json(
        self, bbox: QueryRectangle, spatial_resolution: SpatialResolution | None = None, timeout: int = 3600
    ) -> geoc.WrappedPlotOutput:
        """
        Query a processing graph and return the plot chart result as WrappedPlotOutput
        """

        if not self.__result_descriptor.is_plot_result():
            raise MethodNotCalledOnPlotException()

        session = get_session()

        with geoc.ApiClient(session.configuration) as api_client:
            plots_api = geoc.PlotsApi(api_client)
            return plots_api.get_plot_handler(
                bbox.bbox_str,
                bbox.time_str,
                str(spatial_resolution),
                self.__processing_graph_id.to_dict(),
                bbox.srs,
                _request_timeout=timeout,
            )

    def plot_chart(
        self, bbox: QueryRectangle, spatial_resolution: SpatialResolution | None = None, timeout: int = 3600
    ) -> VegaLite:
        """
        Query a processing graph and return the plot chart result as a vega plot
        """

        response = self.plot_json(bbox, spatial_resolution, timeout)
        vega_spec: VegaSpec = json.loads(response.data["vegaString"])

        return VegaLite(vega_spec)

    def __request_wcs(
        self,
        bbox: QueryRectangle,
        timeout=3600,
        file_format: str = "image/tiff",
        force_no_data_value: float | None = None,
        spatial_resolution: SpatialResolution | None = None,
    ) -> ResponseWrapper:
        """
        Query a processing graph and return the coverage

        Parameters
        ----------
        bbox : A bounding box for the query
        timeout : HTTP request timeout in seconds
        file_format : The format of the returned raster
        force_no_data_value: If not None, use this value as no data value for the requested raster data. \
            Otherwise, use the Geo Engine will produce masked rasters.
        """

        if not self.__result_descriptor.is_raster_result():
            raise MethodNotCalledOnRasterException()

        session = get_session()

        # TODO: properly build CRS string for bbox
        crs = f"urn:ogc:def:crs:{bbox.srs.replace(':', '::')}"

        wcs_url = f"{session.server_url}/wcs/{self.__processing_graph_id}"
        wcs = WebCoverageService(
            wcs_url,
            version="1.1.1",
            auth=Authentication(auth_delegate=session.requests_bearer_auth()),
        )

        resx = None
        resy = None
        if spatial_resolution is not None:
            [resx, resy] = spatial_resolution.resolution_ogc(bbox.srs)

        kwargs = {}

        # TODO: allow subset of bands from RasterQueryRectangle
        if force_no_data_value is not None:
            kwargs["nodatavalue"] = str(float(force_no_data_value))
        if resx is not None:
            kwargs["resx"] = str(resx)
        if resy is not None:
            kwargs["resy"] = str(resy)

        return wcs.getCoverage(
            identifier=f"{self.__processing_graph_id}",
            bbox=bbox.bbox_ogc,
            time=[bbox.time_str],
            format=file_format,
            crs=crs,
            timeout=timeout,
            **kwargs,
        )

    def __get_wcs_tiff_as_memory_file(
        self,
        bbox: QueryRectangle,
        timeout=3600,
        force_no_data_value: float | None = None,
        spatial_resolution: SpatialResolution | None = None,
    ) -> rasterio.io.MemoryFile:
        """
        Query a processing graph and return the raster result as a memory mapped GeoTiff

        Parameters
        ----------
        bbox : A bounding box for the query
        timeout : HTTP request timeout in seconds
        force_no_data_value: If not None, use this value as no data value for the requested raster data. \
            Otherwise, use the Geo Engine will produce masked rasters.
        """

        response = self.__request_wcs(bbox, timeout, "image/tiff", force_no_data_value, spatial_resolution).read()

        # response is checked via `raise_on_error` in `getCoverage` / `openUrl`

        memory_file = rasterio.io.MemoryFile(response)

        return memory_file

    def get_array(
        self,
        bbox: QueryRectangle,
        spatial_resolution: SpatialResolution | None = None,
        timeout=3600,
        force_no_data_value: float | None = None,
    ) -> np.ndarray:
        """
        Query a processing graph and return the raster result as a numpy array

        Parameters
        ----------
        bbox : A bounding box for the query
        timeout : HTTP request timeout in seconds
        force_no_data_value: If not None, use this value as no data value for the requested raster data. \
            Otherwise, use the Geo Engine will produce masked rasters.
        """

        with (
            self.__get_wcs_tiff_as_memory_file(bbox, timeout, force_no_data_value, spatial_resolution) as memfile,
            memfile.open() as dataset,
        ):
            array = dataset.read(1)

            return array

    def get_xarray(
        self,
        bbox: QueryRectangle,
        spatial_resolution: SpatialResolution | None = None,
        timeout=3600,
        force_no_data_value: float | None = None,
    ) -> xr.DataArray:
        """
        Query a processing graph and return the raster result as a georeferenced xarray

        Parameters
        ----------
        bbox : A bounding box for the query
        timeout : HTTP request timeout in seconds
        force_no_data_value: If not None, use this value as no data value for the requested raster data. \
            Otherwise, use the Geo Engine will produce masked rasters.
        """

        with (
            self.__get_wcs_tiff_as_memory_file(bbox, timeout, force_no_data_value, spatial_resolution) as memfile,
            memfile.open() as dataset,
        ):
            data_array = rioxarray.open_rasterio(dataset)

            # helping mypy with inference
            assert isinstance(data_array, xr.DataArray)

            rio: xr.DataArray = data_array.rio
            rio.update_attrs(
                {
                    "crs": rio.crs,
                    "res": rio.resolution(),
                    "transform": rio.transform(),
                },
                inplace=True,
            )

            # TODO: add time information to dataset
            return data_array.load()

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def download_raster(
        self,
        bbox: QueryRectangle,
        file_path: str,
        timeout=3600,
        file_format: str = "image/tiff",
        force_no_data_value: float | None = None,
        spatial_resolution: SpatialResolution | None = None,
    ) -> None:
        """
        Query a processing graph and save the raster result as a file on disk

        Parameters
        ----------
        bbox : A bounding box for the query
        file_path : The path to the file to save the raster to
        timeout : HTTP request timeout in seconds
        file_format : The format of the returned raster
        force_no_data_value: If not None, use this value as no data value for the requested raster data. \
            Otherwise, use the Geo Engine will produce masked rasters.
        """

        response = self.__request_wcs(bbox, timeout, file_format, force_no_data_value, spatial_resolution)

        with open(file_path, "wb") as file:
            file.write(response.read())

    def get_provenance(self, timeout: int = 60) -> list[ProvenanceEntry]:
        """
        Query the provenance of the processing graph
        """

        session = get_session()

        with geoc.ApiClient(session.configuration) as api_client:
            processing_graphs_api = geoc.ProcessingGraphsApi(api_client)
            response = processing_graphs_api.get_processing_graph_provenance_handler(
                self.__processing_graph_id.to_dict(), _request_timeout=timeout
            )

        return [ProvenanceEntry.from_response(item) for item in response]

    def metadata_zip(self, path: PathLike | BytesIO, timeout: int = 60) -> None:
        """
        Query processing graph metadata and citations and stores it as zip file to `path`
        """

        session = get_session()

        with geoc.ApiClient(session.configuration) as api_client:
            processing_graphs_api = geoc.ProcessingGraphsApi(api_client)
            response = processing_graphs_api.get_processing_graph_all_metadata_zip_handler(
                self.__processing_graph_id.to_dict(), _request_timeout=timeout
            )

        if isinstance(path, BytesIO):
            path.write(response)
        else:
            with open(path, "wb") as file:
                file.write(response)

    # pylint: disable=too-many-positional-arguments,too-many-positional-arguments
    def save_as_dataset(
        self,
        query_rectangle: QueryRectangle,
        name: None | str,
        display_name: str,
        description: str = "",
        timeout: int = 3600,
    ) -> Task:
        """Init task to store the processing graph result as a layer"""

        # Currently, it only works for raster results
        if not self.__result_descriptor.is_raster_result():
            raise MethodNotCalledOnRasterException()

        session = get_session()

        if not isinstance(query_rectangle, QueryRectangle):
            eprint("save_as_dataset ignores params other then spatial and tmporal bounds.")

        qrect = geoc.models.raster_to_dataset_query_rectangle.RasterToDatasetQueryRectangle(
            spatial_bounds=SpatialPartition2D.from_bounding_box(query_rectangle.spatial_bounds).to_api_dict(),
            time_interval=query_rectangle.time.to_api_dict(),
        )

        with geoc.ApiClient(session.configuration) as api_client:
            processing_graphs_api = geoc.ProcessingGraphsApi(api_client)
            response = processing_graphs_api.dataset_from_processing_graph_handler(
                self.__processing_graph_id.to_dict(),
                geoc.RasterDatasetFromProcessingGraph(
                    name=name, display_name=display_name, description=description, query=qrect
                ),
                _request_timeout=timeout,
            )

        return Task(TaskId.from_response(response))

    async def raster_stream(
        self,
        query_rectangle: QueryRectangle | RasterQueryRectangle,
        open_timeout: int = 60,
    ) -> AsyncIterator[RasterTile2D]:
        """Stream the processing graph result as series of RasterTile2D (transformable to numpy and xarray)"""

        # Currently, it only works for raster results
        if not self.__result_descriptor.is_raster_result():
            raise MethodNotCalledOnRasterException()

        result_descriptor = cast(RasterResultDescriptor, self.__result_descriptor)

        if not isinstance(query_rectangle, RasterQueryRectangle):
            query_rectangle = query_rectangle.with_raster_bands(
                # TODO: all bands or first band?
                list(range(0, len(result_descriptor.bands)))
            )

        session = get_session()

        url = (
            req.Request(
                "GET",
                url=f"{session.server_url}/processingGraphs/{self.__processing_graph_id}/rasterStream",
                params={
                    "resultType": "arrow",
                    "spatialBounds": query_rectangle.bbox_str,
                    "timeInterval": query_rectangle.time_str,
                    "attributes": ",".join(map(str, query_rectangle.raster_bands)),
                },
            )
            .prepare()
            .url
        )

        if url is None:
            raise InputException("Invalid websocket url")

        async with websockets.asyncio.client.connect(
            uri=self.__replace_http_with_ws(url),
            additional_headers=session.auth_header,
            open_timeout=open_timeout,
            max_size=None,
        ) as websocket:
            tile_bytes: bytes | None = None

            while websocket.state == websockets.protocol.State.OPEN:

                async def read_new_bytes() -> bytes | None:
                    # already send the next request to speed up the process
                    try:
                        await websocket.send("NEXT")
                    except websockets.exceptions.ConnectionClosed:
                        # the websocket connection is already closed, we cannot read anymore
                        return None

                    try:
                        data: str | bytes = await websocket.recv()

                        if isinstance(data, str):
                            # the server sent an error message
                            raise GeoEngineException({"error": data})

                        return data
                    except websockets.exceptions.ConnectionClosedOK:
                        # the websocket connection closed gracefully, so we stop reading
                        return None

                (tile_bytes, tile) = await asyncio.gather(
                    read_new_bytes(),
                    # asyncio.to_thread(process_bytes, tile_bytes), # TODO: use this when min Python version is 3.9
                    backports.to_thread(RasterStreamProcessing.process_bytes, tile_bytes),
                )

                if tile is not None:
                    yield tile

            # process the last tile
            tile = RasterStreamProcessing.process_bytes(tile_bytes)

            if tile is not None:
                yield tile

    async def raster_stream_into_xarray(
        self,
        query_rectangle: RasterQueryRectangle,
        clip_to_query_rectangle: bool = False,
        open_timeout: int = 60,
    ) -> xr.DataArray:
        """
        Stream the processing graph result into memory and output a single xarray.

        NOTE: You can run out of memory if the query rectangle is too large.
        """

        tile_stream = self.raster_stream(query_rectangle, open_timeout=open_timeout)

        timestep_xarrays: list[xr.DataArray] = []

        spatial_clip_bounds = query_rectangle.spatial_bounds if clip_to_query_rectangle else None

        async def read_tiles(
            remainder_tile: RasterTile2D | None,
        ) -> tuple[list[xr.DataArray], RasterTile2D | None]:
            last_timestep: np.datetime64 | None = None
            tiles = []

            if remainder_tile is not None:
                last_timestep = remainder_tile.time_start_ms
                xr_tile = remainder_tile.to_xarray(clip_with_bounds=spatial_clip_bounds)
                tiles.append(xr_tile)

            async for tile in tile_stream:
                timestep: np.datetime64 = tile.time_start_ms
                if last_timestep is None:
                    last_timestep = timestep
                elif last_timestep != timestep:
                    return tiles, tile

                xr_tile = tile.to_xarray(clip_with_bounds=spatial_clip_bounds)
                tiles.append(xr_tile)

            # this seems to be the last time step, so just return tiles
            return tiles, None

        (tiles, remainder_tile) = await read_tiles(None)

        while len(tiles):
            ((new_tiles, new_remainder_tile), new_timestep_xarray) = await asyncio.gather(
                read_tiles(remainder_tile),
                backports.to_thread(RasterStreamProcessing.merge_tiles, tiles),
                # asyncio.to_thread(merge_tiles, tiles), # TODO: use this when min Python version is 3.9
            )

            tiles = new_tiles
            remainder_tile = new_remainder_tile

            if new_timestep_xarray is not None:
                timestep_xarrays.append(new_timestep_xarray)

        output: xr.DataArray = cast(
            xr.DataArray,
            # await asyncio.to_thread( # TODO: use this when min Python version is 3.9
            await backports.to_thread(
                xr.concat,
                # TODO: This is a typings error, since the method accepts also a `xr.DataArray` and returns one
                cast(list[xr.Dataset], timestep_xarrays),
                dim="time",
            ),
        )

        return output

    async def vector_stream(
        self,
        query_rectangle: QueryRectangle,
        time_start_column: str = "time_start",
        time_end_column: str = "time_end",
        open_timeout: int = 60,
    ) -> AsyncIterator[gpd.GeoDataFrame]:
        """Stream the processing graph result as series of `GeoDataFrame`s"""

        def read_arrow_ipc(arrow_ipc: bytes) -> pa.RecordBatch:
            reader = pa.ipc.open_file(arrow_ipc)
            # We know from the backend that there is only one record batch
            record_batch = reader.get_record_batch(0)
            return record_batch

        def create_geo_data_frame(
            record_batch: pa.RecordBatch, time_start_column: str, time_end_column: str
        ) -> gpd.GeoDataFrame:
            metadata = record_batch.schema.metadata
            spatial_reference = metadata[b"spatialReference"].decode("utf-8")

            data_frame = record_batch.to_pandas()

            geometry = gpd.GeoSeries.from_wkt(data_frame[api.GEOMETRY_COLUMN_NAME])
            # delete the duplicated column
            del data_frame[api.GEOMETRY_COLUMN_NAME]

            geo_data_frame = gpd.GeoDataFrame(
                data_frame,
                geometry=geometry,
                crs=spatial_reference,
            )

            # split time column
            geo_data_frame[[time_start_column, time_end_column]] = geo_data_frame[api.TIME_COLUMN_NAME].tolist()
            # delete the duplicated column
            del geo_data_frame[api.TIME_COLUMN_NAME]

            # parse time columns
            for time_column in [time_start_column, time_end_column]:
                # TODO: use this when Python 3.11 is minimum version
                # geo_data_frame[time_column] = pd.to_datetime(
                #     geo_data_frame[time_column],
                #     utc=True,
                #     unit="ms",
                #     # TODO: solve time conversion problem from Geo Engine to Python for large (+/-) time instances
                #     errors="coerce",
                # )
                geo_data_frame[time_column] = pd.Series(
                    geo_data_frame[time_column].values.astype("datetime64[ms]")
                ).dt.tz_localize("UTC")

            return geo_data_frame

        def process_bytes(batch_bytes: bytes | None) -> gpd.GeoDataFrame | None:
            if batch_bytes is None:
                return None

            # process the received data
            record_batch = read_arrow_ipc(batch_bytes)
            tile = create_geo_data_frame(
                record_batch,
                time_start_column=time_start_column,
                time_end_column=time_end_column,
            )

            return tile

        # Currently, it only works for raster results
        if not self.__result_descriptor.is_vector_result():
            raise MethodNotCalledOnVectorException()

        session = get_session()

        params = {
            "resultType": "arrow",
            "spatialBounds": query_rectangle.bbox_str,
            "timeInterval": query_rectangle.time_str,
        }

        url = (
            req.Request(
                "GET",
                url=f"{session.server_url}/processingGraphs/{self.__processing_graph_id}/vectorStream",
                params=params,
            )
            .prepare()
            .url
        )

        if url is None:
            raise InputException("Invalid websocket url")

        async with websockets.asyncio.client.connect(
            uri=self.__replace_http_with_ws(url),
            additional_headers=session.auth_header,
            open_timeout=open_timeout,
            max_size=None,  # allow arbitrary large messages, since it is capped by the server's chunk size
        ) as websocket:
            batch_bytes: bytes | None = None

            while websocket.state == websockets.protocol.State.OPEN:

                async def read_new_bytes() -> bytes | None:
                    # already send the next request to speed up the process
                    try:
                        await websocket.send("NEXT")
                    except websockets.exceptions.ConnectionClosed:
                        # the websocket connection is already closed, we cannot read anymore
                        return None

                    try:
                        data: str | bytes = await websocket.recv()

                        if isinstance(data, str):
                            # the server sent an error message
                            raise GeoEngineException({"error": data})

                        return data
                    except websockets.exceptions.ConnectionClosedOK:
                        # the websocket connection closed gracefully, so we stop reading
                        return None

                (batch_bytes, batch) = await asyncio.gather(
                    read_new_bytes(),
                    # asyncio.to_thread(process_bytes, batch_bytes), # TODO: use this when min Python version is 3.9
                    backports.to_thread(process_bytes, batch_bytes),
                )

                if batch is not None:
                    yield batch

            # process the last tile
            batch = process_bytes(batch_bytes)

            if batch is not None:
                yield batch

    async def vector_stream_into_geopandas(
        self,
        query_rectangle: QueryRectangle,
        time_start_column: str = "time_start",
        time_end_column: str = "time_end",
        open_timeout: int = 60,
    ) -> gpd.GeoDataFrame:
        """
        Stream the processing graph result into memory and output a single geo data frame.

        NOTE: You can run out of memory if the query rectangle is too large.
        """

        chunk_stream = self.vector_stream(
            query_rectangle,
            time_start_column=time_start_column,
            time_end_column=time_end_column,
            open_timeout=open_timeout,
        )

        data_frame: gpd.GeoDataFrame | None = None
        chunk: gpd.GeoDataFrame | None = None

        async def read_dataframe() -> gpd.GeoDataFrame | None:
            try:
                return await chunk_stream.__anext__()
            except StopAsyncIteration:
                return None

        def merge_dataframes(df_a: gpd.GeoDataFrame | None, df_b: gpd.GeoDataFrame | None) -> gpd.GeoDataFrame | None:
            if df_a is None:
                return df_b

            if df_b is None:
                return df_a

            return pd.concat([df_a, df_b], ignore_index=True)

        while True:
            (chunk, data_frame) = await asyncio.gather(
                read_dataframe(),
                backports.to_thread(merge_dataframes, data_frame, chunk),
                # TODO: use this when min Python version is 3.9
                # asyncio.to_thread(merge_dataframes, data_frame, chunk),
            )

            # we can stop when the chunk stream is exhausted
            if chunk is None:
                break

        return data_frame

    def __replace_http_with_ws(self, url: str) -> str:
        """
        Replace the protocol in the url from `http` to `ws`.

        For the websockets library, it is necessary that the url starts with `ws://`.
        For HTTPS, we need to use `wss://` instead.
        """

        [protocol, url_part] = url.split("://", maxsplit=1)

        ws_prefix = "wss://" if "s" in protocol.lower() else "ws://"

        return f"{ws_prefix}{url_part}"


def register_processing_graph(
    processing_graph: dict[str, Any] | ProcessingGraphBuilderOperator, timeout: int = 60
) -> ProcessingGraph:
    """
    Register a processing graph in Geo Engine and receive a `ProcessingGraph`
    """

    if isinstance(processing_graph, ProcessingGraphBuilderOperator):
        processing_graph = processing_graph.to_processing_graph_dict()

    processing_graph_model = geoc.ProcessingGraph.from_dict(processing_graph)

    if processing_graph_model is None:
        raise InputException("Invalid processing graph definition")

    session = get_session()

    with geoc.ApiClient(session.configuration) as api_client:
        processing_graphs_api = geoc.ProcessingGraphsApi(api_client)
        response = processing_graphs_api.register_processing_graph_handler(
            processing_graph_model, _request_timeout=timeout
        )

    return ProcessingGraph(ProcessingGraphId.from_response(response))


def processing_graph_by_id(processing_graph_id: UUID | str) -> ProcessingGraph:
    """
    Create a processing graph object from a processing graph id
    """

    # TODO: check that processing graph exists

    return ProcessingGraph(ProcessingGraphId(processing_graph_id))


@deprecated("Use `register_processing_graph` instead.", category=DeprecationWarning)
def register_workflow(workflow: dict[str, Any] | ProcessingGraphBuilderOperator, timeout: int = 60) -> ProcessingGraph:
    """Deprecated: use `register_processing_graph` instead."""
    return register_processing_graph(workflow, timeout=timeout)


@deprecated("Use `processing_graph_by_id` instead.", category=DeprecationWarning)
def workflow_by_id(workflow_id: UUID | str) -> ProcessingGraph:
    """Deprecated: use `processing_graph_by_id` instead."""
    return processing_graph_by_id(workflow_id)


def get_quota(user_id: UUID | None = None, timeout: int = 60) -> geoc.Quota:
    """
    Gets a user's quota. Only admins can get other users' quota.
    """

    session = get_session()

    with geoc.ApiClient(session.configuration) as api_client:
        user_api = geoc.UserApi(api_client)

        if user_id is None:
            return user_api.quota_handler(_request_timeout=timeout)

        return user_api.get_user_quota_handler(user_id, _request_timeout=timeout)


def update_quota(user_id: UUID, new_available_quota: int, timeout: int = 60) -> None:
    """
    Update a user's quota. Only admins can perform this operation.
    """

    session = get_session()

    with geoc.ApiClient(session.configuration) as api_client:
        user_api = geoc.UserApi(api_client)
        user_api.update_user_quota_handler(
            user_id, geoc.UpdateQuota(available=new_available_quota), _request_timeout=timeout
        )


def data_usage(offset: int = 0, limit: int = 10) -> list[geoc.DataUsage]:
    """
    Get data usage
    """

    session = get_session()

    with geoc.ApiClient(session.configuration) as api_client:
        user_api = geoc.UserApi(api_client)
        response = user_api.data_usage_handler(offset=offset, limit=limit)

        # create dataframe from response
        usage_dicts = [data_usage.model_dump(by_alias=True) for data_usage in response]
        df = pd.DataFrame(usage_dicts)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    return df


def data_usage_summary(
    granularity: geoc.UsageSummaryGranularity,
    dataset: str | None = None,
    offset: int = 0,
    limit: int = 10,
) -> pd.DataFrame:
    """
    Get data usage summary
    """

    session = get_session()

    with geoc.ApiClient(session.configuration) as api_client:
        user_api = geoc.UserApi(api_client)
        response = user_api.data_usage_summary_handler(
            dataset=dataset, granularity=granularity, offset=offset, limit=limit
        )

        # create dataframe from response
        usage_dicts = [data_usage.model_dump(by_alias=True) for data_usage in response]
        df = pd.DataFrame(usage_dicts)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    return df


def data_usage_for_computation(computation_id: UUID) -> int:
    """
    Get data usage for a specific computation
    """

    session = get_session()

    with geoc.ApiClient(session.configuration) as api_client:
        user_api = geoc.UserApi(api_client)
        response = user_api.computation_quota_handler(computation_id)

    computation_credits = 0

    for operator_quota in response:
        computation_credits += operator_quota.count

    return computation_credits
