"""
Module for working with datasets and source definitions
"""

from __future__ import annotations

import tempfile
from abc import abstractmethod
from collections.abc import Iterator
from enum import Enum
from pathlib import Path
from typing import Literal, NamedTuple
from uuid import UUID

import geoengine_api_client
import geoengine_api_client.exceptions
import geoengine_api_client.models
import geopandas as gpd
import numpy as np
from attr import dataclass

from geoengine import api
from geoengine.auth import get_session
from geoengine.error import InputException, MissingFieldInResponseException
from geoengine.permissions import Permission, RoleId, add_permission
from geoengine.resource_identifier import DatasetName, Resource, UploadId
from geoengine.types import (
    FeatureDataType,
    GeoTransform,
    GridBoundingBox2D,
    GridIdx2D,
    Provenance,
    RasterBandDescriptor,
    RasterDataType,
    RasterSymbology,
    RegularTimeDimension,
    SpatialGridDefinition,
    SpatialGridDescriptor,
    SpatialPartition2D,
    TimeDescriptor,
    TimeInterval,
    TimeStep,
    TimeStepGranularity,
    UnitlessMeasurement,
    VectorColumnInfo,
    VectorDataType,
    VectorResultDescriptor,
)


class UnixTimeStampType(Enum):
    """A unix time stamp type"""

    EPOCHSECONDS = "epochSeconds"
    EPOCHMILLISECONDS = "epochMilliseconds"

    def to_api_enum(self) -> geoengine_api_client.UnixTimeStampType:
        return geoengine_api_client.UnixTimeStampType(self.value)


class OgrSourceTimeFormat:
    """Base class for OGR time formats"""

    @abstractmethod
    def to_api_dict(self) -> geoengine_api_client.OgrSourceTimeFormat:
        pass

    @classmethod
    def seconds(cls, timestamp_type: UnixTimeStampType) -> UnixTimeStampOgrSourceTimeFormat:
        return UnixTimeStampOgrSourceTimeFormat(timestamp_type)

    @classmethod
    def auto(cls) -> AutoOgrSourceTimeFormat:
        return AutoOgrSourceTimeFormat()

    @classmethod
    def custom(cls, format_string: str) -> CustomOgrSourceTimeFormat:
        return CustomOgrSourceTimeFormat(format_string)


@dataclass
class UnixTimeStampOgrSourceTimeFormat(OgrSourceTimeFormat):
    """An OGR time format specified in seconds (UNIX time)"""

    timestampType: UnixTimeStampType

    def to_api_dict(self) -> geoengine_api_client.OgrSourceTimeFormat:
        return geoengine_api_client.OgrSourceTimeFormat(
            geoengine_api_client.OgrSourceTimeFormatUnixTimeStamp(
                format="unixTimeStamp",
                timestamp_type=self.timestampType.to_api_enum(),
            )
        )


@dataclass
class AutoOgrSourceTimeFormat(OgrSourceTimeFormat):
    """An auto detection OGR time format"""

    def to_api_dict(self) -> geoengine_api_client.OgrSourceTimeFormat:
        return geoengine_api_client.OgrSourceTimeFormat(geoengine_api_client.OgrSourceTimeFormatAuto(format="auto"))


@dataclass
class CustomOgrSourceTimeFormat(OgrSourceTimeFormat):
    """A custom OGR time format"""

    custom_format: str

    def to_api_dict(self) -> geoengine_api_client.OgrSourceTimeFormat:
        return geoengine_api_client.OgrSourceTimeFormat(
            geoengine_api_client.OgrSourceTimeFormatCustom(format="custom", custom_format=self.custom_format)
        )


class OgrSourceDuration:
    """Base class for the duration part of a OGR time format"""

    @abstractmethod
    def to_api_dict(self) -> geoengine_api_client.OgrSourceDurationSpec:
        pass

    @classmethod
    def zero(cls) -> ZeroOgrSourceDurationSpec:
        return ZeroOgrSourceDurationSpec()

    @classmethod
    def infinite(cls) -> InfiniteOgrSourceDurationSpec:
        return InfiniteOgrSourceDurationSpec()

    @classmethod
    def value(
        cls, value: int, granularity: TimeStepGranularity = TimeStepGranularity.SECONDS
    ) -> ValueOgrSourceDurationSpec:
        """Returns the value of the duration"""
        return ValueOgrSourceDurationSpec(TimeStep(value, granularity))


class ValueOgrSourceDurationSpec(OgrSourceDuration):
    """A fixed value for a source duration"""

    step: TimeStep

    def __init__(self, step: TimeStep):
        self.step = step

    def to_api_dict(self) -> geoengine_api_client.OgrSourceDurationSpec:
        return geoengine_api_client.OgrSourceDurationSpec(
            geoengine_api_client.OgrSourceDurationSpecValue(
                type="value",
                step=self.step.step,
                granularity=self.step.granularity.to_api_enum(),
            )
        )


class ZeroOgrSourceDurationSpec(OgrSourceDuration):
    """An instant, i.e. no duration"""

    def to_api_dict(self) -> geoengine_api_client.OgrSourceDurationSpec:
        return geoengine_api_client.OgrSourceDurationSpec(
            geoengine_api_client.OgrSourceDurationSpecZero(
                type="zero",
            )
        )


class InfiniteOgrSourceDurationSpec(OgrSourceDuration):
    """An open-ended time duration"""

    def to_api_dict(self) -> geoengine_api_client.OgrSourceDurationSpec:
        return geoengine_api_client.OgrSourceDurationSpec(
            geoengine_api_client.OgrSourceDurationSpecInfinite(
                type="infinite",
            )
        )


class OgrSourceDatasetTimeType:
    """A time type specification for OGR dataset definitions"""

    @abstractmethod
    def to_api_dict(self) -> geoengine_api_client.OgrSourceDatasetTimeType:
        pass

    @classmethod
    def none(cls) -> NoneOgrSourceDatasetTimeType:
        return NoneOgrSourceDatasetTimeType()

    @classmethod
    def start(
        cls, start_field: str, start_format: OgrSourceTimeFormat, duration: OgrSourceDuration
    ) -> StartOgrSourceDatasetTimeType:
        """Specify a start column and a fixed duration"""
        return StartOgrSourceDatasetTimeType(start_field, start_format, duration)

    @classmethod
    def start_end(
        cls, start_field: str, start_format: OgrSourceTimeFormat, end_field: str, end_format: OgrSourceTimeFormat
    ) -> StartEndOgrSourceDatasetTimeType:
        """The dataset contains start and end column"""
        return StartEndOgrSourceDatasetTimeType(start_field, start_format, end_field, end_format)

    @classmethod
    def start_duration(
        cls, start_field: str, start_format: OgrSourceTimeFormat, duration_field: str
    ) -> StartDurationOgrSourceDatasetTimeType:
        """The dataset contains start and a duration column"""
        return StartDurationOgrSourceDatasetTimeType(start_field, start_format, duration_field)


@dataclass
class NoneOgrSourceDatasetTimeType(OgrSourceDatasetTimeType):
    """Specify no time information"""

    def to_api_dict(self) -> geoengine_api_client.OgrSourceDatasetTimeType:
        return geoengine_api_client.OgrSourceDatasetTimeType(
            geoengine_api_client.OgrSourceDatasetTimeTypeNone(
                type="none",
            )
        )


@dataclass
class StartOgrSourceDatasetTimeType(OgrSourceDatasetTimeType):
    """Specify a start column and a fixed duration"""

    start_field: str
    start_format: OgrSourceTimeFormat
    duration: OgrSourceDuration

    def to_api_dict(self) -> geoengine_api_client.OgrSourceDatasetTimeType:
        return geoengine_api_client.OgrSourceDatasetTimeType(
            geoengine_api_client.OgrSourceDatasetTimeTypeStart(
                type="start",
                start_field=self.start_field,
                start_format=self.start_format.to_api_dict(),
                duration=self.duration.to_api_dict(),
            )
        )


@dataclass
class StartEndOgrSourceDatasetTimeType(OgrSourceDatasetTimeType):
    """The dataset contains start and end column"""

    start_field: str
    start_format: OgrSourceTimeFormat
    end_field: str
    end_format: OgrSourceTimeFormat

    def to_api_dict(self) -> geoengine_api_client.OgrSourceDatasetTimeType:
        return geoengine_api_client.OgrSourceDatasetTimeType(
            geoengine_api_client.OgrSourceDatasetTimeTypeStartEnd(
                type="startEnd",
                start_field=self.start_field,
                start_format=self.start_format.to_api_dict(),
                end_field=self.end_field,
                end_format=self.end_format.to_api_dict(),
            )
        )


@dataclass
class StartDurationOgrSourceDatasetTimeType(OgrSourceDatasetTimeType):
    """The dataset contains start and a duration column"""

    start_field: str
    start_format: OgrSourceTimeFormat
    duration_field: str

    def to_api_dict(self) -> geoengine_api_client.OgrSourceDatasetTimeType:
        return geoengine_api_client.OgrSourceDatasetTimeType(
            geoengine_api_client.OgrSourceDatasetTimeTypeStartDuration(
                type="startDuration",
                start_field=self.start_field,
                start_format=self.start_format.to_api_dict(),
                duration_field=self.duration_field,
            )
        )


class OgrOnError(Enum):
    """How to handle errors when loading an OGR dataset"""

    IGNORE = "ignore"
    ABORT = "abort"

    def to_api_enum(self) -> geoengine_api_client.OgrSourceErrorSpec:
        return geoengine_api_client.OgrSourceErrorSpec(self.value)


class AddDatasetProperties:
    """The properties for adding a dataset"""

    name: str | None
    display_name: str
    description: str
    # TODO: add more operators
    source_operator: Literal["GdalSource", "OgrSource", "MultiBandGdalSource"]
    symbology: RasterSymbology | None  # TODO: add vector symbology if needed
    provenance: list[Provenance] | None

    def __init__(
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        display_name: str,
        description: str,
        source_operator: Literal["GdalSource", "OgrSource", "MultiBandGdalSource"] = "GdalSource",
        symbology: RasterSymbology | None = None,
        provenance: list[Provenance] | None = None,
        name: str | None = None,
    ):
        """Creates a new `AddDatasetProperties` object"""
        self.name = name
        self.display_name = display_name
        self.description = description
        self.source_operator = source_operator
        self.symbology = symbology
        self.provenance = provenance

    def to_api_dict(self) -> geoengine_api_client.AddDataset:
        """Converts the properties to a dictionary"""
        return geoengine_api_client.AddDataset(
            name=str(self.name) if self.name is not None else None,
            display_name=self.display_name,
            description=self.description,
            source_operator=self.source_operator,
            symbology=self.symbology.to_api_dict() if self.symbology is not None else None,
            provenance=[p.to_api_dict() for p in self.provenance] if self.provenance is not None else None,
        )


class VolumeId:
    """A wrapper for an volume id"""

    __volume_id: UUID

    def __init__(self, volume_id: UUID) -> None:
        self.__volume_id = volume_id

    def __str__(self) -> str:
        return str(self.__volume_id)

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other) -> bool:
        """Checks if two volume ids are equal"""
        if not isinstance(other, self.__class__):
            return False

        return self.__volume_id == other.__volume_id  # pylint: disable=protected-access


def pandas_dtype_to_column_type(dtype: np.dtype) -> FeatureDataType:
    """Convert a pandas `dtype` to a column type"""

    if str(dtype) in ["object", "str"]:
        return FeatureDataType.TEXT

    if np.issubdtype(dtype, np.integer):
        return FeatureDataType.INT

    if np.issubdtype(dtype, np.floating):
        return FeatureDataType.FLOAT

    raise InputException(f"pandas dtype {dtype} has no corresponding column type")


def upload_dataframe(
    df: gpd.GeoDataFrame,
    display_name: str = "Upload from Python",
    name: str | None = None,
    time: OgrSourceDatasetTimeType | None = None,
    on_error: OgrOnError = OgrOnError.ABORT,
    timeout: int = 3600,
) -> DatasetName:
    """
    Uploads a given dataframe to Geo Engine.

    Parameters
    ----------
    df
        The dataframe to upload.
    display_name
        The display name of the dataset. Defaults to "Upload from Python".
    name
        The name the dataset should have. If not given, a random name (UUID) will be generated.
    time
        A time configuration for the dataset. Defaults to `OgrSourceDatasetTimeType.none()`.
    on_error
        The error handling strategy. Defaults to `OgrOnError.ABORT`.
    timeout
        The upload timeout in seconds. Defaults to 3600.

    Returns
    -------
    DatasetName
        The name of the uploaded dataset

    Raises
    ------
    GeoEngineException
        If the dataset could not be uploaded or the name is already taken.
    """
    # pylint: disable=too-many-arguments,too-many-locals,too-many-positional-arguments

    if time is None:
        time = OgrSourceDatasetTimeType.none()

    if len(df) == 0:
        raise InputException("Cannot upload empty dataframe")

    if df.crs is None:
        raise InputException("Dataframe must have a specified crs")

    session = get_session()

    df_json = df.to_json()

    with (
        tempfile.TemporaryDirectory() as temp_dir,
        geoengine_api_client.ApiClient(session.configuration) as api_client,
    ):
        json_file_name = Path(temp_dir) / "geo.json"
        with open(json_file_name, "w", encoding="utf8") as json_file:
            json_file.write(df_json)

        uploads_api = geoengine_api_client.UploadsApi(api_client)
        response = uploads_api.upload_handler([str(json_file_name)], _request_timeout=timeout)

    upload_id = UploadId.from_response(response)

    vector_type = VectorDataType.from_geopandas_type_name(df.geom_type[0])

    columns = {
        key: VectorColumnInfo(data_type=pandas_dtype_to_column_type(value), measurement=UnitlessMeasurement())
        for (key, value) in df.dtypes.items()
        if str(value) != "geometry"
    }

    floats = [key for (key, value) in columns.items() if value.data_type == "float"]
    ints = [key for (key, value) in columns.items() if value.data_type == "int"]
    texts = [key for (key, value) in columns.items() if value.data_type == "text"]

    result_descriptor = (
        VectorResultDescriptor(
            data_type=vector_type,
            spatial_reference=df.crs.to_string(),
            columns=columns,
        )
        .to_api_dict()
        .actual_instance
    )
    if not isinstance(result_descriptor, geoengine_api_client.TypedVectorResultDescriptor):
        raise TypeError("Expected TypedVectorResultDescriptor")

    create = geoengine_api_client.CreateDataset(
        data_path=geoengine_api_client.DataPath(geoengine_api_client.DataPathUpload(upload=str(upload_id))),
        definition=geoengine_api_client.DatasetDefinition(
            properties=AddDatasetProperties(
                display_name=display_name,
                name=name,
                description="Upload from Python",
                source_operator="OgrSource",
            ).to_api_dict(),
            meta_data=geoengine_api_client.MetaDataDefinition(
                geoengine_api_client.OgrMetaData(
                    type="OgrMetaData",
                    loading_info=geoengine_api_client.OgrSourceDataset(
                        file_name="geo.json",
                        layer_name="geo",
                        data_type=vector_type.to_api_enum(),
                        time=time.to_api_dict(),
                        columns=geoengine_api_client.OgrSourceColumnSpec(
                            y="",
                            x="",
                            float=floats,
                            int=ints,
                            text=texts,
                        ),
                        on_error=on_error.to_api_enum(),
                    ),
                    result_descriptor=geoengine_api_client.VectorResultDescriptor.from_dict(
                        result_descriptor.to_dict()
                    ),
                )
            ),
        ),
    )

    with geoengine_api_client.ApiClient(session.configuration) as api_client:
        datasets_api = geoengine_api_client.DatasetsApi(api_client)
        response2 = datasets_api.create_dataset_handler(create, _request_timeout=timeout)

    return DatasetName.from_response(response2)


class StoredDataset(NamedTuple):
    """The result of a store dataset request is a combination of `upload_id` and `dataset_name`"""

    dataset_name: DatasetName
    upload_id: UploadId

    @classmethod
    def from_response(cls, response: api.StoredDataset) -> StoredDataset:
        """Parse a http response to an `StoredDataset`"""

        if "dataset" not in response:  # TODO: improve error handling
            raise MissingFieldInResponseException("dataset", response)
        if "upload" not in response:
            raise MissingFieldInResponseException("upload", response)

        return StoredDataset(
            dataset_name=DatasetName(response["dataset"]), upload_id=UploadId(UUID(response["upload"]))
        )

    def to_api_dict(self) -> api.StoredDataset:
        return api.StoredDataset(dataset=str(self.dataset_name), upload=str(self.upload_id))


@dataclass
class Volume:
    """A volume"""

    name: str
    path: str | None

    @classmethod
    def from_response(cls, response: geoengine_api_client.Volume) -> Volume:
        """Parse a http response to an `Volume`"""
        return Volume(response.name, response.path)

    def to_api_dict(self) -> geoengine_api_client.Volume:
        return geoengine_api_client.Volume(name=self.name, path=self.path)


def volumes(timeout: int = 60) -> list[Volume]:
    """Returns a list of all volumes"""

    session = get_session()

    with geoengine_api_client.ApiClient(session.configuration) as api_client:
        datasets_api = geoengine_api_client.DatasetsApi(api_client)
        response = datasets_api.list_volumes_handler(_request_timeout=timeout)

    return [Volume.from_response(v) for v in response]


def volume_by_name(volume_name: str, timeout: int = 60) -> Volume | None:
    """Returns a volume with the specified name or None if none exists"""
    vols = volumes(timeout)
    vols = [v for v in vols if v.name == volume_name]

    if len(vols) == 0:
        return None

    if len(vols) > 1:
        raise KeyError(f"Volume name {volume_name} is not unique")

    return vols[0]


def add_dataset(
    data_store: Volume | UploadId | Literal["external"],
    properties: AddDatasetProperties,
    meta_data: geoengine_api_client.MetaDataDefinition,
    timeout: int = 60,
) -> DatasetName:
    """Adds a dataset to the Geo Engine"""

    if isinstance(data_store, Volume):
        dataset_path = geoengine_api_client.DataPath(geoengine_api_client.DataPathVolume(volume=data_store.name))
    elif isinstance(data_store, str) and data_store == "external":
        dataset_path = geoengine_api_client.DataPath(data_store)
    else:
        dataset_path = geoengine_api_client.DataPath(geoengine_api_client.DataPathUpload(upload=str(data_store)))

    create = geoengine_api_client.CreateDataset(
        data_path=dataset_path,
        definition=geoengine_api_client.DatasetDefinition(properties=properties.to_api_dict(), meta_data=meta_data),
    )

    session = get_session()

    with geoengine_api_client.ApiClient(session.configuration) as api_client:
        datasets_api = geoengine_api_client.DatasetsApi(api_client)
        response = datasets_api.create_dataset_handler(create, _request_timeout=timeout)

    return DatasetName.from_response(response)


class GdalMultiBandMetaData:
    """The metadata (result descriptor) of a `MultiBandGdalSource` dataset"""

    bands: list[RasterBandDescriptor]
    data_type: RasterDataType
    spatial_reference: str
    time: TimeDescriptor
    spatial_grid: SpatialGridDescriptor

    def __init__(
        self,
        bands: list[RasterBandDescriptor],
        data_type: RasterDataType,
        spatial_reference: str,
        grid_or_geo_transform: SpatialGridDescriptor | GeoTransform,
        time: TimeDescriptor | None = None,
    ) -> None:
        """
        Create a `GdalMultiBandMetaData` object.

        When `time` is not given, a regular time dimension with an epoch origin
        and a step of one day is used. When `spatial_grid` is not given, a
        placeholder source grid is used; the Geo Engine derives the final grid
        when tiles are added to the dataset. The placeholder grid uses the
        given `geo_transform` (or a 1 by 1 unit grid), as the tile files' geo
        transforms must be compatible with the dataset grid's geo transform.
        """
        if time is None:
            time = TimeDescriptor(
                RegularTimeDimension(step=TimeStep(1, TimeStepGranularity.DAYS)),
            )
        if isinstance(grid_or_geo_transform, SpatialGridDescriptor):
            self.spatial_grid = grid_or_geo_transform
        else:
            if not isinstance(grid_or_geo_transform, GeoTransform):
                raise TypeError("grid_or_geo_transform must be a SpatialGridDescriptor or GeoTransform")
            spatial_grid = SpatialGridDescriptor(
                SpatialGridDefinition(
                    geo_transform=grid_or_geo_transform,
                    grid_bounds=GridBoundingBox2D(
                        top_left_idx=GridIdx2D(x_idx=0, y_idx=0),
                        bottom_right_idx=GridIdx2D(x_idx=1, y_idx=1),
                    ),
                ),
                descriptor=geoengine_api_client.SpatialGridDescriptorState.SOURCE,
            )
            self.spatial_grid = spatial_grid
        self.bands = bands
        self.data_type = data_type
        self.spatial_reference = spatial_reference
        self.time = time

    def to_api_dict(self) -> geoengine_api_client.MetaDataDefinition:
        """Converts the metadata to a `MetaDataDefinition` for the API"""
        return geoengine_api_client.MetaDataDefinition(
            geoengine_api_client.GdalMultiBand(
                type="GdalMultiBand",
                result_descriptor=geoengine_api_client.RasterResultDescriptor(
                    data_type=geoengine_api_client.RasterDataType(self.data_type.value),
                    spatial_reference=self.spatial_reference,
                    time=self.time.to_api_dict(),
                    spatial_grid=self.spatial_grid.to_api_dict(),
                    bands=[band.to_api_dict() for band in self.bands],
                ),
            )
        )


@dataclass
class MultiBandGdalFileSpec:
    """A single file that is added as a tile to a `MultiBandGdalSource` dataset"""

    file_path: str
    time: TimeInterval
    spatial_partition: SpatialPartition2D
    band: int
    width: int
    height: int
    geo_transform: GeoTransform
    channel: int = 1
    z_index: int = 0
    no_data_value: float | None = None
    gdal_open_options: list[str] | None = None
    gdal_config_options: list[tuple[str, str]] | None = None
    allow_alphaband_as_mask: bool = False
    file_not_found_handling: Literal["NoData", "Error"] = "Error"

    def to_api_dict(self) -> geoengine_api_client.AddDatasetTile:
        """Converts the file spec to an `AddDatasetTile` for the API"""
        return geoengine_api_client.AddDatasetTile(
            time=self.time.to_api_dict(),
            spatial_partition=self.spatial_partition.to_api_dict(),
            band=self.band,
            z_index=self.z_index,
            params=geoengine_api_client.GdalDatasetParameters(
                file_path=self.file_path,
                rasterband_channel=self.channel,
                geo_transform=self.geo_transform.to_api_dict(),
                width=self.width,
                height=self.height,
                file_not_found_handling=geoengine_api_client.FileNotFoundHandling(self.file_not_found_handling),
                no_data_value=self.no_data_value,
                gdal_open_options=self.gdal_open_options,
                gdal_config_options=(
                    [list(option) for option in self.gdal_config_options]
                    if self.gdal_config_options is not None
                    else None
                ),
                allow_alphaband_as_mask=self.allow_alphaband_as_mask,
            ),
        )


def add_dataset_tiles(dataset: DatasetName | str, tiles: list[MultiBandGdalFileSpec], timeout: int = 60) -> None:
    """Add files (tiles) to an existing `MultiBandGdalSource` dataset"""

    if not isinstance(dataset, DatasetName):
        dataset = DatasetName(dataset)

    session = get_session()

    with geoengine_api_client.ApiClient(session.configuration) as api_client:
        datasets_api = geoengine_api_client.DatasetsApi(api_client)
        datasets_api.add_dataset_tiles_handler(
            str(dataset),
            [tile.to_api_dict() for tile in tiles],
            _request_timeout=timeout,
        )


def add_multiband_gdal_source(
    name: str,
    bands: list[RasterBandDescriptor],
    data_type: RasterDataType,
    spatial_reference: str,
    files: list[MultiBandGdalFileSpec],
    data_store: Volume | str = "external",
    spatial_grid: SpatialGridDescriptor | None = None,
    time: TimeDescriptor | None = None,
    display_name: str | None = None,
    description: str = "",
    share_with: list[RoleId] | None = None,
    permission: Permission = Permission.READ,
    timeout: int = 60,
) -> DatasetName:
    """
    Create a `MultiBandGdalSource` dataset, grant optional permissions and add the given files as tiles.

    By default the dataset is created as external data, so GDAL resolves the
    files (e.g. https or s3 links) when they are queried. A volume name or a
    `Volume` can be given to store the files in a Geo Engine volume. No
    permissions are granted unless `share_with` is given.
    """
    # pylint: disable=too-many-arguments,too-many-positional-arguments

    resolved_data_store: Volume | UploadId | Literal["external"]
    if isinstance(data_store, str):
        if data_store == "external":
            resolved_data_store = "external"
        else:
            volume = volume_by_name(data_store, timeout=timeout)
            if volume is None:
                raise ValueError(f"Volume '{data_store}' not found")
            resolved_data_store = volume
    else:
        resolved_data_store = data_store

    properties = AddDatasetProperties(
        display_name=display_name if display_name is not None else name,
        description=description,
        source_operator="MultiBandGdalSource",
        name=name,
    )

    grid_or_geo_transform: SpatialGridDescriptor | GeoTransform
    if spatial_grid is not None:
        grid_or_geo_transform = spatial_grid
    elif files:
        grid_or_geo_transform = files[0].geo_transform
    else:
        raise ValueError("Either spatial_grid or files must be provided")

    meta_data = GdalMultiBandMetaData(
        bands=bands,
        data_type=data_type,
        spatial_reference=spatial_reference,
        time=time,
        grid_or_geo_transform=grid_or_geo_transform,
    ).to_api_dict()

    dataset_name = add_dataset(
        data_store=resolved_data_store, properties=properties, meta_data=meta_data, timeout=timeout
    )

    if share_with is not None:
        dataset_resource = Resource.from_dataset_name(dataset_name)
        for role in share_with:
            add_permission(role, dataset_resource, permission, timeout=timeout)

    if files:
        add_dataset_tiles(dataset_name, files, timeout=timeout)

    return dataset_name


def add_or_replace_dataset_with_permissions(
    data_store: Volume | UploadId | Literal["external"],
    properties: AddDatasetProperties,
    meta_data: geoengine_api_client.MetaDataDefinition,
    permission_tuples: list[tuple[RoleId, Permission]] | None = None,
    replace_existing=False,
    timeout: int = 60,
) -> DatasetName:
    """
    Add a dataset to the Geo Engine and set permissions.
    Replaces existing datasets if forced!
    """
    # pylint: disable=too-many-arguments,too-many-positional-arguments

    def add_dataset_and_permissions() -> DatasetName:
        dataset_name = add_dataset(data_store=data_store, properties=properties, meta_data=meta_data, timeout=timeout)
        if permission_tuples is not None:
            dataset_res = Resource.from_dataset_name(dataset_name)
            for role, perm in permission_tuples:
                add_permission(role, dataset_res, perm, timeout=timeout)
        return dataset_name

    if properties.name is None:
        dataset_name = add_dataset_and_permissions()

    else:
        dataset_name = DatasetName(properties.name)
        dataset_info = dataset_info_by_name(dataset_name)

        if dataset_info is None:  # dataset is not existing or can't read
            dataset_name = add_dataset_and_permissions()
        else:
            if replace_existing:  # dataset exists and we overwrite it
                delete_dataset(dataset_name)
                dataset_name = add_dataset_and_permissions()

    return dataset_name


def delete_dataset(dataset_name: DatasetName, timeout: int = 60) -> None:
    """Delete a dataset. The dataset must be owned by the caller."""

    session = get_session()

    with geoengine_api_client.ApiClient(session.configuration) as api_client:
        datasets_api = geoengine_api_client.DatasetsApi(api_client)
        datasets_api.delete_dataset_handler(str(dataset_name), _request_timeout=timeout)


class DatasetListOrder(Enum):
    NAME_ASC = "NameAsc"
    NAME_DESC = "NameDesc"


def list_datasets_page(
    offset: int = 0,
    limit: int = 20,
    order: DatasetListOrder = DatasetListOrder.NAME_ASC,
    name_filter: str | None = None,
    timeout: int = 60,
) -> list[geoengine_api_client.DatasetListing]:
    """List datasets"""

    session = get_session()

    with geoengine_api_client.ApiClient(session.configuration) as api_client:
        datasets_api = geoengine_api_client.DatasetsApi(api_client)
        response = datasets_api.list_datasets_handler(
            offset=offset,
            limit=limit,
            order=geoengine_api_client.OrderBy(order.value),
            filter=name_filter,
            _request_timeout=timeout,
        )

    return response


def list_datasets(
    offset: int = 0,
    limit: int = 200,
    order: DatasetListOrder = DatasetListOrder.NAME_ASC,
    name_filter: str | None = None,
    timeout: int = 60,
) -> Iterator[geoengine_api_client.DatasetListing]:
    """List datasets"""

    page_size = 20
    page_count = 0

    while True:
        element_num = page_size * page_count

        if element_num >= limit:
            break

        page = list_datasets_page(
            element_num + offset, page_size, order=order, name_filter=name_filter, timeout=timeout
        )
        page_count += 1

        if len(page) == 0:
            break

        for c, p in enumerate(page):
            if element_num + c > limit:
                break
            yield p


def dataset_info_by_name(
    dataset_name: DatasetName | str, timeout: int = 60
) -> geoengine_api_client.models.Dataset | None:
    """Get dataset information."""

    if not isinstance(dataset_name, DatasetName):
        dataset_name = DatasetName(dataset_name)

    session = get_session()

    with geoengine_api_client.ApiClient(session.configuration) as api_client:
        datasets_api = geoengine_api_client.DatasetsApi(api_client)
        res = None
        try:
            res = datasets_api.get_dataset_handler(str(dataset_name), _request_timeout=timeout)
        except geoengine_api_client.exceptions.BadRequestException as e:
            e_body = e.body
            if isinstance(e_body, str) and "CannotLoadDataset" not in e_body:
                raise e
        return res


def dataset_metadata_by_name(
    dataset_name: DatasetName | str, timeout: int = 60
) -> geoengine_api_client.models.MetaDataDefinition | None:
    """Get dataset information."""

    if not isinstance(dataset_name, DatasetName):
        dataset_name = DatasetName(dataset_name)

    session = get_session()

    with geoengine_api_client.ApiClient(session.configuration) as api_client:
        datasets_api = geoengine_api_client.DatasetsApi(api_client)
        res = None
        try:
            res = datasets_api.get_loading_info_handler(str(dataset_name), _request_timeout=timeout)
        except geoengine_api_client.exceptions.BadRequestException as e:
            e_body = e.body
            if isinstance(e_body, str) and "CannotLoadDataset" not in e_body:
                raise e
        return res
