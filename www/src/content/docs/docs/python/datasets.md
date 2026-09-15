---
sidebar_label: datasets
title: datasets
---

Module for working with datasets and source definitions

## UnixTimeStampType Objects

```python
class UnixTimeStampType(Enum)
```

A unix time stamp type

## OgrSourceTimeFormat Objects

```python
class OgrSourceTimeFormat()
```

Base class for OGR time formats

## UnixTimeStampOgrSourceTimeFormat Objects

```python
@dataclass
class UnixTimeStampOgrSourceTimeFormat(OgrSourceTimeFormat)
```

An OGR time format specified in seconds (UNIX time)

## AutoOgrSourceTimeFormat Objects

```python
@dataclass
class AutoOgrSourceTimeFormat(OgrSourceTimeFormat)
```

An auto detection OGR time format

## CustomOgrSourceTimeFormat Objects

```python
@dataclass
class CustomOgrSourceTimeFormat(OgrSourceTimeFormat)
```

A custom OGR time format

## OgrSourceDuration Objects

```python
class OgrSourceDuration()
```

Base class for the duration part of a OGR time format

#### value

```python
@classmethod
def value(
    cls,
    value: int,
    granularity: TimeStepGranularity = TimeStepGranularity.SECONDS
) -> ValueOgrSourceDurationSpec
```

Returns the value of the duration

## ValueOgrSourceDurationSpec Objects

```python
class ValueOgrSourceDurationSpec(OgrSourceDuration)
```

A fixed value for a source duration

## ZeroOgrSourceDurationSpec Objects

```python
class ZeroOgrSourceDurationSpec(OgrSourceDuration)
```

An instant, i.e. no duration

## InfiniteOgrSourceDurationSpec Objects

```python
class InfiniteOgrSourceDurationSpec(OgrSourceDuration)
```

An open-ended time duration

## OgrSourceDatasetTimeType Objects

```python
class OgrSourceDatasetTimeType()
```

A time type specification for OGR dataset definitions

#### start

```python
@classmethod
def start(cls, start_field: str, start_format: OgrSourceTimeFormat,
          duration: OgrSourceDuration) -> StartOgrSourceDatasetTimeType
```

Specify a start column and a fixed duration

#### start\_end

```python
@classmethod
def start_end(
        cls, start_field: str, start_format: OgrSourceTimeFormat,
        end_field: str,
        end_format: OgrSourceTimeFormat) -> StartEndOgrSourceDatasetTimeType
```

The dataset contains start and end column

#### start\_duration

```python
@classmethod
def start_duration(
        cls, start_field: str, start_format: OgrSourceTimeFormat,
        duration_field: str) -> StartDurationOgrSourceDatasetTimeType
```

The dataset contains start and a duration column

## NoneOgrSourceDatasetTimeType Objects

```python
@dataclass
class NoneOgrSourceDatasetTimeType(OgrSourceDatasetTimeType)
```

Specify no time information

## StartOgrSourceDatasetTimeType Objects

```python
@dataclass
class StartOgrSourceDatasetTimeType(OgrSourceDatasetTimeType)
```

Specify a start column and a fixed duration

## StartEndOgrSourceDatasetTimeType Objects

```python
@dataclass
class StartEndOgrSourceDatasetTimeType(OgrSourceDatasetTimeType)
```

The dataset contains start and end column

## StartDurationOgrSourceDatasetTimeType Objects

```python
@dataclass
class StartDurationOgrSourceDatasetTimeType(OgrSourceDatasetTimeType)
```

The dataset contains start and a duration column

## OgrOnError Objects

```python
class OgrOnError(Enum)
```

How to handle errors when loading an OGR dataset

## AddDatasetProperties Objects

```python
class AddDatasetProperties()
```

The properties for adding a dataset

#### symbology

TODO: add vector symbology if needed

#### \_\_init\_\_

```python
def __init__(display_name: str,
             description: str,
             source_operator: Literal["GdalSource", "OgrSource",
                                      "MultiBandGdalSource"] = "GdalSource",
             symbology: RasterSymbology | None = None,
             provenance: list[Provenance] | None = None,
             name: str | None = None)
```

Creates a new `AddDatasetProperties` object

#### to\_api\_dict

```python
def to_api_dict() -> geoengine_api_client.AddDataset
```

Converts the properties to a dictionary

## VolumeId Objects

```python
class VolumeId()
```

A wrapper for an volume id

#### \_\_eq\_\_

```python
def __eq__(other) -> bool
```

Checks if two volume ids are equal

#### pandas\_dtype\_to\_column\_type

```python
def pandas_dtype_to_column_type(dtype: np.dtype) -> FeatureDataType
```

Convert a pandas `dtype` to a column type

#### upload\_dataframe

```python
def upload_dataframe(df: gpd.GeoDataFrame,
                     display_name: str = "Upload from Python",
                     name: str | None = None,
                     time: OgrSourceDatasetTimeType | None = None,
                     on_error: OgrOnError = OgrOnError.ABORT,
                     timeout: int = 3600) -> DatasetName
```

Uploads a given dataframe to Geo Engine.

Parameters
----------

df
The dataframe to upload.
display_name
The display name of the dataset. Defaults to &quot;Upload from Python&quot;.
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

## StoredDataset Objects

```python
class StoredDataset(NamedTuple)
```

The result of a store dataset request is a combination of `upload_id` and `dataset_name`

#### from\_response

```python
@classmethod
def from_response(cls, response: api.StoredDataset) -> StoredDataset
```

Parse a http response to an `StoredDataset`

## Volume Objects

```python
@dataclass
class Volume()
```

A volume

#### from\_response

```python
@classmethod
def from_response(cls, response: geoengine_api_client.Volume) -> Volume
```

Parse a http response to an `Volume`

#### volumes

```python
def volumes(timeout: int = 60) -> list[Volume]
```

Returns a list of all volumes

#### volume\_by\_name

```python
def volume_by_name(volume_name: str, timeout: int = 60) -> Volume | None
```

Returns a volume with the specified name or None if none exists

#### add\_dataset

```python
def add_dataset(data_store: Volume | UploadId | Literal["external"],
                properties: AddDatasetProperties,
                meta_data: geoengine_api_client.MetaDataDefinition,
                timeout: int = 60) -> DatasetName
```

Adds a dataset to the Geo Engine

## GdalMultiBandMetaData Objects

```python
class GdalMultiBandMetaData()
```

The metadata (result descriptor) of a `MultiBandGdalSource` dataset

#### \_\_init\_\_

```python
def __init__(bands: list[RasterBandDescriptor],
             data_type: RasterDataType,
             spatial_reference: str,
             grid_or_geo_transform: SpatialGridDescriptor | GeoTransform,
             time: TimeDescriptor | None = None) -> None
```

Create a `GdalMultiBandMetaData` object.

When `time` is not given, a regular time dimension with an epoch origin
and a step of one day is used. When `spatial_grid` is not given, a
placeholder source grid is used; the Geo Engine derives the final grid
when tiles are added to the dataset. The placeholder grid uses the
given `geo_transform` (or a 1 by 1 unit grid), as the tile files&#x27; geo
transforms must be compatible with the dataset grid&#x27;s geo transform.

#### to\_api\_dict

```python
def to_api_dict() -> geoengine_api_client.MetaDataDefinition
```

Converts the metadata to a `MetaDataDefinition` for the API

## MultiBandGdalFileSpec Objects

```python
@dataclass
class MultiBandGdalFileSpec()
```

A single file that is added as a tile to a `MultiBandGdalSource` dataset

#### to\_api\_dict

```python
def to_api_dict() -> geoengine_api_client.AddDatasetTile
```

Converts the file spec to an `AddDatasetTile` for the API

#### add\_dataset\_tiles

```python
def add_dataset_tiles(dataset: DatasetName | str,
                      tiles: list[MultiBandGdalFileSpec],
                      timeout: int = 60) -> None
```

Add files (tiles) to an existing `MultiBandGdalSource` dataset

#### add\_multiband\_gdal\_source

```python
def add_multiband_gdal_source(name: str,
                              bands: list[RasterBandDescriptor],
                              data_type: RasterDataType,
                              spatial_reference: str,
                              files: list[MultiBandGdalFileSpec],
                              data_store: Volume | str = "external",
                              spatial_grid: SpatialGridDescriptor
                              | None = None,
                              time: TimeDescriptor | None = None,
                              display_name: str | None = None,
                              description: str = "",
                              share_with: list[RoleId] | None = None,
                              permission: Permission = Permission.READ,
                              timeout: int = 60) -> DatasetName
```

Create a `MultiBandGdalSource` dataset, grant optional permissions and add the given files as tiles.

By default the dataset is created as external data, so GDAL resolves the
files (e.g. https or s3 links) when they are queried. A volume name or a
`Volume` can be given to store the files in a Geo Engine volume. No
permissions are granted unless `share_with` is given.

#### add\_or\_replace\_dataset\_with\_permissions

```python
def add_or_replace_dataset_with_permissions(
        data_store: Volume | UploadId | Literal["external"],
        properties: AddDatasetProperties,
        meta_data: geoengine_api_client.MetaDataDefinition,
        permission_tuples: list[tuple[RoleId, Permission]] | None = None,
        replace_existing=False,
        timeout: int = 60) -> DatasetName
```

Add a dataset to the Geo Engine and set permissions.
Replaces existing datasets if forced!

#### delete\_dataset

```python
def delete_dataset(dataset_name: DatasetName, timeout: int = 60) -> None
```

Delete a dataset. The dataset must be owned by the caller.

#### list\_datasets\_page

```python
def list_datasets_page(
        offset: int = 0,
        limit: int = 20,
        order: DatasetListOrder = DatasetListOrder.NAME_ASC,
        name_filter: str | None = None,
        timeout: int = 60) -> list[geoengine_api_client.DatasetListing]
```

List datasets

#### list\_datasets

```python
def list_datasets(
        offset: int = 0,
        limit: int = 200,
        order: DatasetListOrder = DatasetListOrder.NAME_ASC,
        name_filter: str | None = None,
        timeout: int = 60) -> Iterator[geoengine_api_client.DatasetListing]
```

List datasets

#### dataset\_info\_by\_name

```python
def dataset_info_by_name(
        dataset_name: DatasetName | str,
        timeout: int = 60) -> geoengine_api_client.models.Dataset | None
```

Get dataset information.

#### dataset\_metadata\_by\_name

```python
def dataset_metadata_by_name(
    dataset_name: DatasetName | str,
    timeout: int = 60
) -> geoengine_api_client.models.MetaDataDefinition | None
```

Get dataset information.
