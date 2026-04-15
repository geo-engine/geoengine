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

#### start_end

```python
@classmethod
def start_end(
        cls, start_field: str, start_format: OgrSourceTimeFormat,
        end_field: str,
        end_format: OgrSourceTimeFormat) -> StartEndOgrSourceDatasetTimeType
```

The dataset contains start and end column

#### start_duration

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
             source_operator: Literal["GdalSource",
                                      "OgrSource"] = "GdalSource",
             symbology: RasterSymbology | None = None,
             provenance: list[Provenance] | None = None,
             name: str | None = None)
```

Creates a new `AddDatasetProperties` object

#### to_api_dict

```python
def to_api_dict() -> geoengine_openapi_client.AddDataset
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

#### pandas_dtype_to_column_type

```python
def pandas_dtype_to_column_type(dtype: np.dtype) -> FeatureDataType
```

Convert a pandas `dtype` to a column type

#### upload_dataframe

```python
def upload_dataframe(df: gpd.GeoDataFrame,
                     display_name: str = "Upload from Python",
                     name: str | None = None,
                     time: OgrSourceDatasetTimeType | None = None,
                     on_error: OgrOnError = OgrOnError.ABORT,
                     timeout: int = 3600) -> DatasetName
```

Uploads a given dataframe to Geo Engine.

## Parameters

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

## Returns

DatasetName
The name of the uploaded dataset

## Raises

GeoEngineException
If the dataset could not be uploaded or the name is already taken.

## StoredDataset Objects

```python
class StoredDataset(NamedTuple)
```

The result of a store dataset request is a combination of `upload_id` and `dataset_name`

#### from_response

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

#### from_response

```python
@classmethod
def from_response(cls, response: geoengine_openapi_client.Volume) -> Volume
```

Parse a http response to an `Volume`

#### volumes

```python
def volumes(timeout: int = 60) -> list[Volume]
```

Returns a list of all volumes

#### volume_by_name

```python
def volume_by_name(volume_name: str, timeout: int = 60) -> Volume | None
```

Returns a volume with the specified name or None if none exists

#### add_dataset

```python
def add_dataset(data_store: Volume | UploadId,
                properties: AddDatasetProperties,
                meta_data: geoengine_openapi_client.MetaDataDefinition,
                timeout: int = 60) -> DatasetName
```

Adds a dataset to the Geo Engine

#### add_or_replace_dataset_with_permissions

```python
def add_or_replace_dataset_with_permissions(
        data_store: Volume | UploadId,
        properties: AddDatasetProperties,
        meta_data: geoengine_openapi_client.MetaDataDefinition,
        permission_tuples: list[tuple[RoleId, Permission]] | None = None,
        replace_existing=False,
        timeout: int = 60) -> DatasetName
```

Add a dataset to the Geo Engine and set permissions.
Replaces existing datasets if forced!

#### delete_dataset

```python
def delete_dataset(dataset_name: DatasetName, timeout: int = 60) -> None
```

Delete a dataset. The dataset must be owned by the caller.

#### list_datasets_page

```python
def list_datasets_page(
        offset: int = 0,
        limit: int = 20,
        order: DatasetListOrder = DatasetListOrder.NAME_ASC,
        name_filter: str | None = None,
        timeout: int = 60) -> list[geoengine_openapi_client.DatasetListing]
```

List datasets

#### list_datasets

```python
def list_datasets(
        offset: int = 0,
        limit: int = 200,
        order: DatasetListOrder = DatasetListOrder.NAME_ASC,
        name_filter: str | None = None,
        timeout: int = 60
) -> Iterator[geoengine_openapi_client.DatasetListing]
```

List datasets

#### dataset_info_by_name

```python
def dataset_info_by_name(
        dataset_name: DatasetName | str,
        timeout: int = 60) -> geoengine_openapi_client.models.Dataset | None
```

Get dataset information.

#### dataset_metadata_by_name

```python
def dataset_metadata_by_name(
    dataset_name: DatasetName | str,
    timeout: int = 60
) -> geoengine_openapi_client.models.MetaDataDefinition | None
```

Get dataset information.
