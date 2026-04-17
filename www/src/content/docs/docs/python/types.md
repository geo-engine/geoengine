---
sidebar_label: types
title: types
---

Different type mappings of geo engine types

## SpatialBounds Objects

```python
class SpatialBounds()
```

A spatial bounds object

#### \_\_init\_\_

```python
def __init__(xmin: float, ymin: float, xmax: float, ymax: float) -> None
```

Initialize a new `SpatialBounds` object

#### as_bbox_str

```python
def as_bbox_str(y_axis_first=False) -> str
```

A comma-separated string representation of the spatial bounds with OGC axis ordering

#### as_bbox_tuple

```python
def as_bbox_tuple(y_axis_first=False) -> tuple[float, float, float, float]
```

Return the bbox with OGC axis ordering of the srs

#### x_axis_size

```python
def x_axis_size() -> float
```

The size of the x axis

#### y_axis_size

```python
def y_axis_size() -> float
```

The size of the y axis

## BoundingBox2D Objects

```python
class BoundingBox2D(SpatialBounds)
```

&#x27;A 2D bounding box.

#### from_response

```python
@staticmethod
def from_response(
        response: geoengine_api_client.BoundingBox2D) -> BoundingBox2D
```

create a `BoundingBox2D` from an API response

## SpatialPartition2D Objects

```python
class SpatialPartition2D(SpatialBounds)
```

A 2D spatial partition.

#### from_response

```python
@staticmethod
def from_response(
        response: geoengine_api_client.SpatialPartition2D
) -> SpatialPartition2D
```

create a `SpatialPartition2D` from an API response

#### to_bounding_box

```python
def to_bounding_box() -> BoundingBox2D
```

convert to a `BoundingBox2D`

#### from_bounding_box

```python
@staticmethod
def from_bounding_box(bbox: BoundingBox2D) -> SpatialPartition2D
```

Creates a `SpatialPartition2D` from a `BoundingBox2D`

## TimeInterval Objects

```python
class TimeInterval()
```

&#x27;A time interval.

#### \_\_init\_\_

```python
def __init__(start: datetime | np.datetime64,
             end: datetime | np.datetime64 | None = None) -> None
```

Initialize a new `TimeInterval` object

#### time_str

```python
@property
def time_str() -> str
```

Return the time instance or interval as a string representation

#### from_response

```python
@staticmethod
def from_response(
        response: geoengine_api_client.models.TimeInterval) -> TimeInterval
```

create a `TimeInterval` from an API response

#### to_api_dict

```python
def to_api_dict() -> geoengine_api_client.TimeInterval
```

create a openapi `TimeInterval` from self

#### \_\_eq\_\_

```python
def __eq__(other: Any) -> bool
```

Check if two `TimeInterval` objects are equal.

## SpatialResolutionDict Objects

```python
class SpatialResolutionDict(TypedDict)
```

A spatial resolution as a dictionary

## SpatialResolution Objects

```python
class SpatialResolution()
```

&#x27;A spatial resolution.

#### \_\_init\_\_

```python
def __init__(x_resolution: float, y_resolution: float) -> None
```

Initialize a new `SpatialResolution` object

#### from_response

```python
@staticmethod
def from_response(response: SpatialResolutionDict) -> SpatialResolution
```

create a `SpatialResolution` from an API response

#### resolution_ogc

```python
def resolution_ogc(srs_code: str) -> tuple[float, float]
```

Return the resolution in OGC style

## QueryRectangle Objects

```python
class QueryRectangle()
```

A multi-dimensional query rectangle, consisting of spatial and temporal information.

#### \_\_init\_\_

```python
def __init__(spatial_bounds: BoundingBox2D | SpatialPartition2D
             | tuple[float, float, float, float],
             time_interval: TimeInterval | tuple[datetime, datetime | None],
             srs="EPSG:4326") -> None
```

Initialize a new `QueryRectangle` object

## Parameters

spatial_bounds
The spatial bounds of the query rectangle.
Either a `BoundingBox2D` or a tuple of floats (xmin, ymin, xmax, ymax)
time_interval
The time interval of the query rectangle.
Either a `TimeInterval` or a tuple of `datetime.datetime` objects (start, end)

#### bbox_str

```python
@property
def bbox_str() -> str
```

A comma-separated string representation of the spatial bounds

#### bbox_ogc_str

```python
@property
def bbox_ogc_str() -> str
```

A comma-separated string representation of the spatial bounds with OGC axis ordering

#### bbox_ogc

```python
@property
def bbox_ogc() -> tuple[float, float, float, float]
```

Return the bbox with OGC axis ordering of the srs

#### time

```python
@property
def time() -> TimeInterval
```

Return the time instance or interval

#### spatial_bounds

```python
@property
def spatial_bounds() -> BoundingBox2D
```

Return the spatial bounds

#### time_str

```python
@property
def time_str() -> str
```

Return the time instance or interval as a string representation

#### srs

```python
@property
def srs() -> str
```

Return the SRS string

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Return a string representation of the query rectangle.

#### with_raster_bands

```python
def with_raster_bands(raster_bands: list[int]) -> RasterQueryRectangle
```

Converts a `QueryRectangle` into a `RasterQueryRectangle`

## RasterQueryRectangle Objects

```python
class RasterQueryRectangle(QueryRectangle)
```

A multi-dimensional query rectangle, consisting of spatial and temporal information and raster bands.

#### \_\_init\_\_

```python
def __init__(spatial_bounds: BoundingBox2D | SpatialPartition2D
             | tuple[float, float, float, float],
             time_interval: TimeInterval | tuple[datetime, datetime | None],
             raster_bands: list[int] | None | int,
             srs="EPSG:4326") -> None
```

Initialize a new `QueryRectangle` object

## Parameters

spatial_bounds
The spatial bounds of the query rectangle.
Either a `BoundingBox2D` or a tuple of floats (xmin, ymin, xmax, ymax)
time_interval
The time interval of the query rectangle.
Either a `TimeInterval` or a tuple of `datetime.datetime` objects (start, end)
bands
The raster bands of the query rectangle.
A List of ints representing the band numbers.

#### raster_bands

```python
@property
def raster_bands() -> list[int]
```

Return the query bands

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Return a string representation of the query rectangle.

## ResultDescriptor Objects

```python
class ResultDescriptor()
```

Base class for result descriptors

#### \_\_init\_\_

```python
def __init__(spatial_reference: str,
             time_bounds: TimeInterval | None = None) -> None
```

Initialize a new `ResultDescriptor` object

#### from_response

```python
@staticmethod
def from_response(
        response: geoengine_api_client.TypedResultDescriptor
) -> ResultDescriptor
```

Parse a result descriptor from an http response

#### is_raster_result

```python
@classmethod
def is_raster_result(cls) -> bool
```

Return true if the result is of type raster

#### is_vector_result

```python
@classmethod
def is_vector_result(cls) -> bool
```

Return true if the result is of type vector

#### is_plot_result

```python
@classmethod
def is_plot_result(cls) -> bool
```

Return true if the result is of type plot

#### spatial_reference

```python
@property
def spatial_reference() -> str
```

Return the spatial reference

#### time_bounds

```python
@property
def time_bounds() -> TimeInterval | None
```

Return the time bounds

## VectorResultDescriptor Objects

```python
class VectorResultDescriptor(ResultDescriptor)
```

A vector result descriptor

#### \_\_init\_\_

```python
def __init__(spatial_reference: str,
             data_type: VectorDataType,
             columns: dict[str, VectorColumnInfo],
             time_bounds: TimeInterval | None = None,
             spatial_bounds: BoundingBox2D | None = None) -> None
```

Initialize a vector result descriptor

#### from_response_vector

```python
@staticmethod
def from_response_vector(
    response: geoengine_api_client.TypedVectorResultDescriptor
) -> VectorResultDescriptor
```

Parse a vector result descriptor from an http response

#### data_type

```python
@property
def data_type() -> VectorDataType
```

Return the data type

#### spatial_reference

```python
@property
def spatial_reference() -> str
```

Return the spatial reference

#### columns

```python
@property
def columns() -> dict[str, VectorColumnInfo]
```

Return the columns

#### spatial_bounds

```python
@property
def spatial_bounds() -> BoundingBox2D | None
```

Return the spatial bounds

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Display representation of the vector result descriptor

#### to_api_dict

```python
def to_api_dict() -> geoengine_api_client.TypedResultDescriptor
```

Convert the vector result descriptor to a dictionary

## FeatureDataType Objects

```python
class FeatureDataType(str, Enum)
```

Vector column data type

#### from_string

```python
@staticmethod
def from_string(data_type: str) -> FeatureDataType
```

Create a new `VectorColumnDataType` from a string

#### to_api_enum

```python
def to_api_enum() -> geoengine_api_client.FeatureDataType
```

Convert to an API enum

## VectorColumnInfo Objects

```python
@dataclass
class VectorColumnInfo()
```

Vector column information

#### from_response

```python
@staticmethod
def from_response(
        response: geoengine_api_client.VectorColumnInfo) -> VectorColumnInfo
```

Create a new `VectorColumnInfo` from a JSON response

#### to_api_dict

```python
def to_api_dict() -> geoengine_api_client.VectorColumnInfo
```

Convert to a dictionary

## RasterBandDescriptor Objects

```python
@dataclass(repr=False)
class RasterBandDescriptor()
```

A raster band descriptor

#### from_response

```python
@classmethod
def from_response(
    cls, response: geoengine_api_client.RasterBandDescriptor
) -> RasterBandDescriptor
```

Parse an http response to a `RasterBandDescriptor` object

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Display representation of a raster band descriptor

## GridIdx2D Objects

```python
@dataclass
class GridIdx2D()
```

A grid index

#### from_response

```python
@classmethod
def from_response(cls, response: geoengine_api_client.GridIdx2D) -> GridIdx2D
```

Parse an http response to a `GridIdx2D` object

## GridBoundingBox2D Objects

```python
@dataclass
class GridBoundingBox2D()
```

A grid boundingbox where lower right is inclusive index

#### from_response

```python
@classmethod
def from_response(
        cls,
        response: geoengine_api_client.GridBoundingBox2D) -> GridBoundingBox2D
```

Parse an http response to a `GridBoundingBox2D` object

#### contains_idx

```python
def contains_idx(idx: GridIdx2D) -> bool
```

Test if a `GridIdx2D` is contained by this

## SpatialGridDefinition Objects

```python
@dataclass
class SpatialGridDefinition()
```

A grid boundingbox where lower right is inclusive index

#### from_response

```python
@classmethod
def from_response(
    cls, response: geoengine_api_client.SpatialGridDefinition
) -> SpatialGridDefinition
```

Parse an http response to a `SpatialGridDefinition` object

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Display representation of the SpatialGridDefinition

## SpatialGridDescriptor Objects

```python
@dataclass
class SpatialGridDescriptor()
```

A grid boundingbox where lower right is inclusive index

#### from_response

```python
@classmethod
def from_response(
    cls, response: geoengine_api_client.SpatialGridDescriptor
) -> SpatialGridDescriptor
```

Parse an http response to a `SpatialGridDefinition` object

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Display representation of the SpatialGridDescriptor

## RasterDataType Objects

```python
class RasterDataType(str, Enum)
```

Raster data type enum

#### from_string

```python
@staticmethod
def from_string(data_type: str) -> RasterDataType
```

Create a new `RasterDataType` from a string

#### from_literal

```python
@staticmethod
def from_literal(
    data_type: Literal["U8", "U16", "U32", "U64", "I8", "I16", "I32", "I64",
                       "F32", "F64"]
) -> RasterDataType
```

Create a new `RasterDataType` from a literal

#### to_literal

```python
def to_literal() -> Literal["U8", "U16", "U32", "U64", "I8", "I16", "I32",
                            "I64", "F32", "F64"]
```

Convert to a literal

#### from_api_enum

```python
@staticmethod
def from_api_enum(
        data_type: geoengine_api_client.RasterDataType) -> RasterDataType
```

Create a new `RasterDataType` from an API enum

#### to_api_enum

```python
def to_api_enum() -> geoengine_api_client.RasterDataType
```

Convert to an API enum

#### to_np_dtype

```python
def to_np_dtype() -> np.dtype
```

Convert to a numpy dtype

## TimeDimension Objects

```python
class TimeDimension()
```

A time dimension

#### from_response

```python
@classmethod
def from_response(
    cls, response: geoengine_api_client.TimeDimension
) -> RegularTimeDimension | IrregularTimeDimension
```

Parse a time dimension from an http response

## RegularTimeDimension Objects

```python
class RegularTimeDimension(TimeDimension)
```

A regular time dimension

#### \_\_init\_\_

```python
def __init__(step: TimeStep, origin: np.datetime64 | None = None) -> None
```

Initialize a new `RegularTimeDimension`

#### to_api_dict

```python
def to_api_dict() -> geoengine_api_client.TimeDimension
```

Convert the regular time dimension to a dictionary

#### from_response

```python
@classmethod
def from_response(
        cls,
        response: geoengine_api_client.TimeDimension) -> RegularTimeDimension
```

Parse a regular time dimension from an http response

## IrregularTimeDimension Objects

```python
class IrregularTimeDimension(TimeDimension)
```

The irregular time dimension

#### to_api_dict

```python
def to_api_dict() -> geoengine_api_client.TimeDimension
```

Convert the irregular time dimension to a dictionary

#### from_response

```python
@classmethod
def from_response(cls, response: Any) -> IrregularTimeDimension
```

Parse an irregular time dimension from an http response

## TimeDescriptor Objects

```python
class TimeDescriptor()
```

A time descriptor

#### \_\_init\_\_

```python
def __init__(dimension: TimeDimension,
             bounds: TimeInterval | None = None) -> None
```

Initialize a new `TimeDescriptor`

#### to_api_dict

```python
def to_api_dict() -> geoengine_api_client.TimeDescriptor
```

Convert the time descriptor to a dictionary

#### from_response

```python
@staticmethod
def from_response(
        response: geoengine_api_client.TimeDescriptor) -> TimeDescriptor
```

Parse a time descriptor from an http response

## RasterResultDescriptor Objects

```python
class RasterResultDescriptor(ResultDescriptor)
```

A raster result descriptor

#### \_\_init\_\_

```python
def __init__(
        data_type: RasterDataType | Literal["U8", "U16", "U32", "U64", "I8",
                                            "I16", "I32", "I64", "F32", "F64"],
        bands: list[RasterBandDescriptor],
        spatial_reference: str,
        spatial_grid: SpatialGridDescriptor,
        time: TimeDescriptor | TimeInterval | TimeDimension | None = None
) -> None
```

Initialize a new `RasterResultDescriptor`

#### to_api_dict

```python
def to_api_dict() -> geoengine_api_client.TypedResultDescriptor
```

Convert the raster result descriptor to a dictionary

#### from_response_raster

```python
@staticmethod
def from_response_raster(
    response: geoengine_api_client.TypedRasterResultDescriptor
) -> RasterResultDescriptor
```

Parse a raster result descriptor from an http response

#### spatial_reference

```python
@property
def spatial_reference() -> str
```

Return the spatial reference

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Display representation of the raster result descriptor

## PlotResultDescriptor Objects

```python
class PlotResultDescriptor(ResultDescriptor)
```

A plot result descriptor

#### \_\_init\_\_

```python
def __init__(spatial_reference: str,
             time_bounds: TimeInterval | None = None,
             spatial_bounds: BoundingBox2D | None = None) -> None
```

Initialize a new `PlotResultDescriptor`

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Display representation of the plot result descriptor

#### from_response_plot

```python
@staticmethod
def from_response_plot(
    response: geoengine_api_client.TypedPlotResultDescriptor
) -> PlotResultDescriptor
```

Create a new `PlotResultDescriptor` from a JSON response

#### spatial_reference

```python
@property
def spatial_reference() -> str
```

Return the spatial reference

#### to_api_dict

```python
def to_api_dict() -> geoengine_api_client.TypedResultDescriptor
```

Convert the plot result descriptor to a dictionary

## VectorDataType Objects

```python
class VectorDataType(str, Enum)
```

An enum of vector data types

#### from_geopandas_type_name

```python
@classmethod
def from_geopandas_type_name(cls, name: str) -> VectorDataType
```

Resolve vector data type from geopandas geometry type

#### from_literal

```python
@staticmethod
def from_literal(
    literal: Literal["Data", "MultiPoint", "MultiLineString", "MultiPolygon"]
) -> VectorDataType
```

Resolve vector data type from literal

#### from_api_enum

```python
@staticmethod
def from_api_enum(
        data_type: geoengine_api_client.VectorDataType) -> VectorDataType
```

Resolve vector data type from API enum

#### from_string

```python
@staticmethod
def from_string(string: str) -> VectorDataType
```

Resolve vector data type from string

## TimeStepGranularity Objects

```python
class TimeStepGranularity(Enum)
```

An enum of time step granularities

## TimeStep Objects

```python
@dataclass
class TimeStep()
```

A time step that consists of a granularity and a step size

#### \_\_init\_\_

```python
def __init__(step: int, granularity: TimeStepGranularity | str) -> None
```

Initialize a new `TimeStep` object

#### from_response

```python
@classmethod
def from_response(cls, response: geoengine_api_client.TimeStep) -> TimeStep
```

Parse an http response to a `TimeStep` object

## Provenance Objects

```python
@dataclass
class Provenance()
```

Provenance information as triplet of citation, license and uri

#### from_response

```python
@classmethod
def from_response(cls,
                  response: geoengine_api_client.Provenance) -> Provenance
```

Parse an http response to a `Provenance` object

## ProvenanceEntry Objects

```python
@dataclass
class ProvenanceEntry()
```

Provenance of a dataset

#### from_response

```python
@classmethod
def from_response(
        cls,
        response: geoengine_api_client.ProvenanceEntry) -> ProvenanceEntry
```

Parse an http response to a `ProvenanceEntry` object

## Symbology Objects

```python
class Symbology()
```

Base class for symbology

#### from_response

```python
@staticmethod
def from_response(response: geoengine_api_client.Symbology) -> Symbology
```

Parse an http response to a `Symbology` object

## VectorSymbology Objects

```python
class VectorSymbology(Symbology)
```

A vector symbology

## RasterColorizer Objects

```python
class RasterColorizer()
```

Base class for raster colorizer

#### from_response

```python
@classmethod
def from_response(
        cls,
        response: geoengine_api_client.RasterColorizer) -> RasterColorizer
```

Parse an http response to a `RasterColorizer` object

## SingleBandRasterColorizer Objects

```python
@dataclass
class SingleBandRasterColorizer(RasterColorizer)
```

A raster colorizer for a specified band

## MultiBandRasterColorizer Objects

```python
@dataclass
class MultiBandRasterColorizer(RasterColorizer)
```

A raster colorizer for multiple bands

## RasterSymbology Objects

```python
class RasterSymbology(Symbology)
```

A raster symbology

#### \_\_init\_\_

```python
def __init__(raster_colorizer: RasterColorizer, opacity: float = 1.0) -> None
```

Initialize a new `RasterSymbology`

#### to_api_dict

```python
def to_api_dict() -> geoengine_api_client.Symbology
```

Convert the raster symbology to a dictionary

#### from_response_raster

```python
@staticmethod
def from_response_raster(
        response: geoengine_api_client.RasterSymbology) -> RasterSymbology
```

Parse an http response to a `RasterSymbology` object

#### \_\_eq\_\_

```python
def __eq__(value)
```

Check if two RasterSymbologies are equal

## DataId Objects

```python
class DataId()
```

Base class for data ids

#### from_response

```python
@classmethod
def from_response(cls, response: geoengine_api_client.DataId) -> DataId
```

Parse an http response to a `DataId` object

## InternalDataId Objects

```python
class InternalDataId(DataId)
```

An internal data id

#### from_response_internal

```python
@classmethod
def from_response_internal(
        cls, response: geoengine_api_client.InternalDataId) -> InternalDataId
```

Parse an http response to a `InternalDataId` object

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Display representation of an internal data id

#### \_\_eq\_\_

```python
def __eq__(other) -> bool
```

Check if two internal data ids are equal

## ExternalDataId Objects

```python
class ExternalDataId(DataId)
```

An external data id

#### from_response_external

```python
@classmethod
def from_response_external(
        cls, response: geoengine_api_client.ExternalDataId) -> ExternalDataId
```

Parse an http response to a `ExternalDataId` object

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Display representation of an external data id

#### \_\_eq\_\_

```python
def __eq__(other) -> bool
```

Check if two external data ids are equal

## Measurement Objects

```python
class Measurement()
```

Base class for measurements

#### from_response

```python
@staticmethod
def from_response(response: geoengine_api_client.Measurement) -> Measurement
```

Parse a result descriptor from an http response

## UnitlessMeasurement Objects

```python
class UnitlessMeasurement(Measurement)
```

A measurement that is unitless

#### \_\_str\_\_

```python
def __str__() -> str
```

String representation of a unitless measurement

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Display representation of a unitless measurement

## ContinuousMeasurement Objects

```python
class ContinuousMeasurement(Measurement)
```

A measurement that is continuous

#### \_\_init\_\_

```python
def __init__(measurement: str, unit: str | None) -> None
```

Initialize a new `ContiuousMeasurement`

#### from_response_continuous

```python
@staticmethod
def from_response_continuous(
    response: geoengine_api_client.ContinuousMeasurement
) -> ContinuousMeasurement
```

Initialize a new `ContiuousMeasurement from a JSON response

#### \_\_str\_\_

```python
def __str__() -> str
```

String representation of a continuous measurement

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Display representation of a continuous measurement

## ClassificationMeasurement Objects

```python
class ClassificationMeasurement(Measurement)
```

A measurement that is a classification

#### \_\_init\_\_

```python
def __init__(measurement: str, classes: dict[int, str]) -> None
```

Initialize a new `ClassificationMeasurement`

#### from_response_classification

```python
@staticmethod
def from_response_classification(
    response: geoengine_api_client.ClassificationMeasurement
) -> ClassificationMeasurement
```

Initialize a new `ClassificationMeasurement from a JSON response

#### \_\_str\_\_

```python
def __str__() -> str
```

String representation of a classification measurement

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Display representation of a classification measurement

## GeoTransform Objects

```python
class GeoTransform()
```

The `GeoTransform` specifies the relationship between pixel coordinates and geographic coordinates.

#### y_max

In Geo Engine, x_pixel_size is always positive.

#### x_pixel_size

In Geo Engine, y_pixel_size is always negative.

#### \_\_init\_\_

```python
def __init__(x_min: float, y_max: float, x_pixel_size: float,
             y_pixel_size: float)
```

Initialize a new `GeoTransform`

#### from_response

```python
@classmethod
def from_response(cls,
                  response: geoengine_api_client.GeoTransform) -> GeoTransform
```

Parse a geotransform from an HTTP JSON response

#### to_gdal

```python
def to_gdal() -> tuple[float, float, float, float, float, float]
```

Convert to a GDAL geotransform

#### coord_to_pixel_ul

```python
def coord_to_pixel_ul(x_cord: float, y_coord: float) -> GridIdx2D
```

Convert a coordinate to a pixel index rould towards top left

#### coord_to_pixel_lr

```python
def coord_to_pixel_lr(x_cord: float, y_coord: float) -> GridIdx2D
```

Convert a coordinate to a pixel index ound towards lower right

#### pixel_ul_to_coord

```python
def pixel_ul_to_coord(x_pixel: int, y_pixel: int) -> tuple[float, float]
```

Convert a pixel position into a coordinate

#### spatial_to_grid_bounds

```python
def spatial_to_grid_bounds(
        bounds: SpatialPartition2D | BoundingBox2D) -> GridBoundingBox2D
```

Converts a BoundingBox2D or a SpatialPartition2D into a GridBoundingBox2D

#### grid_bounds_to_spatial_bounds

```python
def grid_bounds_to_spatial_bounds(
        bounds: GridBoundingBox2D) -> SpatialPartition2D
```

Converts a GridBoundingBox2D into a SpatialPartition2D

#### \_\_eq\_\_

```python
def __eq__(other) -> bool
```

Check if two geotransforms are equal
