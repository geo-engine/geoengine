---
sidebar_label: operators
title: processing_graph_builder.operators
---

This module contains helpers to create processing graph operators for the Geo Engine API.

## Operator Objects

```python
class Operator()
```

Base class for all operators.

#### name

```python
@abstractmethod
def name() -> str
```

Returns the name of the operator.

#### to\_dict

```python
@abstractmethod
def to_dict() -> dict[str, Any]
```

Returns a dictionary representation of the operator that can be used to create a JSON request for the API.

#### data\_type

```python
@abstractmethod
def data_type() -> Literal["Raster", "Vector"]
```

Returns the type of the operator.

#### to\_processing\_graph\_dict

```python
def to_processing_graph_dict() -> dict[str, Any]
```

Returns a dictionary representation of a processing graph that calls the operator&quot; &quot;that can be used to create a JSON request for the processing graph API.

#### to\_workflow\_dict

```python
@deprecated("Use `Operator.to_processing_graph_dict` instead.",
            category=DeprecationWarning)
def to_workflow_dict() -> dict[str, Any]
```

Deprecated: use `Operator.to_processing_graph_dict` instead.

#### from\_processing\_graph\_dict

```python
@classmethod
def from_processing_graph_dict(cls, processing_graph) -> Operator
```

Returns an operator from a processing graph dictionary.

#### from\_workflow\_dict

```python
@classmethod
@deprecated("Use `Operator.from_processing_graph_dict` instead.",
            category=DeprecationWarning)
def from_workflow_dict(cls, workflow) -> Operator
```

Deprecated: use `Operator.from_processing_graph_dict` instead.

## RasterOperator Objects

```python
class RasterOperator(Operator)
```

Base class for all raster operators.

#### from\_operator\_dict

```python
@classmethod
def from_operator_dict(cls, operator_dict: dict[str, Any]) -> RasterOperator
```

Returns an operator from a dictionary.

## VectorOperator Objects

```python
class VectorOperator(Operator)
```

Base class for all vector operators.

#### from\_operator\_dict

```python
@classmethod
def from_operator_dict(cls, operator_dict: dict[str, Any]) -> VectorOperator
```

Returns an operator from a dictionary.

## GdalSource Objects

```python
class GdalSource(RasterOperator)
```

A GDAL source operator.

#### \_\_init\_\_

```python
def __init__(dataset: str | DatasetName)
```

Creates a new GDAL source operator.

#### from\_operator\_dict

```python
@classmethod
def from_operator_dict(cls, operator_dict: dict[str, Any]) -> GdalSource
```

Returns an operator from a dictionary.

## MultiBandGdalSource Objects

```python
class MultiBandGdalSource(RasterOperator)
```

A multi-band GDAL source operator referencing a named dataset with all its bands.

#### \_\_init\_\_

```python
def __init__(dataset: str | DatasetName)
```

Creates a new multi-band GDAL source operator.

#### from\_operator\_dict

```python
@classmethod
def from_operator_dict(cls, operator_dict: dict[str,
                                                Any]) -> MultiBandGdalSource
```

Returns an operator from a dictionary.

## OgrSource Objects

```python
class OgrSource(VectorOperator)
```

An OGR source operator.

#### \_\_init\_\_

```python
def __init__(dataset: str | DatasetName,
             attribute_projection: str | None = None,
             attribute_filters: str | None = None)
```

Creates a new OGR source operator.

#### from\_operator\_dict

```python
@classmethod
def from_operator_dict(cls, operator_dict: dict[str, Any]) -> OgrSource
```

Returns an operator from a dictionary.

## Interpolation Objects

```python
class Interpolation(RasterOperator)
```

An interpolation operator.

#### \_\_init\_\_

```python
def __init__(source_operator: RasterOperator,
             output_x: float,
             output_y: float,
             output_method: Literal["resolution", "fraction"] = "resolution",
             interpolation: Literal["biLinear",
                                    "nearestNeighbor"] = "biLinear",
             output_origin_reference: tuple[float, float] | None = None)
```

Creates a new interpolation operator.

#### from\_operator\_dict

```python
@classmethod
def from_operator_dict(cls, operator_dict: dict[str, Any]) -> Interpolation
```

Returns an operator from a dictionary.

## Downsampling Objects

```python
class Downsampling(RasterOperator)
```

A Downsampling operator.

#### \_\_init\_\_

```python
def __init__(source_operator: RasterOperator,
             output_x: float,
             output_y: float,
             output_method: Literal["resolution", "fraction"] = "resolution",
             sample_method: Literal["nearestNeighbor"] = "nearestNeighbor",
             output_origin_reference: tuple[float, float] | None = None)
```

Creates a new Downsampling operator.

#### from\_operator\_dict

```python
@classmethod
def from_operator_dict(cls, operator_dict: dict[str, Any]) -> Downsampling
```

Returns an operator from a dictionary.

## ColumnNames Objects

```python
class ColumnNames()
```

Base class for deriving column names from bands of a raster.

#### from\_dict

```python
@classmethod
def from_dict(cls, rename_dict: dict[str, Any]) -> ColumnNames
```

Returns a ColumnNames object from a dictionary.

## ColumnNamesDefault Objects

```python
class ColumnNamesDefault(ColumnNames)
```

column names with default suffix.

## ColumnNamesSuffix Objects

```python
class ColumnNamesSuffix(ColumnNames)
```

Rename bands with custom suffixes.

## ColumnNamesNames Objects

```python
class ColumnNamesNames(ColumnNames)
```

Rename bands with new names.

## RasterVectorJoin Objects

```python
class RasterVectorJoin(VectorOperator)
```

A RasterVectorJoin operator.

#### \_\_init\_\_

```python
def __init__(raster_sources: list[RasterOperator],
             vector_source: VectorOperator,
             names: ColumnNames,
             temporal_aggregation: Literal["none", "first", "mean"] = "none",
             temporal_aggregation_ignore_nodata: bool = False,
             feature_aggregation: Literal["first", "mean"] = "mean",
             feature_aggregation_ignore_nodata: bool = False)
```

Creates a new RasterVectorJoin operator.

#### from\_operator\_dict

```python
@classmethod
def from_operator_dict(cls, operator_dict: dict[str, Any]) -> RasterVectorJoin
```

Returns an operator from a dictionary.

## PointInPolygonFilter Objects

```python
class PointInPolygonFilter(VectorOperator)
```

A PointInPolygonFilter operator.

#### \_\_init\_\_

```python
def __init__(point_source: VectorOperator, polygon_source: VectorOperator)
```

Creates a new PointInPolygonFilter filter operator.

#### from\_operator\_dict

```python
@classmethod
def from_operator_dict(cls, operator_dict: dict[str,
                                                Any]) -> PointInPolygonFilter
```

Returns an operator from a dictionary.

## RasterScaling Objects

```python
class RasterScaling(RasterOperator)
```

A RasterScaling operator.

This operator scales the values of a raster by a given slope and offset.

The scaling is done as follows:
y = (x - offset) / slope

The unscale mode is the inverse of the scale mode:
x = y * slope + offset

#### \_\_init\_\_

```python
def __init__(source: RasterOperator,
             slope: float | str | None = None,
             offset: float | str | None = None,
             scaling_mode: Literal["mulSlopeAddOffset",
                                   "subOffsetDivSlope"] = "mulSlopeAddOffset",
             output_measurement: str | None = None)
```

Creates a new RasterScaling operator.

## RasterTypeConversion Objects

```python
class RasterTypeConversion(RasterOperator)
```

A RasterTypeConversion operator.

#### \_\_init\_\_

```python
def __init__(source: RasterOperator,
             output_data_type: Literal["U8", "U16", "U32", "U64", "I8", "I16",
                                       "I32", "I64", "F32", "F64"])
```

Creates a new RasterTypeConversion operator.

## Reprojection Objects

```python
class Reprojection(Operator)
```

A Reprojection operator.

#### \_\_init\_\_

```python
def __init__(source: Operator, target_spatial_reference: str)
```

Creates a new Reprojection operator.

#### as\_vector

```python
def as_vector() -> VectorOperator
```

Casts this operator to a VectorOperator.

#### as\_raster

```python
def as_raster() -> RasterOperator
```

Casts this operator to a RasterOperator.

#### from\_operator\_dict

```python
@classmethod
def from_operator_dict(cls, operator_dict: dict[str, Any]) -> Reprojection
```

Constructs the operator from the given dictionary.

## Expression Objects

```python
class Expression(RasterOperator)
```

An Expression operator.

#### \_\_init\_\_

```python
def __init__(expression: str,
             source: RasterOperator,
             output_type: Literal["U8", "U16", "U32", "U64", "I8", "I16",
                                  "I32", "I64", "F32", "F64"] = "F32",
             map_no_data: bool = False,
             output_band: RasterBandDescriptor | None = None)
```

Creates a new Expression operator.

## BandwiseExpression Objects

```python
class BandwiseExpression(RasterOperator)
```

A bandwise Expression operator.

#### \_\_init\_\_

```python
def __init__(expression: str,
             source: RasterOperator,
             output_type: Literal["U8", "U16", "U32", "U64", "I8", "I16",
                                  "I32", "I64", "F32", "F64"] = "F32",
             map_no_data: bool = False)
```

Creates a new Expression operator.

## GeoVectorDataType Objects

```python
class GeoVectorDataType(Enum)
```

The output type of geometry vector data.

## VectorExpression Objects

```python
class VectorExpression(VectorOperator)
```

The `VectorExpression` operator.

#### \_\_init\_\_

```python
def __init__(source: VectorOperator,
             *,
             expression: str,
             input_columns: list[str],
             output_column: str | GeoVectorDataType,
             geometry_column_name: str | None = None,
             output_measurement: Measurement | None = None)
```

Creates a new VectorExpression operator.

## TemporalRasterAggregation Objects

```python
class TemporalRasterAggregation(RasterOperator)
```

A TemporalRasterAggregation operator.

#### \_\_init\_\_

```python
def __init__(source: RasterOperator,
             aggregation_type: Literal["mean", "min", "max", "median", "count",
                                       "sum", "first", "last",
                                       "percentileEstimate"],
             ignore_no_data: bool = False,
             granularity: Literal["days", "months", "years", "hours",
                                  "minutes", "seconds", "millis"] = "days",
             window_size: int = 1,
             output_type: Literal["U8", "U16", "U32", "U64", "I8", "I16",
                                  "I32", "I64", "F32", "F64"] | None = None,
             percentile: float | None = None,
             window_reference: datetime.datetime | np.datetime64
             | None = None)
```

Creates a new TemporalRasterAggregation operator.

## TimeShift Objects

```python
class TimeShift(Operator)
```

A RasterTypeConversion operator.

#### \_\_init\_\_

```python
def __init__(source: RasterOperator | VectorOperator,
             shift_type: Literal["relative", "absolute"],
             granularity: Literal["days", "months", "years", "hours",
                                  "minutes", "seconds", "millis"], value: int)
```

Creates a new RasterTypeConversion operator.

#### as\_vector

```python
def as_vector() -> VectorOperator
```

Casts this operator to a VectorOperator.

#### as\_raster

```python
def as_raster() -> RasterOperator
```

Casts this operator to a RasterOperator.

#### from\_operator\_dict

```python
@classmethod
def from_operator_dict(cls, operator_dict: dict[str, Any]) -> TimeShift
```

Constructs the operator from the given dictionary.

## RenameBands Objects

```python
class RenameBands()
```

Base class for renaming bands of a raster.

#### from\_dict

```python
@classmethod
def from_dict(cls, rename_dict: dict[str, Any]) -> RenameBands
```

Returns a RenameBands object from a dictionary.

## RenameBandsDefault Objects

```python
class RenameBandsDefault(RenameBands)
```

Rename bands with default suffix.

## RenameBandsSuffix Objects

```python
class RenameBandsSuffix(RenameBands)
```

Rename bands with custom suffixes.

## RenameBandsRename Objects

```python
class RenameBandsRename(RenameBands)
```

Rename bands with new names.

## RasterStacker Objects

```python
class RasterStacker(RasterOperator)
```

The RasterStacker operator.

#### \_\_init\_\_

```python
def __init__(sources: list[RasterOperator], rename: RenameBands | None = None)
```

Creates a new RasterStacker operator.

## BandFilter Objects

```python
class BandFilter(RasterOperator)
```

The BandFilter operator.

Selects a subset of the source&#x27;s bands, either by band names or by band
indices. The order of the remaining bands is preserved and the output
bands are renumbered to 0..n.

#### \_\_init\_\_

```python
def __init__(source: RasterOperator, bands: list[str] | list[int])
```

Creates a new BandFilter operator.

#### from\_operator\_dict

```python
@classmethod
def from_operator_dict(cls, operator_dict: dict[str, Any]) -> BandFilter
```

Returns an operator from a dictionary.

## BandNeighborhoodAggregate Objects

```python
class BandNeighborhoodAggregate(RasterOperator)
```

The BandNeighborhoodAggregate operator.

#### \_\_init\_\_

```python
def __init__(source: RasterOperator,
             aggregate: BandNeighborhoodAggregateParams)
```

Creates a new BandNeighborhoodAggregate operator.

## BandNeighborhoodAggregateParams Objects

```python
class BandNeighborhoodAggregateParams()
```

Abstract base class for band neighborhood aggregate params.

#### from\_dict

```python
@classmethod
def from_dict(
    cls, band_neighborhood_aggregate_dict: dict[str, Any]
) -> BandNeighborhoodAggregateParams
```

Returns a BandNeighborhoodAggregate object from a dictionary.

## BandNeighborhoodAggregateFirstDerivative Objects

```python
@dataclass
class BandNeighborhoodAggregateFirstDerivative(BandNeighborhoodAggregateParams
                                               )
```

The first derivative band neighborhood aggregate.

## BandNeighborhoodAggregateAverage Objects

```python
@dataclass
class BandNeighborhoodAggregateAverage(BandNeighborhoodAggregateParams)
```

The average band neighborhood aggregate.

## Onnx Objects

```python
class Onnx(RasterOperator)
```

Onnx ML operator.

#### \_\_init\_\_

```python
def __init__(source: RasterOperator, model: str)
```

Creates a new Onnx operator.

## PolygonRasterizationValue Objects

```python
class PolygonRasterizationValue()
```

The value that a polygon burns into the pixels it covers.

#### from\_dict

```python
@classmethod
def from_dict(cls, value_dict: dict[str, Any]) -> PolygonRasterizationValue
```

Returns a value object from a dictionary.

#### constant

```python
@classmethod
def constant(cls, value: float) -> PolygonRasterizationValue
```

Each covered pixel gets the `value`.

#### count

```python
@classmethod
def count(cls) -> PolygonRasterizationValue
```

Each pixel gets the number of polygons that cover it.

#### attribute

```python
@classmethod
def attribute(cls,
              column: str,
              aggregation: PolygonRasterizationAggregation,
              ignore_no_data: bool = True) -> PolygonRasterizationValue
```

Each pixel gets the value of a numeric column, aggregated over all polygons that cover it.

If `ignore_no_data` is `False`, features with a null value make all pixels they cover no data.

## PolygonRasterizationValueConstant Objects

```python
@dataclass
class PolygonRasterizationValueConstant(PolygonRasterizationValue)
```

Each covered pixel gets the constant value.

## PolygonRasterizationValueCount Objects

```python
@dataclass
class PolygonRasterizationValueCount(PolygonRasterizationValue)
```

Each pixel gets the number of polygons that cover it.

## PolygonRasterizationValueAttribute Objects

```python
@dataclass
class PolygonRasterizationValueAttribute(PolygonRasterizationValue)
```

Each pixel gets the value of a numeric column, aggregated over all polygons that cover it.

## PolygonRasterizationTime Objects

```python
class PolygonRasterizationTime()
```

How the time slices of the output raster are derived from the features.

#### from\_dict

```python
@classmethod
def from_dict(cls, time_dict: dict[str, Any]) -> PolygonRasterizationTime
```

Returns a time object from a dictionary.

#### feature\_times

```python
@classmethod
def feature_times(cls) -> PolygonRasterizationTime
```

Every start and end time of a feature is a slice boundary.

#### regular

```python
@classmethod
def regular(
    cls,
    granularity: TimeGranularity,
    step: int = 1,
    reference: datetime.datetime | np.datetime64 | None = None
) -> PolygonRasterizationTime
```

Regular time slices of length `step`, aligned to `reference` (default: Unix epoch).

#### ignore

```python
@classmethod
def ignore(cls) -> PolygonRasterizationTime
```

All features are rasterized into one time slice that is valid forever.

## PolygonRasterizationTimeFeatureTimes Objects

```python
@dataclass
class PolygonRasterizationTimeFeatureTimes(PolygonRasterizationTime)
```

Every start and end time of a feature is a slice boundary.

## PolygonRasterizationTimeRegular Objects

```python
@dataclass
class PolygonRasterizationTimeRegular(PolygonRasterizationTime)
```

Regular time slices that contain all features whose time intersects them.

## PolygonRasterizationTimeIgnore Objects

```python
@dataclass
class PolygonRasterizationTimeIgnore(PolygonRasterizationTime)
```

All features are rasterized into one time slice that is valid forever.

## PolygonRasterization Objects

```python
class PolygonRasterization(RasterOperator)
```

The `PolygonRasterization` operator, which rasterizes (multi) polygons.

A pixel is covered by a polygon if its center lies inside the polygon.
Pixels that are not covered by any polygon are no data.

#### \_\_init\_\_

```python
def __init__(source: VectorOperator,
             *,
             spatial_resolution: tuple[float, float],
             origin_coordinate: tuple[float, float] | None = None,
             value: PolygonRasterizationValue | None = None,
             output_type: Literal["U8", "U16", "U32", "U64", "I8", "I16",
                                  "I32", "I64", "F32", "F64"] | None = None,
             output_measurement: Measurement | None = None,
             time: PolygonRasterizationTime | None = None)
```

Creates a new PolygonRasterization operator.

`spatial_resolution` are the positive pixel sizes in x and y direction.
The rows of the grid run from north to south, and `origin_coordinate` is a pixel corner
to which the grid is aligned (default: `(0, 0)`).
Omitted parameters use the defaults of the server, i.e., a constant value of `1` and
time slices between the start and end times of the features.
