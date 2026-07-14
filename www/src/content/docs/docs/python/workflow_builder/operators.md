---
sidebar_label: operators
title: workflow_builder.operators
---

This module contains helpers to create workflow operators for the Geo Engine API.

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

#### to_dict

```python
@abstractmethod
def to_dict() -> dict[str, Any]
```

Returns a dictionary representation of the operator that can be used to create a JSON request for the API.

#### data_type

```python
@abstractmethod
def data_type() -> Literal["Raster", "Vector"]
```

Returns the type of the operator.

#### to_workflow_dict

```python
def to_workflow_dict() -> dict[str, Any]
```

Returns a dictionary representation of a workflow that calls the operator&quot; &quot;that can be used to create a JSON request for the workflow API.

#### from_workflow_dict

```python
@classmethod
def from_workflow_dict(cls, workflow) -> Operator
```

Returns an operator from a workflow dictionary.

## RasterOperator Objects

```python
class RasterOperator(Operator)
```

Base class for all raster operators.

#### from_operator_dict

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

#### from_operator_dict

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

#### from_operator_dict

```python
@classmethod
def from_operator_dict(cls, operator_dict: dict[str, Any]) -> GdalSource
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

#### from_operator_dict

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

#### from_operator_dict

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

#### from_operator_dict

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

#### from_dict

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

#### from_operator_dict

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

#### from_operator_dict

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
x = y \* slope + offset

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

#### as_vector

```python
def as_vector() -> VectorOperator
```

Casts this operator to a VectorOperator.

#### as_raster

```python
def as_raster() -> RasterOperator
```

Casts this operator to a RasterOperator.

#### from_operator_dict

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

#### as_vector

```python
def as_vector() -> VectorOperator
```

Casts this operator to a VectorOperator.

#### as_raster

```python
def as_raster() -> RasterOperator
```

Casts this operator to a RasterOperator.

#### from_operator_dict

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

#### from_dict

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

#### from_dict

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
