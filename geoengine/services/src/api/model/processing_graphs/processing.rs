use crate::api::model::{
    datatypes::{
        Coordinate2D, SpatialReference, SpatialResolution, TimeGranularity, TimeInstance,
        TimeInterval, TimeStep,
    },
    processing_graphs::{
        VectorOperator,
        parameters::{
            ColumnNames, FeatureAggregationMethod, Measurement, OutputColumn, RasterBandDescriptor,
            RasterDataType, TemporalAggregationMethod,
        },
        source_parameters::{
            MultipleRasterSources, SingleRasterOrVectorSource, SingleRasterSource,
            SingleVectorMultipleRasterSources, SingleVectorSource,
        },
    },
};
use geoengine_macros::{api_operator, type_tag};
use geoengine_operators::{
    machine_learning::{onnx::Onnx as OperatorsOnnx, onnx::OnnxParams as OperatorsOnnxParameters},
    processing::{
        Aggregation as OperatorsAggregation,
        AttributeAggregateType as OperatorsAttributeAggregateType,
        BandDistance as OperatorsBandDistance, BandFilter as OperatorsBandFilter,
        BandFilterParams as OperatorsBandFilterParameters,
        BandNeighborhoodAggregate as OperatorsBandNeighborhoodAggregate,
        BandNeighborhoodAggregateMethod as OperatorsBandNeighborhoodAggregateMethod,
        BandNeighborhoodAggregateParams as OperatorsBandNeighborhoodAggregateParameters,
        BandwiseExpression as OperatorsBandwiseExpression,
        BandwiseExpressionParams as OperatorsBandwiseExpressionParameters,
        ColumnRangeFilter as OperatorsColumnRangeFilter,
        ColumnRangeFilterParams as OperatorsColumnRangeFilterParameters,
        DensityParams as OperatorsDensityParameters,
        DeriveOutRasterSpecsSource as OperatorsDeriveOutRasterSpecsSource,
        Downsampling as OperatorsDownsampling, DownsamplingMethod as OperatorsDownsamplingMethod,
        DownsamplingParams as OperatorsDownsamplingParameters,
        DownsamplingResolution as OperatorsDownsamplingResolution,
        Expression as OperatorsExpression, ExpressionParams as OperatorsExpressionParameters,
        Fraction as OperatorsFraction, Interpolation as OperatorsInterpolation,
        InterpolationMethod as OperatorsInterpolationMethod,
        InterpolationParams as OperatorsInterpolationParameters,
        InterpolationResolution as OperatorsInterpolationResolution,
        LineSimplification as OperatorsLineSimplification,
        LineSimplificationAlgorithm as OperatorsLineSimplificationAlgorithm,
        LineSimplificationParams as OperatorsLineSimplificationParameters,
        NeighborhoodAggregate as OperatorsNeighborhoodAggregate,
        NeighborhoodAggregateParams as OperatorsNeighborhoodAggregateParameters,
        OutputColumn as OperatorsOutputColumn,
        PointInPolygonFilter as OperatorsPointInPolygonFilter,
        PointInPolygonFilterParams as OperatorsPointInPolygonFilterParameters,
        PointInPolygonFilterSource as OperatorsPointInPolygonFilterSource,
        Radiance as OperatorsRadiance, RadianceParams as OperatorsRadianceParameters,
        RasterScaling as OperatorsRasterScaling,
        RasterScalingParams as OperatorsRasterScalingParameters,
        RasterStacker as OperatorsRasterStacker,
        RasterStackerParams as OperatorsRasterStackerParameters,
        RasterTypeConversion as OperatorsRasterTypeConversion,
        RasterTypeConversionParams as OperatorsRasterTypeConversionParameters,
        RasterVectorJoin as OperatorsRasterVectorJoin,
        RasterVectorJoinParams as OperatorsRasterVectorJoinParameters,
        Rasterization as OperatorsRasterization,
        RasterizationParams as OperatorsRasterizationParameters,
        Reflectance as OperatorsReflectance, ReflectanceParams as OperatorsReflectanceParameters,
        Reprojection as OperatorsReprojection,
        ReprojectionParams as OperatorsReprojectionParameters, ScalingMode as OperatorsScalingMode,
        SlopeOffsetSelection as OperatorsSlopeOffsetSelection, Temperature as OperatorsTemperature,
        TemperatureParams as OperatorsTemperatureParameters,
        TemporalRasterAggregation as OperatorsTemporalRasterAggregation,
        TemporalRasterAggregationParameters as OperatorsTemporalRasterAggregationParameters,
        TimeProjection as OperatorsTimeProjection,
        TimeProjectionParams as OperatorsTimeProjectionParameters, TimeShift as OperatorsTimeShift,
        TimeShiftParams as OperatorsTimeShiftParameters,
        VectorExpression as OperatorsVectorExpression,
        VectorExpressionParams as OperatorsVectorExpressionParameters,
        VectorJoin as OperatorsVectorJoin, VectorJoinParams as OperatorsVectorJoinParameters,
        VectorJoinSources as OperatorsVectorJoinSources, VectorJoinType as OperatorsVectorJoinType,
        VisualPointClustering as OperatorsVisualPointClustering,
        VisualPointClusteringParams as OperatorsVisualPointClusteringParameters,
    },
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// The `Expression` operator performs a pixel-wise mathematical expression on one or more bands of a raster source.
/// The expression is specified as a user-defined script in a very simple language.
/// The output is a raster time series with the result of the expression and with time intervals that are the same as for the inputs.
/// Users can specify an output data type.
/// Internally, the expression is evaluated using floating-point numbers.
///
/// An example usage scenario is to calculate NDVI for a red and a near-infrared raster channel.
/// The expression uses a raster source with two bands, referred to as A and B, and calculates the formula `(A - B) / (A + B)`.
/// When the temporal resolution is months, our output NDVI will also be a monthly time series.
///
/// ## Types
///
/// The following describes the types used in the parameters.
///
/// ### Expression
///
/// Expressions are simple scripts to perform pixel-wise computations.
/// One can refer to the raster inputs as `A` for the first raster band, `B` for the second, and so on.
/// Furthermore, expressions can check with `A IS NODATA`, `B IS NODATA`, etc. for NO DATA values.
/// This is important if `mapNoData` is set to true.
/// Otherwise, NO DATA values are mapped automatically to the output NO DATA value.
/// Finally, the value `NODATA` can be used to output NO DATA.
///
/// Users can think of this implicit function signature for, e.g., two inputs:
///
/// ```rust,ignore
/// fn (A: f64, B: f64) -> f64
/// ```
///
/// As a start, expressions contain algebraic operations and mathematical functions.
///
/// ```rust,ignore
/// (A + B) / 2
/// ```
///
/// In addition, branches can be used to check for conditions.
///
/// ```rust,ignore
/// if A IS NODATA {
///     B
/// } else {
///     A
/// }
/// ```
///
/// Function calls can be used to access utility functions.
///
/// ```rust,ignore
/// max(A, 0)
/// ```
///
/// Currently, the following functions are available:
///
/// - `abs(a)`: absolute value
/// - `min(a, b)`, `min(a, b, c)`: minimum value
/// - `max(a, b)`, `max(a, b, c)`: maximum value
/// - `sqrt(a)`: square root
/// - `ln(a)`: natural logarithm
/// - `log10(a)`: base 10 logarithm
/// - `cos(a)`, `sin(a)`, `tan(a)`, `acos(a)`, `asin(a)`, `atan(a)`: trigonometric functions
/// - `pi()`, `e()`: mathematical constants
/// - `round(a)`, `ceil(a)`, `floor(a)`: rounding functions
/// - `mod(a, b)`: division remainder
/// - `to_degrees(a)`, `to_radians(a)`: conversion to degrees or radians
///
/// To generate more complex expressions, it is possible to have variable assignments.
///
/// ```rust,ignore
/// let mean = (A + B) / 2;
/// let coefficient = 0.357;
/// mean * coefficient
/// ```
///
/// Note, that all assignments are separated by semicolons.
/// However, the last expression must be without a semicolon.
#[api_operator(
    title = "Raster Expression",
    examples(json!({
        "type": "Expression",
        "params": {
            "expression": "(A - B) / (A + B)",
            "outputType": "F32",
            "outputBand": {
                "name": "NDVI",
                "measurement": { "type": "unitless" },
            },
            "mapNoData": true
        },
        "sources": {
            "raster": {
                "type": "GdalSource",
                "params": {
                    "data": "ndvi"
                }
            }
        }
    })),
)]
pub struct Expression {
    pub params: ExpressionParameters,
    pub sources: Box<SingleRasterSource>,
}

/// ## Types
///
/// The following describes the types used in the parameters.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExpressionParameters {
    /// Expression script
    ///
    /// Example: `"(A - B) / (A + B)"`
    #[schema(examples("(A - B) / (A + B)"))]
    pub expression: String,
    /// A raster data type for the output
    #[schema(examples("F32"))]
    pub output_type: RasterDataType,
    /// Description about the output
    #[schema(
        nullable = false /* cannot be null, but left out, avoids `Option<Option<_>>` in openapi client  */,
        examples(json!({
            "name": "NDVI",
            "measurement": { "type": "unitless" },
        }))
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_band: Option<RasterBandDescriptor>,
    /// Should NO DATA values be mapped with the `expression`? Otherwise, they are mapped automatically to NO DATA.
    #[schema(examples(true))]
    pub map_no_data: bool,
}

impl TryFrom<Expression> for OperatorsExpression {
    type Error = anyhow::Error;

    fn try_from(value: Expression) -> Result<Self, Self::Error> {
        Ok(OperatorsExpression {
            params: OperatorsExpressionParameters {
                expression: value.params.expression,
                output_type: value.params.output_type.into(),
                output_band: value.params.output_band.map(Into::into),
                map_no_data: value.params.map_no_data,
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type", content = "values")]
pub enum RenameBands {
    #[schema(title = "Default")]
    Default,
    #[schema(title = "Suffix")]
    Suffix(Vec<String>),
    #[schema(title = "Rename")]
    Rename(Vec<String>),
}

impl From<RenameBands> for geoengine_datatypes::raster::RenameBands {
    fn from(value: RenameBands) -> Self {
        match value {
            RenameBands::Default => Self::Default,
            RenameBands::Suffix(values) => Self::Suffix(values),
            RenameBands::Rename(values) => Self::Rename(values),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(untagged)]
pub enum BandsByNameOrIndex {
    /// Select bands by their names.
    Name(Vec<String>),
    /// Select bands by zero-based band indices.
    Index(Vec<usize>),
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub enum DeriveOutRasterSpecsSource {
    /// Derive output bounds from source data bounds.
    DataBounds,
    /// Derive output bounds from the target projection bounds.
    #[default]
    ProjectionBounds,
}

impl From<DeriveOutRasterSpecsSource> for OperatorsDeriveOutRasterSpecsSource {
    fn from(value: DeriveOutRasterSpecsSource) -> Self {
        match value {
            DeriveOutRasterSpecsSource::DataBounds => Self::DataBounds,
            DeriveOutRasterSpecsSource::ProjectionBounds => Self::ProjectionBounds,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum InterpolationResolution {
    Resolution(InterpolationResolutionResolution),
    Fraction(InterpolationResolutionFraction),
}

#[type_tag(value = "resolution")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct InterpolationResolutionResolution {
    /// Explicit output resolution (`x`, `y`) in target coordinates.
    pub x: f64,
    pub y: f64,
}

#[type_tag(value = "fraction")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct InterpolationResolutionFraction {
    /// Scaling factor in x/y direction.
    pub x: f64,
    pub y: f64,
}

impl From<InterpolationResolution> for OperatorsInterpolationResolution {
    fn from(value: InterpolationResolution) -> Self {
        match value {
            InterpolationResolution::Resolution(InterpolationResolutionResolution {
                r#type: _,
                x,
                y,
            }) => Self::Resolution(geoengine_datatypes::primitives::SpatialResolution { x, y }),
            InterpolationResolution::Fraction(InterpolationResolutionFraction {
                r#type: _,
                x,
                y,
            }) => Self::Fraction(OperatorsFraction { x, y }),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum InterpolationMethod {
    /// Nearest-neighbor interpolation.
    NearestNeighbor,
    /// Bilinear interpolation.
    BiLinear,
}

impl From<InterpolationMethod> for OperatorsInterpolationMethod {
    fn from(value: InterpolationMethod) -> Self {
        match value {
            InterpolationMethod::NearestNeighbor => Self::NearestNeighbor,
            InterpolationMethod::BiLinear => Self::BiLinear,
        }
    }
}

/// Aggregation methods for `TemporalRasterAggregation`.
///
/// Available variants are `min`, `max`, `first`, `last`, `mean`, `sum`, `count`, and `percentileEstimate`.
/// Encountering NO DATA makes the aggregation result NO DATA unless `ignoreNoData` is `true`.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum Aggregation {
    Min(AggregationMin),
    Max(AggregationMax),
    First(AggregationFirst),
    Last(AggregationLast),
    Mean(AggregationMean),
    Sum(AggregationSum),
    Count(AggregationCount),
    PercentileEstimate(AggregationPercentileEstimate),
}

#[type_tag(value = "min")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AggregationMin {
    pub ignore_no_data: bool,
}

#[type_tag(value = "max")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AggregationMax {
    pub ignore_no_data: bool,
}

#[type_tag(value = "first")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AggregationFirst {
    pub ignore_no_data: bool,
}

#[type_tag(value = "last")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AggregationLast {
    pub ignore_no_data: bool,
}

#[type_tag(value = "mean")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AggregationMean {
    pub ignore_no_data: bool,
}

#[type_tag(value = "sum")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AggregationSum {
    pub ignore_no_data: bool,
}

#[type_tag(value = "count")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AggregationCount {
    pub ignore_no_data: bool,
}

#[type_tag(value = "percentileEstimate")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AggregationPercentileEstimate {
    pub ignore_no_data: bool,
    pub percentile: f64,
}

impl From<Aggregation> for OperatorsAggregation {
    fn from(value: Aggregation) -> Self {
        match value {
            Aggregation::Min(AggregationMin {
                r#type: _,
                ignore_no_data,
            }) => Self::Min { ignore_no_data },
            Aggregation::Max(AggregationMax {
                r#type: _,
                ignore_no_data,
            }) => Self::Max { ignore_no_data },
            Aggregation::First(AggregationFirst {
                r#type: _,
                ignore_no_data,
            }) => Self::First { ignore_no_data },
            Aggregation::Last(AggregationLast {
                r#type: _,
                ignore_no_data,
            }) => Self::Last { ignore_no_data },
            Aggregation::Mean(AggregationMean {
                r#type: _,
                ignore_no_data,
            }) => Self::Mean { ignore_no_data },
            Aggregation::Sum(AggregationSum {
                r#type: _,
                ignore_no_data,
            }) => Self::Sum { ignore_no_data },
            Aggregation::Count(AggregationCount {
                r#type: _,
                ignore_no_data,
            }) => Self::Count { ignore_no_data },
            Aggregation::PercentileEstimate(AggregationPercentileEstimate {
                r#type: _,
                ignore_no_data,
                percentile,
            }) => Self::PercentileEstimate {
                ignore_no_data,
                percentile,
            },
        }
    }
}

/// The `Reprojection` operator reprojects data from one spatial reference system to another.
/// It accepts exactly one input which can either be a raster or a vector data stream.
/// The operator produces all data that, after reprojection, is contained in the query rectangle.
///
/// ## Data Type Specifics
///
/// The concrete behavior depends on the data type.
///
/// ### Vector Data
///
/// The operator reprojects all coordinates of the features individually.
/// The result contains all features that, after reprojection, are intersected by the query rectangle.
///
/// ### Raster Data
///
/// To create tiles in the target projection, the operator loads corresponding tiles in the source projection.
/// For each output pixel, the value of the nearest input pixel is used.
///
/// If parts of a tile are outside of the source extent after projection, the operator produces NO DATA values.
///
/// ## Errors
///
/// The operator returns an error if the target projection is unknown or if input data cannot be reprojected.
#[api_operator(
    title = "Reprojection",
    examples(json!({
        "type": "Reprojection",
        "params": {
            "deriveOutSpec": "projectionBounds",
            "targetSpatialReference": "EPSG:32632"
        },
        "sources": {
            "source": {
                "type": "MockPointSource",
                "params": {
                    "points": [{ "x": 8.77069, "y": 50.80904 }],
                    "spatialBounds": { "type": "none" }
                }
            }
        }
    }))
)]
pub struct Reprojection {
    pub params: ReprojectionParameters,
    pub sources: Box<SingleRasterOrVectorSource>,
}

/// Parameters for the `Reprojection` operator.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReprojectionParameters {
    /// Target spatial reference system.
    #[schema(value_type = String, examples("EPSG:32632"))]
    pub target_spatial_reference: SpatialReference,
    /// Controls how raster output bounds are derived.
    ///
    /// The default `projectionBounds` usually keeps a projection-aligned target grid,
    /// while `dataBounds` derives it directly from source data bounds.
    #[schema(examples("projectionBounds"))]
    #[serde(default)]
    pub derive_out_spec: DeriveOutRasterSpecsSource,
}

impl TryFrom<Reprojection> for OperatorsReprojection {
    type Error = anyhow::Error;

    fn try_from(value: Reprojection) -> Result<Self, Self::Error> {
        Ok(OperatorsReprojection {
            params: OperatorsReprojectionParameters {
                target_spatial_reference: value.params.target_spatial_reference.into(),
                derive_out_spec: value.params.derive_out_spec.into(),
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `TemporalRasterAggregation` operator aggregates a raster time series into uniform time windows.
/// The output starts with the first window that contains the query start and contains all windows
/// that overlap the query interval.
///
/// Pixel values are computed by aggregating all input rasters that contribute to the current window.
///
/// ## Errors
///
/// If the aggregation method is `first`, `last`, or `mean` and the input raster has no NO DATA value,
/// an error is returned.
#[api_operator(
    title = "Temporal Raster Aggregation",
    examples(json!({
        "type": "TemporalRasterAggregation",
        "params": {
            "aggregation": { "type": "mean", "ignoreNoData": true },
            "window": { "granularity": "months", "step": 1 }
        },
        "sources": {
            "raster": {
                "type": "Expression",
                "params": {
                    "expression": "(A - B) / (A + B)",
                    "outputType": "F32",
                    "outputBand": {
                        "name": "NDVI",
                        "measurement": { "type": "unitless" }
                    },
                    "mapNoData": false
                },
                "sources": {
                    "raster": {
                        "type": "GdalSource",
                        "params": { "data": "ndvi" }
                    }
                }
            }
        }
    }))
)]
pub struct TemporalRasterAggregation {
    pub params: TemporalRasterAggregationParameters,
    pub sources: Box<SingleRasterSource>,
}

/// Parameters for the `TemporalRasterAggregation` operator.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TemporalRasterAggregationParameters {
    /// Aggregation method for values within each time window.
    ///
    /// Encountering NO DATA makes the aggregation result NO DATA unless
    /// `ignoreNoData` is `true` for the selected aggregation variant.
    #[schema(examples(json!({ "type": "mean", "ignoreNoData": true })))]
    pub aggregation: Aggregation,
    /// Window size and granularity for the output time series.
    #[schema(examples(json!({ "granularity": "months", "step": 1 })))]
    pub window: TimeStep,
    /// Optional reference timestamp used as the anchor for window boundaries.
    ///
    /// If omitted, windows are anchored at `1970-01-01T00:00:00Z`.
    #[schema(examples("2020-01-01T00:00:00.000Z"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_reference: Option<TimeInstance>,
    /// Optional output raster data type.
    #[schema(examples("F32"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_type: Option<RasterDataType>,
}

impl TryFrom<TemporalRasterAggregation> for OperatorsTemporalRasterAggregation {
    type Error = anyhow::Error;

    fn try_from(value: TemporalRasterAggregation) -> Result<Self, Self::Error> {
        Ok(OperatorsTemporalRasterAggregation {
            params: OperatorsTemporalRasterAggregationParameters {
                aggregation: value.params.aggregation.into(),
                window: value.params.window.into(),
                window_reference: value.params.window_reference.map(Into::into),
                output_type: value.params.output_type.map(Into::into),
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `RasterStacker` stacks all of its inputs into a single raster time series.
/// It queries all inputs and combines them by band, space, and then time.
///
/// The output raster has as many bands as the sum of all input bands.
/// Tiles are automatically temporally aligned.
///
/// All inputs must have the same data type and spatial reference.
///
#[api_operator(
    title = "Raster Stacker",
    examples(json!({
        "type": "RasterStacker",
        "params": {
            "renameBands": { "type": "default" }
        },
        "sources": {
            "rasters": [
                {
                    "type": "GdalSource",
                    "params": { "data": "example-a" }
                },
                {
                    "type": "GdalSource",
                    "params": { "data": "example-b" }
                }
            ]
        }
    }))
)]
pub struct RasterStacker {
    pub params: RasterStackerParameters,
    pub sources: Box<MultipleRasterSources>,
}

/// Parameters for the `RasterStacker` operator.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RasterStackerParameters {
    /// Strategy for deriving output band names.
    ///
    /// - `default`: appends ` (n)` with the smallest `n` that avoids a conflict.
    /// - `suffix`: appends one suffix per input.
    /// - `rename`: explicitly provides names for all resulting bands.
    #[schema(examples(json!({ "type": "default" }), json!({ "type": "suffix", "values": ["_a", "_b"] })))]
    pub rename_bands: RenameBands,
}

impl TryFrom<RasterStacker> for OperatorsRasterStacker {
    type Error = anyhow::Error;

    fn try_from(value: RasterStacker) -> Result<Self, Self::Error> {
        Ok(OperatorsRasterStacker {
            params: OperatorsRasterStackerParameters {
                rename_bands: value.params.rename_bands.into(),
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `RasterTypeConversion` operator changes the data type of raster pixels.
///
/// Applying this conversion may cause precision loss.
/// For example, converting `F32` value `3.1` to `U8` results in `3`.
///
/// If a value is outside of the range of the target data type,
/// it is clipped to the valid range of that type.
/// For example, converting `F32` value `300.0` to `U8` results in `255`.
///
#[api_operator(
    title = "Raster Type Conversion",
    examples(json!({
        "type": "RasterTypeConversion",
        "params": {
            "outputDataType": "U16"
        },
        "sources": {
            "raster": {
                "type": "GdalSource",
                "params": { "data": "example" }
            }
        }
    }))
)]
pub struct RasterTypeConversion {
    pub params: RasterTypeConversionParameters,
    pub sources: Box<SingleRasterSource>,
}

/// Parameters for the `RasterTypeConversion` operator.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RasterTypeConversionParameters {
    /// Output raster data type.
    #[schema(examples("U16"))]
    pub output_data_type: RasterDataType,
}

impl TryFrom<RasterTypeConversion> for OperatorsRasterTypeConversion {
    type Error = anyhow::Error;

    fn try_from(value: RasterTypeConversion) -> Result<Self, Self::Error> {
        Ok(OperatorsRasterTypeConversion {
            params: OperatorsRasterTypeConversionParameters {
                output_data_type: value.params.output_data_type.into(),
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `Interpolation` operator increases raster resolution by interpolating values of an input raster.
///
/// If queried with a resolution that is coarser than the input resolution,
/// interpolation is not applicable and an error is returned.
///
/// ## Resolution
///
/// The target resolution can be specified either as an explicit `Resolution` (in pixel units)
/// or as a `Fraction` that scales the input resolution.
///
/// ```rust,ignore
/// // Scale the input resolution by a factor of 2 in both x and y directions
/// InterpolationResolution::Fraction(Fraction { x: 2.0, y: 2.0 })
/// ```
///
/// ```rust,ignore
/// // Use an explicit resolution of 50×50 pixel units
/// InterpolationResolution::Resolution(SpatialResolution { x: 50.0, y: 50.0 })
/// ```
#[api_operator(
    title = "Interpolation",
    examples(json!({
        "type": "Interpolation",
        "params": {
            "interpolation": "nearestNeighbor",
            "outputResolution": {
                "type": "fraction",
                "x": 2.0,
                "y": 2.0
            }
        },
        "sources": {
            "raster": {
                "type": "MultiBandGdalSource",
                "params": { "data": "sentinel-2-l2a_EPSG32632_U8_20" }
            }
        }
    }))
)]
pub struct Interpolation {
    pub params: InterpolationParameters,
    pub sources: Box<SingleRasterSource>,
}

/// Parameters for the `Interpolation` operator.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct InterpolationParameters {
    /// Interpolation method.
    #[schema(examples("nearestNeighbor"))]
    pub interpolation: InterpolationMethod,
    /// Target output resolution.
    #[schema(examples(json!({ "type": "fraction", "x": 2.0, "y": 2.0 })))]
    pub output_resolution: InterpolationResolution,
    /// Optional reference point used to align the output grid origin.
    #[schema(examples(json!({ "x": 0.0, "y": 0.0 })))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_origin_reference: Option<Coordinate2D>,
}

impl TryFrom<Interpolation> for OperatorsInterpolation {
    type Error = anyhow::Error;

    fn try_from(value: Interpolation) -> Result<Self, Self::Error> {
        Ok(OperatorsInterpolation {
            params: OperatorsInterpolationParameters {
                interpolation: value.params.interpolation.into(),
                output_resolution: value.params.output_resolution.into(),
                output_origin_reference: value.params.output_origin_reference.map(Into::into),
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `Downsampling` operator decreases raster resolution by sampling values of an input raster.
///
/// If queried with a resolution that is finer than the input resolution,
/// downsampling is not applicable and an error is returned.
///
/// ## Resolution
///
/// The target resolution can be specified either as an explicit `Resolution` (in pixel units)
/// or as a `Fraction` that scales the input resolution.
///
/// ```rust,ignore
/// // Scale the input resolution by a factor of 2 in both x and y directions
/// DownsamplingResolution::Fraction(Fraction { x: 2.0, y: 2.0 })
/// ```
///
/// ```rust,ignore
/// // Use an explicit resolution of 200×200 pixel units
/// DownsamplingResolution::Resolution(SpatialResolution { x: 200.0, y: 200.0 })
/// ```
#[api_operator(
    title = "Downsampling",
    examples(json!({
        "type": "Downsampling",
        "params": {
            "samplingMethod": "nearestNeighbor",
            "outputResolution": {
                "type": "fraction",
                "x": 2.0,
                "y": 2.0
            }
        },
        "sources": {
            "raster": {
                "type": "MultiBandGdalSource",
                "params": { "data": "sentinel-2-l2a_EPSG32632_U8_20" }
            }
        }
    }))
)]
pub struct Downsampling {
    pub params: DownsamplingParameters,
    pub sources: Box<SingleRasterSource>,
}

/// Parameters for the `Downsampling` operator.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DownsamplingParameters {
    /// Downsampling method.
    #[schema(examples("nearestNeighbor"))]
    pub sampling_method: DownsamplingMethod,
    /// Target output resolution.
    #[schema(examples(json!({ "type": "fraction", "x": 2.0, "y": 2.0 })))]
    pub output_resolution: DownsamplingResolution,
    /// Optional reference point used to align the output grid origin.
    #[schema(examples(json!({ "x": 0.0, "y": 0.0 })))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_origin_reference: Option<Coordinate2D>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum DownsamplingMethod {
    /// Nearest-neighbor downsampling.
    NearestNeighbor,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum DownsamplingResolution {
    Resolution(DownsamplingResolutionResolution),
    Fraction(DownsamplingResolutionFraction),
}

#[type_tag(value = "resolution")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DownsamplingResolutionResolution {
    /// Explicit output resolution (`x`, `y`) in target coordinates.
    pub x: f64,
    pub y: f64,
}

#[type_tag(value = "fraction")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DownsamplingResolutionFraction {
    /// Scaling factor in x/y direction.
    pub x: f64,
    pub y: f64,
}

impl From<DownsamplingResolution> for OperatorsDownsamplingResolution {
    fn from(value: DownsamplingResolution) -> Self {
        match value {
            DownsamplingResolution::Resolution(DownsamplingResolutionResolution {
                r#type: _,
                x,
                y,
            }) => Self::Resolution(geoengine_datatypes::primitives::SpatialResolution { x, y }),
            DownsamplingResolution::Fraction(DownsamplingResolutionFraction {
                r#type: _,
                x,
                y,
            }) => Self::Fraction(OperatorsFraction { x, y }),
        }
    }
}

impl From<DownsamplingMethod> for OperatorsDownsamplingMethod {
    fn from(value: DownsamplingMethod) -> Self {
        match value {
            DownsamplingMethod::NearestNeighbor => Self::NearestNeighbor,
        }
    }
}

impl TryFrom<Downsampling> for OperatorsDownsampling {
    type Error = anyhow::Error;

    fn try_from(value: Downsampling) -> Result<Self, Self::Error> {
        Ok(OperatorsDownsampling {
            params: OperatorsDownsamplingParameters {
                sampling_method: value.params.sampling_method.into(),
                output_resolution: value.params.output_resolution.into(),
                output_origin_reference: value.params.output_origin_reference.map(Into::into),
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `BandFilter` operator selects bands from a raster source by band names or band indices.
///
/// It removes all non-selected bands while preserving the original order of remaining bands.
///
/// ## Errors
///
/// The operator returns an error if no bands are selected or if selected band names/indices
/// cannot be mapped to existing input bands.
#[api_operator(
    title = "Band Filter",
    examples(json!({
        "type": "BandFilter",
        "params": {
            "bands": ["nir", "red"]
        },
        "sources": {
            "raster": {
                "type": "MultiBandGdalSource",
                "params": { "data": "sentinel-2-l2a_EPSG32632_U16_10" }
            }
        }
    }))
)]
pub struct BandFilter {
    pub params: BandFilterParameters,
    pub sources: Box<SingleRasterSource>,
}

/// Parameters for the `BandFilter` operator.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BandFilterParameters {
    /// Selected bands either by names (e.g. `["nir", "red"]`) or indices (e.g. `[0, 2]`).
    #[schema(examples(json!(["nir", "red"]), json!([0, 2])))]
    pub bands: BandsByNameOrIndex,
}

impl From<BandsByNameOrIndex> for geoengine_operators::processing::BandsByNameOrIndex {
    fn from(value: BandsByNameOrIndex) -> Self {
        match value {
            BandsByNameOrIndex::Name(names) => Self::Name(names),
            BandsByNameOrIndex::Index(indices) => Self::Index(indices),
        }
    }
}

impl TryFrom<BandFilter> for OperatorsBandFilter {
    type Error = anyhow::Error;

    fn try_from(value: BandFilter) -> Result<Self, Self::Error> {
        Ok(OperatorsBandFilter {
            params: OperatorsBandFilterParameters {
                bands: value.params.bands.into(),
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `RasterVectorJoin` operator allows combining a single vector input and multiple raster inputs.
/// For each raster input, a new column is added to the collection from the vector input.
/// The new column contains the value of the raster at the location of the vector feature.
/// For features covering multiple pixels like `MultiPoints` or `MultiPolygons`, the value is calculated using an aggregation function selected by the user.
/// The same is true if the temporal extent of a vector feature covers multiple raster time steps.
/// More details are described below.
///
/// **Example**:
/// You have a collection of agricultural fields (`Polygons`) and a collection of raster images containing each pixel's monthly NDVI value.
/// For your application, you want to know the NDVI value of each field.
/// The `RasterVectorJoin` operator allows you to combine the vector and raster data and offers multiple spatial and temporal aggregation strategies.
/// For example, you can use the `first` aggregation function to get the NDVI value of the first pixel that intersects with each field.
/// This is useful for exploratory analysis since the computation is very fast.
/// To calculate the mean NDVI value of all pixels that intersect with the field you should use the `mean` aggregation function.
/// Since the NDVI data is a monthly time series, you have to specify the temporal aggregation function as well.
/// The default is `none` which will create a new feature for each month.
/// Other options are `first` and `mean` which will calculate the first or mean NDVI value for each field over time.
///
/// ## Errors
///
/// If the length of `names` is not equal to the number of raster inputs, an error is thrown.
///
#[api_operator(
    title = "Raster Vector Join",
    examples(json!({
        "type": "RasterVectorJoin",
        "params": {
            "names": ["NDVI"],
            "featureAggregation": "first",
            "temporalAggregation": "mean",
            "temporalAggregationIgnoreNoData": true
        },
        "sources": {
            "vector": {
                "type": "OgrSource",
                "params": {
                    "data": "places"
                }
            },
            "rasters": [{
                "type": "GdalSource",
                "params": {
                "data": "ndvi"
                }
            }]
        }
    }))
)]
pub struct RasterVectorJoin {
    pub params: RasterVectorJoinParameters,
    pub sources: Box<SingleVectorMultipleRasterSources>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RasterVectorJoinParameters {
    /// Specify how the new column names are derived from the raster band names.
    ///
    /// The `ColumnNames` type is used to specify how the new column names are derived from the raster band names.
    ///
    /// - **default**: Appends " (n)" to the band name with the smallest `n` that avoids a conflict.
    /// - **suffix**: Specifies a suffix for each input, to be appended to the band names.
    /// - **rename**: A list of names for each new column.
    ///
    #[schema(examples(
        json!({"type": "default"}),
        json!({"type": "suffix", "values": ["_sentinel2"]}),
        json!({"type": "rename", "values": ["red", "green", "blue"]}),
    ))]
    pub names: ColumnNames,
    /// The aggregation function to use for features covering multiple pixels.
    #[schema(examples("first"))]
    pub feature_aggregation: FeatureAggregationMethod,
    /// Whether to ignore no data values in the aggregation. Defaults to `false`.
    #[serde(default)]
    #[schema(examples(true))]
    pub feature_aggregation_ignore_no_data: bool,
    /// The aggregation function to use for features covering multiple (raster) time steps.
    #[schema(examples("mean"))]
    pub temporal_aggregation: TemporalAggregationMethod,
    /// Whether to ignore no data values in the aggregation. Defaults to `false`.
    #[serde(default)]
    #[schema(examples(true))]
    pub temporal_aggregation_ignore_no_data: bool,
}

impl TryFrom<RasterVectorJoin> for OperatorsRasterVectorJoin {
    type Error = anyhow::Error;

    fn try_from(value: RasterVectorJoin) -> Result<Self, Self::Error> {
        Ok(OperatorsRasterVectorJoin {
            params: OperatorsRasterVectorJoinParameters {
                names: value.params.names.into(),
                feature_aggregation: value.params.feature_aggregation.into(),
                feature_aggregation_ignore_no_data: value.params.feature_aggregation_ignore_no_data,
                temporal_aggregation: value.params.temporal_aggregation.into(),
                temporal_aggregation_ignore_no_data: value
                    .params
                    .temporal_aggregation_ignore_no_data,
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `BandNeighborhoodAggregate` operator performs a pixel-wise aggregate function over neighboring bands.
/// The output is a raster time series with the same number of bands as the input raster.
/// The pixel values are replaced by the result of the aggregate function.
/// This allows, for example, the computation of a moving average over the bands of a raster time series.
///
/// ## Types
///
/// The following describes the types used in the parameters.
///
/// ### NeighborhoodAggregate
///
/// There are several types of neighborhood aggregate functions.
///
/// #### Average
///
/// This aggregate function computes the average of the neighboring bands.
/// The `windowSize` parameter defines the number of bands to consider for the average and must be an odd number.
/// For the borders, the window is reduced to the available bands.
///
/// #### `FirstDerivative`
///
/// This aggregate function computes an approximation of the first derivative of the neighboring bands using the central difference method.
/// To compute the distance between neighboring bands, a `bandDistance` parameter is required.
///
/// ## Errors
///
/// The operation fails if there are not enough bands in the input raster to compute the aggregate function or the number of bands does not match the requirements of the aggregate function.
#[api_operator(
    title = "Band Neighborhood Aggregate",
    examples(json!({
        "type": "BandNeighborhoodAggregate",
        "params": {
            "aggregate": { "type": "average", "windowSize": 3 }
        },
        "sources": {
            "raster": {
                "type": "GdalSource",
                "params": { "data": "sentinel-2" }
            }
        }
    }))
)]
pub struct BandNeighborhoodAggregate {
    pub params: BandNeighborhoodAggregateParameters,
    pub sources: Box<SingleRasterSource>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BandNeighborhoodAggregateParameters {
    /// The aggregation method to apply to neighboring bands.
    ///
    /// The average method computes the mean over the current band window, while the
    /// first-derivative method approximates the derivative using the configured band distance.
    #[schema(examples(
        json!({ "type": "average", "windowSize": 3 }),
        json!({
            "type": "firstDerivative",
            "bandDistance": { "type": "equallySpaced", "distance": 1.0 }
        })
    ))]
    pub aggregate: BandNeighborhoodAggregateMethod,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum BandNeighborhoodAggregateMethod {
    FirstDerivative(BandNeighborhoodAggregateMethodFirstDerivative),
    Average(BandNeighborhoodAggregateMethodAverage),
}

#[type_tag(value = "firstDerivative")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BandNeighborhoodAggregateMethodFirstDerivative {
    /// Distance between neighboring bands used to approximate the first derivative.
    #[schema(examples(json!({
        "type": "equallySpaced",
        "distance": 1.0
    })))]
    pub band_distance: BandDistance,
}

#[type_tag(value = "average")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BandNeighborhoodAggregateMethodAverage {
    /// Number of neighboring bands to include in the moving average window.
    /// The window size must be odd; at the raster borders the window is reduced.
    #[schema(examples(3, 5))]
    pub window_size: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum BandDistance {
    EquallySpaced(BandDistanceEquallySpaced),
}

#[type_tag(value = "equallySpaced")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BandDistanceEquallySpaced {
    /// Constant distance between consecutive bands.
    #[schema(examples(1.0))]
    pub distance: f64,
}

impl From<BandDistance> for OperatorsBandDistance {
    fn from(value: BandDistance) -> Self {
        match value {
            BandDistance::EquallySpaced(BandDistanceEquallySpaced {
                r#type: _,
                distance,
            }) => Self::EquallySpaced { distance },
        }
    }
}

impl From<BandNeighborhoodAggregateMethod> for OperatorsBandNeighborhoodAggregateMethod {
    fn from(value: BandNeighborhoodAggregateMethod) -> Self {
        match value {
            BandNeighborhoodAggregateMethod::FirstDerivative(
                BandNeighborhoodAggregateMethodFirstDerivative {
                    r#type: _,
                    band_distance,
                },
            ) => Self::FirstDerivative {
                band_distance: band_distance.into(),
            },
            BandNeighborhoodAggregateMethod::Average(BandNeighborhoodAggregateMethodAverage {
                r#type: _,
                window_size,
            }) => Self::Average { window_size },
        }
    }
}

impl TryFrom<BandNeighborhoodAggregate> for OperatorsBandNeighborhoodAggregate {
    type Error = anyhow::Error;

    fn try_from(value: BandNeighborhoodAggregate) -> Result<Self, Self::Error> {
        Ok(OperatorsBandNeighborhoodAggregate {
            params: OperatorsBandNeighborhoodAggregateParameters {
                aggregate: value.params.aggregate.into(),
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `BandwiseExpression` operator performs a pixel-wise mathematical expression on each band of a raster source.
/// For more details on the expression syntax, see the `Expression` operator.
/// Note that in the `BandwiseExpression` operator it is only possible to map one pixel value to another and not reference any other pixels or bands.
/// The variable name for the pixel value is `x`.
///
/// ## Errors
///
/// The parsing of the expression can fail if there are, for example, syntax errors.
#[api_operator(
    title = "Bandwise Expression",
    examples(json!({
        "type": "BandwiseExpression",
        "params": {
            "expression": "x * 2.0",
            "outputType": "F64",
            "mapNoData": false
        },
        "sources": { "raster": { "type": "GdalSource", "params": { "data": "example" } } }
    }))
)]
pub struct BandwiseExpression {
    pub params: BandwiseExpressionParameters,
    pub sources: Box<SingleRasterSource>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BandwiseExpressionParameters {
    /// Scalar expression applied to each band value.
    #[schema(examples("x * 2.0", "ln(1 / x)"))]
    pub expression: String,
    /// Output raster data type for the computed band values.
    #[schema(examples("F32", "F64"))]
    pub output_type: RasterDataType,
    /// Whether NO DATA values should be mapped with the expression.
    /// Otherwise, they are mapped automatically to NO DATA.
    #[schema(examples(false, true))]
    pub map_no_data: bool,
}

impl TryFrom<BandwiseExpression> for OperatorsBandwiseExpression {
    type Error = anyhow::Error;

    fn try_from(value: BandwiseExpression) -> Result<Self, Self::Error> {
        Ok(OperatorsBandwiseExpression {
            params: OperatorsBandwiseExpressionParameters {
                expression: value.params.expression,
                output_type: value.params.output_type.into(),
                map_no_data: value.params.map_no_data,
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `ColumnRangeFilter` operator allows filtering `FeatureCollection`s.
/// Users can define one or more data ranges for a column in the data table that is then filtered.
/// The filter can be used for numerical as well as textual columns.
/// Each range is inclusive, i.e. `[start, end]` includes both the `start` and the `end` values.
///
/// ## Errors
///
/// If the value in the `column` parameter is not a column of the feature collection, an error is thrown.
#[api_operator(
    title = "Column Range Filter",
    examples(json!({
        "type": "ColumnRangeFilter",
        "params": {
            "column": "temperature",
            "ranges": [{ "type": "float", "start": 0.0, "end": 50.0 }],
            "keepNulls": true
        },
        "sources": {
            "vector": {
                "type": "OgrSource",
                "params": { "data": "example" }
            }
        }
    }))
)]
pub struct ColumnRangeFilter {
    pub params: ColumnRangeFilterParameters,
    pub sources: Box<SingleVectorSource>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ColumnRangeFilterParameters {
    /// Column name to filter on.
    #[schema(examples("temperature", "population", "name"))]
    pub column: String,
    /// One or more inclusive ranges for the selected column values.
    /// Numeric ranges use `[start, end]`; string ranges use `["a", "k"]`.
    #[schema(examples(
        json!([[0.0, 50.0]]),
        json!([[1000, 10000]]),
        json!([["a", "k"], ["v", "z"]])
    ))]
    pub ranges: Vec<StringOrNumberRange>,
    /// Whether rows whose column value is NULL should be retained.
    #[schema(examples(true, false))]
    pub keep_nulls: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(untagged)]
pub enum StringOrNumberRange {
    String([String; 2]),
    Float([f64; 2]),
    Int([i64; 2]),
}

impl From<StringOrNumberRange> for geoengine_operators::util::input::StringOrNumberRange {
    fn from(value: StringOrNumberRange) -> Self {
        match value {
            StringOrNumberRange::String([start, end]) => Self::String(start..=end),
            StringOrNumberRange::Float([start, end]) => Self::Float(start..=end),
            StringOrNumberRange::Int([start, end]) => Self::Int(start..=end),
        }
    }
}

impl TryFrom<ColumnRangeFilter> for OperatorsColumnRangeFilter {
    type Error = anyhow::Error;

    fn try_from(value: ColumnRangeFilter) -> Result<Self, Self::Error> {
        Ok(OperatorsColumnRangeFilter {
            params: OperatorsColumnRangeFilterParameters {
                column: value.params.column,
                ranges: value.params.ranges.into_iter().map(Into::into).collect(),
                keep_nulls: value.params.keep_nulls,
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `LineSimplification` operator allows simplifying `FeatureCollection`s of (multi-)lines or (multi-)polygons by removing vertices.
/// Users can select a simplification algorithm and specify an `epsilon` for parametrization.
/// Alternatively, they can omit the `epsilon`, which results in the epsilon being automatically determined by the query's spatial resolution.
///
/// ## Errors
///
/// - If `epsilon` is set but <= 0, an error is thrown.
/// - If the input is not a `MultiPolygon` or `MultiLineString`, an error is thrown.
#[api_operator(
    title = "Line Simplification",
    examples(json!({
        "type": "LineSimplification",
        "params": {
            "algorithm": "douglasPeucker",
            "epsilon": 0.5
        },
        "sources": {
            "vector": {
                "type": "OgrSource",
                "params": { "data": "roads" }
            }
        }
    }))
)]
pub struct LineSimplification {
    pub params: LineSimplificationParameters,
    pub sources: Box<SingleVectorSource>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LineSimplificationParameters {
    /// Simplification algorithm used to reduce the geometry complexity.
    #[schema(examples("douglasPeucker", "visvalingam"))]
    pub algorithm: LineSimplificationAlgorithm,
    /// Distance threshold for the simplification algorithm.
    #[schema(examples(0.5, 1.0))]
    pub epsilon: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum LineSimplificationAlgorithm {
    /// Removes vertices using the Douglas-Peucker algorithm.
    DouglasPeucker,
    /// Removes vertices using the Visvalingam algorithm.
    Visvalingam,
}

impl From<LineSimplificationAlgorithm> for OperatorsLineSimplificationAlgorithm {
    fn from(value: LineSimplificationAlgorithm) -> Self {
        match value {
            LineSimplificationAlgorithm::DouglasPeucker => Self::DouglasPeucker,
            LineSimplificationAlgorithm::Visvalingam => Self::Visvalingam,
        }
    }
}

impl TryFrom<LineSimplification> for OperatorsLineSimplification {
    type Error = anyhow::Error;

    fn try_from(value: LineSimplification) -> Result<Self, Self::Error> {
        Ok(OperatorsLineSimplification {
            params: OperatorsLineSimplificationParameters {
                algorithm: value.params.algorithm.into(),
                epsilon: value.params.epsilon,
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `NeighborhoodAggregate` operator computes an aggregate function for a pixel and its neighborhood.
/// The operator can be defined as a neighborhood matrix with either weights or predefined shapes and an aggregate function.
/// For each time step in the raster time series, the operator computes the aggregate for each pixel and its neighborhood.
///
/// ## Types
///
/// There are several types of neighborhoods. They define a matrix of weights. The rows and columns of this matrix must be odd.
///
/// ### `WeightsMatrix`
///
/// The weights matrix is defined as an n × m matrix of floating-point values. It is applied to the pixel and its neighborhood to serve as the input for the aggregate function.
///
/// ### Rectangle
///
/// The rectangle neighborhood is defined by its shape n × m. The result is a weights matrix with all weights set to `1.0`.
///
/// ### `AggregateFunction`
///
/// The aggregate function computes a single value from a set of values. Supported functions are `sum` and `standardDeviation`.
///
/// ## Errors
///
/// If the neighborhood rows or columns are not positive or odd, an error is thrown.
#[api_operator(
    title = "Neighborhood Aggregate",
    examples(json!({
        "type": "NeighborhoodAggregate",
        "params": {
            "neighborhood": { "type": "rectangle", "dimensions": [3, 3] },
            "aggregateFunction": "sum"
        },
        "sources": {
            "raster": {
                "type": "GdalSource",
                "params": { "data": "example" }
            }
        }
    }))
)]
pub struct NeighborhoodAggregate {
    pub params: NeighborhoodAggregateParameters,
    pub sources: Box<SingleRasterSource>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NeighborhoodAggregateParameters {
    /// Neighborhood definition applied around each pixel.
    /// This can be a rectangular neighborhood or a custom weights matrix.
    #[schema(examples(
        json!({ "type": "rectangle", "dimensions": [3, 3] }),
        json!({
            "type": "weightsMatrix",
            "weights": [
                [1.0, 2.0, 3.0],
                [4.0, 5.0, 6.0],
                [7.0, 8.0, 9.0]
            ]
        })
    ))]
    pub neighborhood: NeighborhoodKernel,
    /// Aggregate function applied across the neighborhood values.
    #[schema(examples("sum", "standardDeviation"))]
    pub aggregate_function: NeighborhoodAggregateMethod,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum NeighborhoodAggregateMethod {
    Sum(NeighborhoodAggregateMethodSum),
    StandardDeviation(NeighborhoodAggregateMethodStandardDeviation),
}

#[type_tag(value = "sum")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NeighborhoodAggregateMethodSum {}

#[type_tag(value = "standardDeviation")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NeighborhoodAggregateMethodStandardDeviation {}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum NeighborhoodKernel {
    Rectangle(NeighborhoodKernelRectangle),
    WeightsMatrix(NeighborhoodKernelWeightsMatrix),
}

#[type_tag(value = "rectangle")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NeighborhoodKernelRectangle {
    /// Width and height of the rectangular neighborhood in pixels.
    #[schema(examples(json!([3, 3]), json!([5, 5])))]
    pub dimensions: [usize; 2],
}

#[type_tag(value = "weightsMatrix")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NeighborhoodKernelWeightsMatrix {
    /// Explicit matrix of neighborhood weights.
    #[schema(examples(json!([
        [1.0, 0.0, -1.0],
        [2.0, 0.0, -2.0],
        [1.0, 0.0, -1.0]
    ])))]
    pub weights: Vec<Vec<f64>>,
}

impl From<NeighborhoodKernel> for geoengine_operators::processing::NeighborhoodParams {
    fn from(value: NeighborhoodKernel) -> Self {
        match value {
            NeighborhoodKernel::Rectangle(NeighborhoodKernelRectangle {
                r#type: _,
                dimensions,
            }) => Self::Rectangle { dimensions },
            NeighborhoodKernel::WeightsMatrix(NeighborhoodKernelWeightsMatrix {
                r#type: _,
                weights,
            }) => Self::WeightsMatrix { weights },
        }
    }
}

impl From<NeighborhoodAggregateMethod>
    for geoengine_operators::processing::AggregateFunctionParams
{
    fn from(value: NeighborhoodAggregateMethod) -> Self {
        match value {
            NeighborhoodAggregateMethod::Sum(_) => Self::Sum,
            NeighborhoodAggregateMethod::StandardDeviation(_) => Self::StandardDeviation,
        }
    }
}

impl TryFrom<NeighborhoodAggregate> for OperatorsNeighborhoodAggregate {
    type Error = anyhow::Error;

    fn try_from(value: NeighborhoodAggregate) -> Result<Self, Self::Error> {
        Ok(OperatorsNeighborhoodAggregate {
            params: OperatorsNeighborhoodAggregateParameters {
                neighborhood: value.params.neighborhood.into(),
                aggregate_function: value.params.aggregate_function.into(),
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `Onnx` operator applies a machine learning model to a raster stack.
#[api_operator(
    title = "ONNX",
    examples(json!({
        "type": "Onnx",
        "params": {
            "model": "my-model"
        },
        "sources": {
            "raster": {
                "type": "GdalSource",
                "params": { "data": "example" }
            }
        }
    }))
)]
pub struct Onnx {
    pub params: OnnxParameters,
    pub sources: Box<SingleRasterSource>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OnnxParameters {
    pub model: crate::api::model::datatypes::MlModelName,
}

impl TryFrom<Onnx> for OperatorsOnnx {
    type Error = anyhow::Error;

    fn try_from(value: Onnx) -> Result<Self, Self::Error> {
        Ok(OperatorsOnnx {
            params: OperatorsOnnxParameters {
                model: value.params.model.into(),
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `PointInPolygonFilter` operator filters point features of a (multi-)point collection with polygons.
/// In more detail, the points of each feature are checked against the polygons of the other collection.
/// If one or more point is included in any polygon's ring, the feature is included in the output.
///
/// For instance, you can filter tree features inside the polygons of a forest.
/// All features, that weren't inside any forest polygon, are considered either part of another forest or outliers and are thus removed.
///
/// ## Errors
///
/// If the `points` vector input is not a (multi-)point feature collection, an error is thrown.
///
/// If the `polygons` vector input is not a (multi-)polygon feature collection, an error is thrown.
///
#[api_operator(
    title = "Point In Polygon Filter",
    examples(json!({
        "type": "PointInPolygonFilter",
        "params": {},
        "sources": {
            "points": {
                "type": "OgrSource",
                "params": {
                    "data": "places",
                    "attributeProjection": ["name", "population"]
                }
            },
            "polygons": {
                "type": "OgrSource",
                "params": {
                    "data": "germany_outline"
                }
            }
        }
    }))
)]
pub struct PointInPolygonFilter {
    pub params: PointInPolygonFilterParameters,
    pub sources: PointInPolygonFilterSource,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PointInPolygonFilterParameters {}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PointInPolygonFilterSource {
    /// Point collection that is checked against the polygon collection.
    pub points: Box<VectorOperator>,
    /// Polygon collection used as the filter mask.
    pub polygons: Box<VectorOperator>,
}

impl TryFrom<PointInPolygonFilterSource> for OperatorsPointInPolygonFilterSource {
    type Error = anyhow::Error;

    fn try_from(value: PointInPolygonFilterSource) -> Result<Self, Self::Error> {
        Ok(Self {
            points: (*value.points).try_into()?,
            polygons: (*value.polygons).try_into()?,
        })
    }
}

impl TryFrom<PointInPolygonFilter> for OperatorsPointInPolygonFilter {
    type Error = anyhow::Error;

    fn try_from(value: PointInPolygonFilter) -> Result<Self, Self::Error> {
        Ok(OperatorsPointInPolygonFilter {
            params: OperatorsPointInPolygonFilterParameters {},
            sources: value.sources.try_into()?,
        })
    }
}

/// The `Radiance` operator converts raw raster values to radiance.
#[api_operator(
    title = "Radiance",
    examples(json!({
        "type": "Radiance",
        "params": {},
        "sources": { "raster": { "type": "GdalSource", "params": { "data": "msg_raw" } } }
    }))
)]
pub struct Radiance {
    pub params: RadianceParameters,
    pub sources: Box<SingleRasterSource>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RadianceParameters {}

impl TryFrom<Radiance> for OperatorsRadiance {
    type Error = anyhow::Error;

    fn try_from(value: Radiance) -> Result<Self, Self::Error> {
        Ok(OperatorsRadiance {
            params: OperatorsRadianceParameters {},
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The raster scaling operator scales/unscales the values of a raster by a given slope factor and offset.
/// This allows to shrink and expand the value range of the pixel values needed to store a raster. It also allows to shift values to all-positive values and back.
/// We use the [GDAL](https://gdal.org/index.html) terms of [scale](https://gdal.org/programs/gdal_translate.html#cmdoption-gdal_translate-scale) and [unscale](https://gdal.org/programs/gdal_translate.html#cmdoption-gdal_translate-unscale).
/// Raster data is often scaled to reduce memory/storage consumption.
/// To get the "real" raster values the unscale operation is applied.
/// Keep in mind that scaling might reduce the precision of the pixel values.
/// (To actually reduce the size of the raster, use the [raster type conversion operator](/docs/operators/rastertypeconversion) and transform to a smaller datatype after scaling.)
///
/// The operator applies the following formulas to every pixel.
///
/// For _unscaling_ the formula is: `p_new = p_old * slope + offset`. The key for this mode is `mulSlopeAddOffset`.
///
/// For _scaling_ the formula is: `p_new = (p_old - offset) / slope`. The key for this mode is `subOffsetDivSlope`.
///
/// `p_old` and `p_new` refer to the old and new pixel value. The slope and offset values are either properties attached to the input raster or a fixed value.
///
/// An example for Meteosat Second Generation properties is:
///
/// - offset: `msg.calibration_offset`
/// - slope: `msg.calibration_slope`
///
/// \*if no `outputMeasurement` is given, the measurement of the input raster is used.
///
/// The `RasterScaling` operator expects exactly one _raster_ input.
///
/// | Parameter | Type |
/// | --------- | ----- |
/// | `source` | `SingleRasterSource` |
///
/// ## Types
///
/// The following describes the types used in the parameters.
///
/// ### `SlopeOffsetSelection`
///
/// The `SlopeOffsetSelection` type is used to specify a metadata key or a constant value.
///
/// | Value | Description |
/// | ----- | ----------- |
/// | `{"type": "auto"}` * | Use slope and offset from the tiles properties |
/// | `{"type": "constant", "value": number}` | A constant value. |
/// | `{"type": "metadataKey", "domain": string, "key": string}` | A metadata key to lookup dynamic values from raster (tile) properties. |
///
/// * if set to `"auto"`, the operator will use the values from the dedicated (GDAL) raster properties for scale and offset.
///
#[api_operator(
    title = "Raster Scaling",
    examples(json!({
      "type": "RasterScaling",
      "params": {
        "slope": {
          "type": "metadataKey",
          "domain": "",
          "key": "scale"
        },
        "offset": {
          "type": "constant",
          "value": 1.0
        },
        "outputMeasurement": null,
        "scalingMode": "mulSlopeAddOffset"
      },
      "sources": {
        "raster": {
          "type": "GdalSource",
          "params": {
            "data": "modis-b6"
          }
        }
      }
    }))
)]
pub struct RasterScaling {
    pub params: RasterScalingParameters,
    pub sources: Box<SingleRasterSource>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RasterScalingParameters {
    /// The slope factor used for the scaling or unscaling operation.
    #[schema(examples(json!({ "type": "metadataKey", "domain": "", "key": "scale" }))) ]
    pub slope: SlopeOffsetSelection,
    /// The offset used for the scaling or unscaling operation.
    #[schema(examples(json!({ "type": "constant", "value": 1.0 }))) ]
    pub offset: SlopeOffsetSelection,
    /// Optional output measurement of the result of the scaling operation.
    #[schema(examples(json!({ "type": "continuous", "measurement": "Reflectance", "unit": "%" }))) ]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_measurement: Option<Measurement>,
    /// Selects whether the values are scaled or unscaled.
    #[schema(examples("mulSlopeAddOffset"))]
    pub scaling_mode: ScalingMode,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum ScalingMode {
    MulSlopeAddOffset(ScalingModeMulSlopeAddOffset),
    SubOffsetDivSlope(ScalingModeSubOffsetDivSlope),
}

#[type_tag(value = "mulSlopeAddOffset")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScalingModeMulSlopeAddOffset {}

#[type_tag(value = "subOffsetDivSlope")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScalingModeSubOffsetDivSlope {}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum SlopeOffsetSelection {
    Auto(SlopeOffsetSelectionAuto),
    MetadataKey(SlopeOffsetSelectionMetadataKey),
    Constant(SlopeOffsetSelectionConstant),
}

#[type_tag(value = "auto")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SlopeOffsetSelectionAuto {}

#[type_tag(value = "metadataKey")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SlopeOffsetSelectionMetadataKey {
    /// Optional metadata domain to look up the raster property.
    #[schema(examples("", "msg"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    /// Raster property key that contains the slope or offset value.
    #[schema(examples("scale", "calibration_slope"))]
    pub key: String,
}

#[type_tag(value = "constant")]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SlopeOffsetSelectionConstant {
    /// Fixed slope or offset value.
    #[schema(examples(0.1, 1.0))]
    pub value: f64,
}

impl From<SlopeOffsetSelection> for OperatorsSlopeOffsetSelection {
    fn from(value: SlopeOffsetSelection) -> Self {
        match value {
            SlopeOffsetSelection::Auto(_) => Self::Auto,
            SlopeOffsetSelection::MetadataKey(SlopeOffsetSelectionMetadataKey {
                r#type: _,
                domain,
                key,
            }) => {
                Self::MetadataKey(geoengine_datatypes::raster::RasterPropertiesKey { domain, key })
            }
            SlopeOffsetSelection::Constant(SlopeOffsetSelectionConstant { r#type: _, value }) => {
                Self::Constant { value }
            }
        }
    }
}

impl From<ScalingMode> for OperatorsScalingMode {
    fn from(value: ScalingMode) -> Self {
        match value {
            ScalingMode::MulSlopeAddOffset(_) => Self::MulSlopeAddOffset,
            ScalingMode::SubOffsetDivSlope(_) => Self::SubOffsetDivSlope,
        }
    }
}

impl TryFrom<RasterScaling> for OperatorsRasterScaling {
    type Error = anyhow::Error;

    fn try_from(value: RasterScaling) -> Result<Self, Self::Error> {
        Ok(OperatorsRasterScaling {
            params: OperatorsRasterScalingParameters {
                slope: value.params.slope.into(),
                offset: value.params.offset.into(),
                output_measurement: value.params.output_measurement.map(Into::into),
                scaling_mode: value.params.scaling_mode.into(),
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `Rasterization` operator creates a raster from a point vector source.
/// It offers two options for rasterization: A grid rasterization and a (gaussian) density rasterization (heatmap).
///
/// ## Errors
///
/// If the `cutoff` is not in `[0, 1)` or the `stddev` is negative, an error will be thrown.
///
#[api_operator(
    title = "Rasterization",
    examples(
        json!({
          "type": "Raster",
          "operator": {
            "type": "Rasterization",
            "params": {
              "type": "grid",
              "spatialResolution": {
                "x": 10,
                "y": 10
              },
              "gridSizeMode": "fixed",
              "originCoordinate": {
                "x": 0,
                "y": 0
              }
            },
            "sources": {
              "vector": {
                "type": "OgrSource",
                "params": {
                  "data": "ne_10m_ports",
                  "attributeProjection": null,
                  "attributeFilters": null
                }
              }
            }
          }
        }),
        json!({
          "type": "Raster",
          "operator": {
            "type": "Rasterization",
            "params": {
              "type": "density",
              "cutoff": 0.01,
              "stddev": 1
            },
            "sources": {
              "vector": {
                "type": "OgrSource",
                "params": {
                  "data": "ne_10m_ports",
                  "attributeProjection": null,
                  "attributeFilters": null
                }
              }
            }
          }
        }),
    )
)]
pub struct Rasterization {
    pub params: RasterizationParameters,
    pub sources: Box<SingleVectorSource>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RasterizationParameters {
    /// Spatial resolution of the output grid in x/y direction.
    #[schema(examples(json!({ "x": 10.0, "y": 10.0 }))) ]
    pub spatial_resolution: SpatialResolution,
    /// Origin coordinate to which the raster grid is aligned.
    #[schema(examples(json!({ "x": 0.0, "y": 0.0 }))) ]
    pub origin_coordinate: Coordinate2D,
    /// Optional density parameters for a Gaussian density rasterization.
    #[schema(examples(json!({ "cutoff": 0.01, "stddev": 1.0 }))) ]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub density_params: Option<DensityParams>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DensityParams {
    /// Cutoff percentage of maximum density, expressed as a fraction in [0, 1).
    #[schema(examples(0.01))]
    pub cutoff: f64,
    /// Standard deviation parameter for the Gaussian density function.
    #[schema(examples(1.0))]
    pub stddev: f64,
}

impl TryFrom<Rasterization> for OperatorsRasterization {
    type Error = anyhow::Error;

    fn try_from(value: Rasterization) -> Result<Self, Self::Error> {
        Ok(OperatorsRasterization {
            params: OperatorsRasterizationParameters {
                spatial_resolution: value.params.spatial_resolution.into(),
                origin_coordinate: value.params.origin_coordinate.into(),
                density_params: value
                    .params
                    .density_params
                    .map(|p| OperatorsDensityParameters {
                        cutoff: p.cutoff,
                        stddev: p.stddev,
                    }),
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `Reflectance` operator converts radiance values to reflectance.
#[api_operator(
    title = "Reflectance",
    examples(json!({
        "type": "Reflectance",
        "params": { "solarCorrection": true, "forceHRV": false },
        "sources": { "raster": { "type": "GdalSource", "params": { "data": "msg" } } }
    }))
)]
pub struct Reflectance {
    pub params: ReflectanceParameters,
    pub sources: Box<SingleRasterSource>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReflectanceParameters {
    pub solar_correction: bool,
    #[serde(rename = "forceHRV")]
    pub force_hrv: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub force_satellite: Option<u8>,
}

impl TryFrom<Reflectance> for OperatorsReflectance {
    type Error = anyhow::Error;

    fn try_from(value: Reflectance) -> Result<Self, Self::Error> {
        Ok(OperatorsReflectance {
            params: OperatorsReflectanceParameters {
                solar_correction: value.params.solar_correction,
                force_hrv: value.params.force_hrv,
                force_satellite: value.params.force_satellite,
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `Temperature` operator converts raw raster values to temperature.
#[api_operator(
    title = "Temperature",
    examples(json!({
        "type": "Temperature",
        "params": { "forceSatellite": 8 },
        "sources": { "raster": { "type": "GdalSource", "params": { "data": "msg_raw" } } }
    }))
)]
pub struct Temperature {
    pub params: TemperatureParameters,
    pub sources: Box<SingleRasterSource>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TemperatureParameters {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub force_satellite: Option<u8>,
}

impl TryFrom<Temperature> for OperatorsTemperature {
    type Error = anyhow::Error;

    fn try_from(value: Temperature) -> Result<Self, Self::Error> {
        Ok(OperatorsTemperature {
            params: OperatorsTemperatureParameters {
                force_satellite: value.params.force_satellite,
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `TimeProjection` projects vector dataset timestamps to new granularities and ranges.
/// The output is a new vector dataset with the same geometry and attributes as the input.
/// However, each time step is projected to a new time range.
/// Moreover, the [`QueryRectangle`'s](../datatypes/queryrectangle) temporal extent is enlarged as well to include the projected time range.
///
/// An example usage scenario is to transform snapshot observations into yearly time slices.
/// For instance, animal occurrences are observed at a daily granularity.
/// If you want to aggregate the data to a yearly granularity, you can use the `TimeProjection` operator.
/// This will change the validity of each element in the dataset to the full year where it was observed.
/// This is, for instance, useful when you want to combine it with raster time series and use different temporal semantics than the originally recorded validities.
///
/// ## Errors
///
/// If the `step` is negative, an error is thrown.
///
#[api_operator(
    title = "Time Projection",
    examples(json!({
        "type": "TimeProjection",
        "params": {
          "step": {
            "granularity": "years",
            "step": 1
          }
        },
        "sources": {
          "vector": {
            "type": "OgrSource",
            "params": {
              "data": "ndvi"
            }
          }
        }
    }))
)]
pub struct TimeProjection {
    pub params: TimeProjectionParameters,
    pub sources: Box<SingleVectorSource>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TimeProjectionParameters {
    /// Time granularity and size for the projection step.
    #[schema(examples(json!({ "granularity": "years", "step": 1 }))) ]
    pub step: crate::api::model::datatypes::TimeStep,
    /// Optional anchor point for the time step.
    #[schema(examples("2010-01-01T00:00:00Z"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_reference: Option<TimeInstance>,
}

impl TryFrom<TimeProjection> for OperatorsTimeProjection {
    type Error = anyhow::Error;

    fn try_from(value: TimeProjection) -> Result<Self, Self::Error> {
        Ok(OperatorsTimeProjection {
            params: OperatorsTimeProjectionParameters {
                step: value.params.step.into(),
                step_reference: value.params.step_reference.map(Into::into),
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `TimeShift` operator allows retrieving data temporally relative to the actual [`QueryRectangle`](../datatypes/queryrectangle).
/// It shifts the query rectangle by a given amount of time and modifies the result data accordingly.
/// Users have two options for specifying the time shift:
///
/// 1. Relative shift – shift relatively to the query rectangle, e.g., one month or one year to the past.
///    This can be useful for comparing multiple points in time relative to the query rectangle.
/// 2. Absolute shift – change query rectangle to a fixed temporal reference, e.g., January 2014.
///    This can be used to compare data in the query rectangle's time to a fixed point of reference.
///
/// The output is either a stream of raster data or a stream of vector data depending on the input.
///
/// An example usage scenario is to compare the current time with the previous time of the same raster data.
/// For instance, a raster source outputs monthly data aggregates of mean temperatures.
/// If you want to compute the difference between the current month and the previous month, you can use the `TimeShift` operator.
/// You will have two workflows.
/// One is the unmodified temperature raster source.
/// The other is the same source, shifted by one month.
/// Then, you can use both workflows as sources of an [`Expression`](/docs/operators/expression) operator.
///
/// _Note_: This operator modifies the time values of the returned data.
/// For rasters and vector data, it shifts the time intervals opposite to the time shift specified in the operator.
/// This is necessary to have only data inside the result that is part of the [`QueryRectangle`'s](../datatypes/queryrectangle) time interval.
/// As an example, we shift monthly data by one month to the past.
/// Our query rectangle points to February.
/// Then, the operator shifts the query rectangle to January.
/// The data, originally valid for January, is shifted forward to February again, to fit into the original query rectangle, which is February.
///
#[api_operator(
    title = "Time Shift",
    examples(
        json!({
            "type": "TimeShift",
            "params": {
                "type": "relative",
                "granularity": "months",
                "value": -1
            },
            "sources": {
                "source": {
                    "type": "GdalSource",
                    "params": {
                        "data": "ndvi"
                    }
                }
            }
        }),
        json!({
            "type": "TimeShift",
            "params": {
                "type": "absolute",
                "time_interval": {
                    "start": "2010-01-01T00:00:00Z",
                    "end": "2010-02-01T00:00:00Z"
                }
            },
            "sources": {
                "source": {
                    "type": "GdalSource",
                    "params": {
                        "data": "ndvi"
                    }
                }
            }
        })
    )
)]
pub struct TimeShift {
    pub params: TimeShiftParameters,
    pub sources: Box<SingleRasterOrVectorSource>,
}

/// Time shifts can be expressed either as a relative offset or as a fixed absolute time interval.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum TimeShiftParameters {
    Relative(TimeShiftParametersRelative),
    Absolute(TimeShiftParametersAbsolute),
}

/// If `type` is `relative`, you need to specify the following parameters:
#[type_tag(value = "relative")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TimeShiftParametersRelative {
    /// Time granularity for the relative shift.
    #[schema(examples("months", "years"))]
    pub granularity: TimeGranularity,
    /// Signed time step for the relative shift.
    #[schema(examples(-1, 1))]
    pub value: i32,
}

/// If the `type` is `absolute`, you need to specify the following parameters:
#[type_tag(value = "absolute")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TimeShiftParametersAbsolute {
    /// Fixed time interval used as the shift target.
    #[schema(examples(json!({ "start": "2010-01-01T00:00:00Z", "end": "2010-02-01T00:00:00Z" }))) ]
    pub time_interval: TimeInterval,
}

impl From<TimeShiftParameters> for OperatorsTimeShiftParameters {
    fn from(value: TimeShiftParameters) -> Self {
        match value {
            TimeShiftParameters::Relative(TimeShiftParametersRelative {
                r#type: _,
                granularity,
                value,
            }) => Self::Relative {
                granularity: granularity.into(),
                value,
            },
            TimeShiftParameters::Absolute(TimeShiftParametersAbsolute {
                r#type: _,
                time_interval,
            }) => Self::Absolute {
                time_interval: time_interval.into(),
            },
        }
    }
}

impl TryFrom<TimeShift> for OperatorsTimeShift {
    type Error = anyhow::Error;

    fn try_from(value: TimeShift) -> Result<Self, Self::Error> {
        Ok(OperatorsTimeShift {
            params: value.params.into(),
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `VectorJoin` operator allows combining multiple vector inputs into a single feature collection.
/// There are multiple join variants defined, which are described below.
///
/// For instance, you want to join tabular data to a point collection of buildings.
/// The point collection contains the geolocation of the buildings and their id.
/// The attribute data collection has the building id and the height information.
/// Combining the two feature collections leads to a single point collection with geolocation and height information.
///
/// ## Errors
///
/// If the value in the `left` parameter is not a column of the left feature collection, an error is thrown.
///
/// If the value in the `right` parameter is not a column of the right feature collection, an error is thrown.
///
/// ### `EquiGeoToData`
///
/// If the left input is not a geo data collection, an error is thrown.
///
/// If the right input is not a (non-geo) data collection, an error is thrown.
///
#[api_operator(
    title = "Vector Join",
    examples(json!({
        "type": "VectorJoin",
        "params": {
            "type": "EquiGeoToData",
            "leftColumn": "id",
            "rightColumn": "id",
            "rightColumnSuffix": "_other"
        },
        "sources": {
            "points": {
                "type": "OgrSource",
                "params": {
                    "data": "places",
                    "attributeProjection": ["name", "population"]
                }
            },
            "polygons": {
                "type": "OgrSource",
                "params": {
                    "data": "germany_outline"
                }
            }
        }
    }))
)]
pub struct VectorJoin {
    pub params: VectorJoinParameters,
    pub sources: Box<VectorJoinSources>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VectorJoinParameters {
    /// ### `EquiGeoToData`
    ///
    /// | Parameter | Type | Description | Example Value |
    /// | --------- | ---- | ----------- | ------------- |
    /// | `leftColumn` | string | The column name of the left input | `"id"` |
    /// | `rightColumn` | string | The column name of the right input | `"id"` |
    /// | `rightColumnSuffix` | (Optional) string | A value to suffix the right join column to avoid name clashes with the columns of the left input. If nothing is specified, the default value is `right`. | `"right"` |
    #[serde(flatten)]
    pub join_type: VectorJoinType,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum VectorJoinType {
    EquiGeoToData(VectorJoinTypeEquiGeoToData),
}

#[type_tag(value = "equiGeoToData")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VectorJoinTypeEquiGeoToData {
    /// Column name of the left input used in the join.
    #[schema(examples("id"))]
    pub left_column: String,
    /// Column name of the right input used in the join.
    #[schema(examples("id"))]
    pub right_column: String,
    /// Optional suffix added to right-side columns to avoid collisions.
    #[schema(examples("_other", "right"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right_column_suffix: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VectorJoinSources {
    pub left: Box<VectorOperator>,
    pub right: Box<VectorOperator>,
}

impl TryFrom<VectorJoinSources> for OperatorsVectorJoinSources {
    type Error = anyhow::Error;

    fn try_from(value: VectorJoinSources) -> Result<Self, Self::Error> {
        Ok(Self {
            left: (*value.left).try_into()?,
            right: (*value.right).try_into()?,
        })
    }
}

impl From<VectorJoinType> for OperatorsVectorJoinType {
    fn from(value: VectorJoinType) -> Self {
        match value {
            VectorJoinType::EquiGeoToData(VectorJoinTypeEquiGeoToData {
                r#type: _,
                left_column,
                right_column,
                right_column_suffix,
            }) => Self::EquiGeoToData {
                left_column,
                right_column,
                right_column_suffix,
            },
        }
    }
}

impl From<VectorJoinParameters> for OperatorsVectorJoinParameters {
    fn from(value: VectorJoinParameters) -> Self {
        Self {
            join_type: value.join_type.into(),
        }
    }
}

impl TryFrom<VectorJoin> for OperatorsVectorJoin {
    type Error = anyhow::Error;

    fn try_from(value: VectorJoin) -> Result<Self, Self::Error> {
        Ok(OperatorsVectorJoin {
            params: value.params.into(),
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `VisualPointClustering` is a clustering operator for point collections that removes clutter and preserves the spatial structure of the input.
/// The output is a point collection with a count and radius attribute.
/// The operator utilizes the input resolution of the query to determine when points, being displayed as circles, would overlap.
/// Moreover, it allows aggregating non-geo attributes to preserve the other columns of the input.
/// For more information on the algorithm, cf. the paper [Beilschmidt, C. et al.: A Linear-Time Algorithm for the Aggregation and Visualization of Big Spatial Point Data. SIGSPATIAL/GIS 2017: 73:1-73:4](https://doi.org/10.1145/3139958.3140037).
///
/// An exemplary use case for this operator is the visualization of point data in an online map application.
/// There, you can use this operator as the final step of the workflow to cluster the points and display them as circles.
/// These circles then pose a decluttered view of the data, e.g., via a WFS endpoint.
///
/// ## Errors
///
/// If the source value `vector` is not a point collection, an error is thrown.
///
/// If multiple columns in `columnAggregates` have the same names, an error is thrown.
///
#[api_operator(
    title = "Visual Point Clustering",
    examples(json!({
        "type": "VisualPointClustering",
        "params": {
            "minRadiusPx": 8.0,
            "deltaPx": 1.0,
            "radiusColumn": "__radius",
            "countColumn": "__count",
            "columnAggregates": {
            "mean_population": {
                "columnName": "population",
                "aggregateType": "MeanNumber",
                "measurement": { "type": "unitless" }
            },
            "sample_names": {
                "columnName": "name",
                "aggregateType": "StringSample"
            }
            }
        },
        "sources": {
            "vector": {
                "type": "OgrSource",
                "params": {
                    "data": "places",
                    "attributeProjection": ["name", "population"]
                }
            }
        }
    }))
)]
pub struct VisualPointClustering {
    pub params: VisualPointClusteringParameters,
    pub sources: Box<SingleVectorSource>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VisualPointClusteringParameters {
    /// Minimum circle radius in pixels.
    #[schema(examples(8.0, 10.0))]
    pub min_radius_px: f64,
    /// Minimum circle-to-circle distance in pixels.
    #[schema(examples(1.0))]
    pub delta_px: f64,
    /// Spatial resolution used during clustering.
    #[schema(examples(0.5, 1.0))]
    pub resolution: f64,
    /// Column name used to store the cluster radius.
    #[schema(examples("__radius"))]
    pub radius_column: String,
    /// Column name used to store the number of clustered points.
    #[schema(examples("__count"))]
    pub count_column: String,
    /// Map of source columns to aggregation definitions used for clustered output attributes.
    #[schema(examples(json!({ "mean_population": { "columnName": "population", "aggregateType": "MeanNumber", "measurement": { "type": "unitless" } } }))) ]
    pub column_aggregates: std::collections::HashMap<String, AttributeAggregateDef>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AttributeAggregateDef {
    /// Name of the source column to aggregate.
    #[schema(examples("population", "name"))]
    pub column_name: String,
    /// Aggregation function applied to the source column.
    #[schema(examples("meanNumber", "stringSample"))]
    pub aggregate_type: AttributeAggregateType,
    /// Optional measurement metadata for the aggregated result.
    #[schema(examples(json!({ "type": "unitless" }))) ]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measurement: Option<Measurement>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum AttributeAggregateType {
    MeanNumber,
    StringSample,
    Null,
}

impl From<AttributeAggregateType> for OperatorsAttributeAggregateType {
    fn from(value: AttributeAggregateType) -> Self {
        match value {
            AttributeAggregateType::MeanNumber => Self::MeanNumber,
            AttributeAggregateType::StringSample => Self::StringSample,
            AttributeAggregateType::Null => Self::Null,
        }
    }
}

impl TryFrom<VisualPointClustering> for OperatorsVisualPointClustering {
    type Error = anyhow::Error;

    fn try_from(value: VisualPointClustering) -> Result<Self, Self::Error> {
        Ok(OperatorsVisualPointClustering {
            params: OperatorsVisualPointClusteringParameters {
                min_radius_px: value.params.min_radius_px,
                delta_px: value.params.delta_px,
                resolution: value.params.resolution,
                radius_column: value.params.radius_column,
                count_column: value.params.count_column,
                column_aggregates: value
                    .params
                    .column_aggregates
                    .into_iter()
                    .map(|(k, v)| {
                        Ok((
                            k,
                            geoengine_operators::processing::AttributeAggregateDef {
                                column_name: v.column_name,
                                aggregate_type: v.aggregate_type.into(),
                                measurement: v.measurement.map(Into::into),
                            },
                        ))
                    })
                    .collect::<Result<_, anyhow::Error>>()?,
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

/// The `VectorExpression` operator performs a feature-wise expression function on a feature collection of a vector source.
/// The expression is specified as a user-defined script in a very simple language.
/// The output is a feature collection with the result of the expression and with time intervals that are the same as for the inputs.
/// Users can either add a new column or replace the geometry column with the outputs of the expression.
/// Internally, the expression is evaluated using floating-point numbers.
///
/// An example usage scenario is to calculate a population density from an `area` and a `population_size` column.
/// The expression uses a feature collection with two columns, referred to with their column names `area` and a `population_size`, and calculates the formula `area / population_size`.
/// The output feature collection contains the result of the density expression in a new column.
///
/// Another example is to calculate the centroid of a polygon geometry.
/// The expression uses a feature collection with a geometry column and calculates the formula `centroid(geom)`.
/// The output feature collection contains the result of the centroid expression replacing the original geometries.
///
/// ## Types
///
/// The following describes the types used in the parameters.
///
/// ### Expression
///
/// Expressions are simple scripts to perform feature-wise computations.
/// One can refer to the columns with their name, e.g., `area` and a `population_size`.
/// Furthermore, expressions can check with `A IS NODATA`, `B IS NODATA`, etc. for empty or NO DATA values.
/// Finally, the value `NODATA` can be used to output empty or NO DATA.
///
/// Users can think of this implicit function signature for, e.g., two inputs:
///
/// ```rust,ignore
/// fn (A: f64, B: f64) -> f64
/// ```
///
/// As a start, expressions contain algebraic operations and mathematical functions.
///
/// ```rust,ignore
/// (A + B) / 2
/// ```
///
/// In addition, branches can be used to check for conditions.
///
/// ```rust,ignore
/// if A IS NODATA {
///     B
/// } else {
///     A
/// }
/// ```
///
/// To generate more complex expressions, it is possible to have variable assignments.
///
/// ```rust,ignore
/// let mean = (A + B) / 2;
/// let coefficient = 0.357;
/// mean * coefficient
/// ```
///
/// Note, that all assignments are separated by semicolons.
/// However, the last expression must be without a semicolon.
///
/// #### Numbers
///
/// Function calls can be used to access utility functions.
///
/// ```rust,ignore
/// max(A, 0)
/// ```
///
/// Currently, the following functions are available:
///
/// - `abs(a)`: absolute value
/// - `min(a, b)`, `min(a, b, c)`: minimum value
/// - `max(a, b)`, `max(a, b, c)`: maximum value
/// - `sqrt(a)`: square root
/// - `ln(a)`: natural logarithm
/// - `log10(a)`: base 10 logarithm
/// - `cos(a)`, `sin(a)`, `tan(a)`, `acos(a)`, `asin(a)`, `atan(a)`: trigonometric functions
/// - `pi()`, `e()`: mathematical constants
/// - `round(a)`, `ceil(a)`, `floor(a)`: rounding functions
/// - `mod(a, b)`: division remainder
/// - `to_degrees(a)`, `to_radians(a)`: conversion to degrees or radians
///
/// #### Geometries
///
/// Geometries can be referred to using the `geometryColumnName`, which is `geom` by default.
/// There are several functions to work with geometries:
///
/// - `centroid(geom)`: returns the centroid of the geometry
/// - `area(geom)`: returns the area of the geometry
///
/// An example expression to calculate the centroid of a geometry is:
///
/// ```rust,ignore
/// centroid(geom)
/// ```
///
/// ## Errors
///
/// The parsing of the expression can fail if there are, e.g., syntax errors.
///
#[api_operator(
    title = "Vector Expression",
    examples(
        json!({
            "type": "VectorExpression",
            "params": {
                "inputColumns": ["area", "population_size"],
                "outputColumn": { "type": "column", "value": "density" },
                "expression": "area /  population_size",
                "outputMeasurement": { "type": "unitless" }
            },
            "sources": {
                "vector": {
                "type": "OgrSource",
                "params": {
                    "data": "areas"
                }
                }
            }
        }),
        json!({
            "type": "VectorExpression",
            "params": {
                "inputColumns": [],
                "outputColumn": { "type": "geometry", "value": "MultiPoint" },
                "expression": "centroid(geom)",
                "geometryColumnName": "geom"
            },
            "sources": {
                "vector": {
                "type": "OgrSource",
                "params": {
                    "data": "areas"
                }
                }
            }
        }),
    )
)]
pub struct VectorExpression {
    pub params: VectorExpressionParameters,
    pub sources: Box<SingleVectorSource>,
}

/// Parameters for the `VectorExpression` operator.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VectorExpressionParameters {
    /// The columns to use as variables in the expression.
    ///
    /// For usage in the expression, all special characters are replaced by underscores.
    /// E.g., `precipitation.cm` becomes `precipitation_cm`.
    /// If the column name starts with a number, an underscore is prepended.
    /// E.g., `1column` becomes `_1column`.
    #[schema(examples(json!(["temperature", "humidity"])))]
    pub input_columns: Vec<String>,

    /// The expression to evaluate.
    #[schema(examples("temperature * (1 + humidity / 100)"))]
    pub expression: String,

    /// The type and name of the new column.
    #[schema(examples(
        json!({"type": "column", "value": "adjusted_temperature"}),
        json!({"type": "geometry", "value": "MultiPolygon"}),
    ))]
    pub output_column: OutputColumn,

    /// The variable name of the geometry column.
    /// The default is `geom`.
    #[serde(default = "geometry_default_column_name")]
    #[schema(examples(json!("geom")))]
    pub geometry_column_name: String,

    /// The measurement of the new column.
    /// The default is unitless.
    #[schema(examples(
        json!({"type": "unitless"}),
        json!({"type": "continuous", "measurement": "length", "unit": "m"}),
        json!({"type": "classification", "measurement": "severity", "classes": ["low", "medium", "high"]}),
    ))]
    #[serde(default = "Measurement::unitless")]
    pub output_measurement: Measurement,
}

fn geometry_default_column_name() -> String {
    "geom".into()
}

impl From<OutputColumn> for OperatorsOutputColumn {
    fn from(value: OutputColumn) -> Self {
        match value {
            OutputColumn::Geometry(vector_data_type) => Self::Geometry(match vector_data_type {
                crate::api::model::datatypes::VectorDataType::Data => {
                    unreachable!("data is not a valid geometry output type")
                }
                crate::api::model::datatypes::VectorDataType::MultiPoint => {
                    geoengine_datatypes::collections::GeoVectorDataType::MultiPoint
                }
                crate::api::model::datatypes::VectorDataType::MultiLineString => {
                    geoengine_datatypes::collections::GeoVectorDataType::MultiLineString
                }
                crate::api::model::datatypes::VectorDataType::MultiPolygon => {
                    geoengine_datatypes::collections::GeoVectorDataType::MultiPolygon
                }
            }),
            OutputColumn::Column(column_name) => Self::Column(column_name),
        }
    }
}

impl TryFrom<VectorExpression> for OperatorsVectorExpression {
    type Error = anyhow::Error;

    fn try_from(value: VectorExpression) -> Result<Self, Self::Error> {
        Ok(OperatorsVectorExpression {
            params: OperatorsVectorExpressionParameters {
                input_columns: value.params.input_columns,
                expression: value.params.expression,
                output_column: value.params.output_column.into(),
                geometry_column_name: value.params.geometry_column_name,
                output_measurement: value.params.output_measurement.into(),
            },
            sources: (*value.sources).try_into()?,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::api::model::{
        datatypes::{Coordinate2D, NamedData, TimeGranularity, VectorDataType},
        processing_graphs::{
            OgrSource, OgrSourceParameters, RasterOperator, SingleRasterOrVectorOperator,
            VectorOperator,
            parameters::{ClassificationMeasurement, ContinuousMeasurement, SpatialBoundsDerive},
            source::{
                GdalSource, GdalSourceParameters, MockPointSource, MockPointSourceParameters,
                MultiBandGdalSource,
            },
        },
    };
    use float_cmp::approx_eq;
    use serde_json::json;

    #[test]
    fn it_converts_expressions() {
        let api = Expression {
            r#type: Default::default(),
            params: ExpressionParameters {
                expression: "2 * A + B".to_string(),
                output_type: RasterDataType::F32,
                output_band: None,
                map_no_data: true,
            },
            sources: Box::new(SingleRasterSource {
                raster: RasterOperator::GdalSource(GdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: NamedData::with_system_name("example_data"),
                        overview_level: None,
                    },
                }),
            }),
        };

        let ops = OperatorsExpression::try_from(api).expect("conversion failed");

        assert_eq!(ops.params.expression, "2 * A + B");
        assert_eq!(
            ops.params.output_type,
            geoengine_datatypes::raster::RasterDataType::F32
        );
        assert!(ops.params.output_band.is_none());
        assert!(ops.params.map_no_data);
    }

    #[test]
    fn it_converts_raster_vector_join_params() {
        let api = RasterVectorJoin {
            r#type: Default::default(),
            params: RasterVectorJoinParameters {
                names: ColumnNames::Names {
                    values: vec!["a".to_string(), "b".to_string()],
                },
                feature_aggregation: FeatureAggregationMethod::First,
                feature_aggregation_ignore_no_data: true,
                temporal_aggregation: TemporalAggregationMethod::Mean,
                temporal_aggregation_ignore_no_data: false,
            },
            sources: Box::new(SingleVectorMultipleRasterSources {
                vector: VectorOperator::MockPointSource(MockPointSource {
                    r#type: Default::default(),
                    params: MockPointSourceParameters {
                        points: vec![Coordinate2D { x: 0.0, y: 0.0 }],
                        spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                    },
                }),
                rasters: vec![RasterOperator::GdalSource(GdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: NamedData::with_system_name("example_data"),
                        overview_level: None,
                    },
                })],
            }),
        };

        let ops_params = OperatorsRasterVectorJoin::try_from(api).expect("conversion failed");

        assert!(matches!(
            ops_params.params.names,
            geoengine_operators::processing::ColumnNames::Names(_)
        ));
        assert_eq!(
            ops_params.params.feature_aggregation,
            geoengine_operators::processing::FeatureAggregationMethod::First
        );
        assert!(ops_params.params.feature_aggregation_ignore_no_data);
        assert_eq!(
            ops_params.params.temporal_aggregation,
            geoengine_operators::processing::TemporalAggregationMethod::Mean
        );
        assert!(!ops_params.params.temporal_aggregation_ignore_no_data);
    }

    #[test]
    fn it_converts_reprojection_params() {
        let api = Reprojection {
            r#type: Default::default(),
            params: ReprojectionParameters {
                target_spatial_reference: "EPSG:32632".parse().expect("valid srs"),
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: Box::new(SingleRasterOrVectorSource {
                source: SingleRasterOrVectorOperator::Vector(VectorOperator::MockPointSource(
                    MockPointSource {
                        r#type: Default::default(),
                        params: MockPointSourceParameters {
                            points: vec![Coordinate2D { x: 0.0, y: 0.0 }],
                            spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                        },
                    },
                )),
            }),
        };

        let ops = OperatorsReprojection::try_from(api).expect("conversion failed");

        assert_eq!(
            ops.params.target_spatial_reference.to_string(),
            "EPSG:32632"
        );
        assert!(matches!(
            ops.params.derive_out_spec,
            geoengine_operators::processing::DeriveOutRasterSpecsSource::ProjectionBounds
        ));
    }

    #[test]
    fn it_converts_temporal_raster_aggregation_params() {
        let api = TemporalRasterAggregation {
            r#type: Default::default(),
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Mean(AggregationMean {
                    r#type: Default::default(),
                    ignore_no_data: true,
                }),
                window: crate::api::model::datatypes::TimeStep {
                    granularity: TimeGranularity::Months,
                    step: 1,
                },
                window_reference: None,
                output_type: None,
            },
            sources: Box::new(SingleRasterSource {
                raster: RasterOperator::GdalSource(GdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: NamedData::with_system_name("example_data"),
                        overview_level: None,
                    },
                }),
            }),
        };

        let ops = OperatorsTemporalRasterAggregation::try_from(api).expect("conversion failed");

        assert!(matches!(
            ops.params.aggregation,
            geoengine_operators::processing::Aggregation::Mean {
                ignore_no_data: true
            }
        ));
        assert_eq!(ops.params.window.step, 1);
        assert!(ops.params.window_reference.is_none());
        assert!(ops.params.output_type.is_none());
    }

    #[test]
    fn it_converts_raster_stacker_params() {
        let api = RasterStacker {
            r#type: Default::default(),
            params: RasterStackerParameters {
                rename_bands: RenameBands::Suffix(vec!["_a".to_string(), "_b".to_string()]),
            },
            sources: Box::new(MultipleRasterSources {
                rasters: vec![
                    RasterOperator::GdalSource(GdalSource {
                        r#type: Default::default(),
                        params: GdalSourceParameters {
                            data: NamedData::with_system_name("example_data_a"),
                            overview_level: None,
                        },
                    }),
                    RasterOperator::GdalSource(GdalSource {
                        r#type: Default::default(),
                        params: GdalSourceParameters {
                            data: NamedData::with_system_name("example_data_b"),
                            overview_level: None,
                        },
                    }),
                ],
            }),
        };

        let ops = OperatorsRasterStacker::try_from(api).expect("conversion failed");

        assert_eq!(
            ops.params.rename_bands,
            geoengine_datatypes::raster::RenameBands::Suffix(vec![
                "_a".to_string(),
                "_b".to_string()
            ])
        );
        assert_eq!(ops.sources.rasters.len(), 2);
    }

    #[test]
    fn it_converts_raster_type_conversion_params() {
        let api = RasterTypeConversion {
            r#type: Default::default(),
            params: RasterTypeConversionParameters {
                output_data_type: RasterDataType::U16,
            },
            sources: Box::new(SingleRasterSource {
                raster: RasterOperator::GdalSource(GdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: NamedData::with_system_name("example_data"),
                        overview_level: None,
                    },
                }),
            }),
        };

        let ops = OperatorsRasterTypeConversion::try_from(api).expect("conversion failed");

        assert_eq!(
            ops.params.output_data_type,
            geoengine_datatypes::raster::RasterDataType::U16
        );
    }

    #[test]
    fn it_converts_interpolation_params() {
        let api = Interpolation {
            r#type: Default::default(),
            params: InterpolationParameters {
                interpolation: InterpolationMethod::NearestNeighbor,
                output_resolution: InterpolationResolution::Fraction(
                    InterpolationResolutionFraction {
                        r#type: Default::default(),
                        x: 2.0,
                        y: 2.0,
                    },
                ),
                output_origin_reference: None,
            },
            sources: Box::new(SingleRasterSource {
                raster: RasterOperator::MultiBandGdalSource(MultiBandGdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: NamedData::with_system_name("example_data"),
                        overview_level: None,
                    },
                }),
            }),
        };

        let ops = OperatorsInterpolation::try_from(api).expect("conversion failed");

        assert!(matches!(
            ops.params.interpolation,
            geoengine_operators::processing::InterpolationMethod::NearestNeighbor
        ));
        assert!(matches!(
            ops.params.output_resolution,
            geoengine_operators::processing::InterpolationResolution::Fraction(
                geoengine_operators::processing::Fraction { x, y }
            )
            if (x - 2.0).abs() < f64::EPSILON && (y - 2.0).abs() < f64::EPSILON
        ));
        assert!(ops.params.output_origin_reference.is_none());
    }

    #[test]
    fn it_converts_downsampling_params() {
        let api = Downsampling {
            r#type: Default::default(),
            params: DownsamplingParameters {
                sampling_method: DownsamplingMethod::NearestNeighbor,
                output_resolution: DownsamplingResolution::Fraction(
                    DownsamplingResolutionFraction {
                        r#type: Default::default(),
                        x: 2.0,
                        y: 2.0,
                    },
                ),
                output_origin_reference: Some(crate::api::model::datatypes::Coordinate2D {
                    x: 0.0,
                    y: 0.0,
                }),
            },
            sources: Box::new(SingleRasterSource {
                raster: RasterOperator::MultiBandGdalSource(MultiBandGdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: NamedData::with_system_name("example_data"),
                        overview_level: None,
                    },
                }),
            }),
        };

        let ops = OperatorsDownsampling::try_from(api).expect("conversion failed");

        assert!(matches!(
            ops.params.sampling_method,
            geoengine_operators::processing::DownsamplingMethod::NearestNeighbor
        ));
        assert!(matches!(
            ops.params.output_resolution,
            geoengine_operators::processing::DownsamplingResolution::Fraction(
                geoengine_operators::processing::Fraction { x, y }
            )
            if (x - 2.0).abs() < f64::EPSILON && (y - 2.0).abs() < f64::EPSILON
        ));
        assert_eq!(
            ops.params.output_origin_reference,
            Some(geoengine_datatypes::primitives::Coordinate2D::new(0.0, 0.0))
        );
    }

    #[test]
    fn it_converts_band_filter_params() {
        let api = BandFilter {
            r#type: Default::default(),
            params: BandFilterParameters {
                bands: BandsByNameOrIndex::Name(vec!["nir".to_string(), "red".to_string()]),
            },
            sources: Box::new(SingleRasterSource {
                raster: RasterOperator::MultiBandGdalSource(MultiBandGdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: NamedData::with_system_name("example_data"),
                        overview_level: None,
                    },
                }),
            }),
        };

        let ops = OperatorsBandFilter::try_from(api).expect("conversion failed");

        assert_eq!(
            serde_json::to_value(ops.params).expect("params should serialize"),
            json!({
                "bands": ["nir", "red"]
            })
        );
    }

    #[test]
    fn it_converts_band_neighborhood_aggregate_params() {
        let api = BandNeighborhoodAggregate {
            r#type: Default::default(),
            params: BandNeighborhoodAggregateParameters {
                aggregate: BandNeighborhoodAggregateMethod::Average(
                    BandNeighborhoodAggregateMethodAverage {
                        r#type: Default::default(),
                        window_size: 3,
                    },
                ),
            },
            sources: Box::new(SingleRasterSource {
                raster: RasterOperator::MultiBandGdalSource(MultiBandGdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: NamedData::with_system_name("example_data"),
                        overview_level: None,
                    },
                }),
            }),
        };

        let ops = OperatorsBandNeighborhoodAggregate::try_from(api).expect("conversion failed");

        assert!(matches!(
            ops.params.aggregate,
            geoengine_operators::processing::BandNeighborhoodAggregateMethod::Average {
                window_size: 3
            }
        ));
    }

    #[test]
    fn it_converts_point_in_polygon_filter_params() {
        let api = PointInPolygonFilter {
            r#type: Default::default(),
            params: PointInPolygonFilterParameters {},
            sources: PointInPolygonFilterSource {
                points: Box::new(VectorOperator::MockPointSource(MockPointSource {
                    r#type: Default::default(),
                    params: MockPointSourceParameters {
                        points: vec![Coordinate2D { x: 0.0, y: 0.0 }],
                        spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                    },
                })),
                polygons: Box::new(VectorOperator::OgrSource(OgrSource {
                    r#type: Default::default(),
                    params: OgrSourceParameters {
                        data: NamedData::with_system_name("polygons"),
                        attribute_projection: None,
                        attribute_filters: None,
                    },
                })),
            },
        };

        let ops = OperatorsPointInPolygonFilter::try_from(api).expect("conversion failed");
        assert_eq!(ops.sources.points.data_names_collect(&mut Vec::new()), ());
    }

    #[test]
    fn it_converts_rasterization_params() {
        let api = Rasterization {
            r#type: Default::default(),
            params: RasterizationParameters {
                spatial_resolution: crate::api::model::datatypes::SpatialResolution {
                    x: 1.0,
                    y: 2.0,
                },
                origin_coordinate: Coordinate2D { x: 0.0, y: 0.0 },
                density_params: Some(DensityParams {
                    cutoff: 0.5,
                    stddev: 1.5,
                }),
            },
            sources: Box::new(SingleVectorSource {
                vector: VectorOperator::MockPointSource(MockPointSource {
                    r#type: Default::default(),
                    params: MockPointSourceParameters {
                        points: vec![Coordinate2D { x: 0.0, y: 0.0 }],
                        spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                    },
                }),
            }),
        };

        let ops = OperatorsRasterization::try_from(api).expect("conversion failed");

        approx_eq!(f64, ops.params.spatial_resolution.x, 1.0);
        approx_eq!(f64, ops.params.spatial_resolution.y, 2.0);
        approx_eq!(f64, ops.params.origin_coordinate.x, 0.0);
        assert!(ops.params.density_params.is_some());
        approx_eq!(f64, ops.params.density_params.unwrap().cutoff, 0.5);
        approx_eq!(f64, ops.params.density_params.unwrap().stddev, 1.5);
    }

    #[test]
    fn it_converts_reflectance_params() {
        let api = Reflectance {
            r#type: Default::default(),
            params: ReflectanceParameters {
                solar_correction: true,
                force_hrv: false,
                force_satellite: Some(8),
            },
            sources: Box::new(SingleRasterSource {
                raster: RasterOperator::GdalSource(GdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: NamedData::with_system_name("example_data"),
                        overview_level: None,
                    },
                }),
            }),
        };

        let ops = OperatorsReflectance::try_from(api).expect("conversion failed");

        assert!(ops.params.solar_correction);
        assert!(!ops.params.force_hrv);
        assert_eq!(ops.params.force_satellite, Some(8));
    }

    #[test]
    fn it_converts_temperature_params() {
        let api = Temperature {
            r#type: Default::default(),
            params: TemperatureParameters {
                force_satellite: Some(8),
            },
            sources: Box::new(SingleRasterSource {
                raster: RasterOperator::GdalSource(GdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: NamedData::with_system_name("example_data"),
                        overview_level: None,
                    },
                }),
            }),
        };

        let ops = OperatorsTemperature::try_from(api).expect("conversion failed");

        assert_eq!(ops.params.force_satellite, Some(8));
    }

    #[test]
    fn it_converts_time_projection_params() {
        let api = TimeProjection {
            r#type: Default::default(),
            params: TimeProjectionParameters {
                step: crate::api::model::datatypes::TimeStep {
                    granularity: TimeGranularity::Months,
                    step: 1,
                },
                step_reference: None,
            },
            sources: Box::new(SingleVectorSource {
                vector: VectorOperator::MockPointSource(MockPointSource {
                    r#type: Default::default(),
                    params: MockPointSourceParameters {
                        points: vec![Coordinate2D { x: 0.0, y: 0.0 }],
                        spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                    },
                }),
            }),
        };

        let ops = OperatorsTimeProjection::try_from(api).expect("conversion failed");

        assert_eq!(ops.params.step.step, 1);
        assert!(ops.params.step_reference.is_none());
    }

    #[test]
    fn it_converts_time_shift_params() {
        let api = TimeShift {
            r#type: Default::default(),
            params: TimeShiftParameters::Relative(TimeShiftParametersRelative {
                r#type: Default::default(),
                granularity: TimeGranularity::Days,
                value: 1,
            }),
            sources: Box::new(SingleRasterOrVectorSource {
                source: SingleRasterOrVectorOperator::Raster(RasterOperator::GdalSource(
                    GdalSource {
                        r#type: Default::default(),
                        params: GdalSourceParameters {
                            data: NamedData::with_system_name("example_data"),
                            overview_level: None,
                        },
                    },
                )),
            }),
        };

        let ops = OperatorsTimeShift::try_from(api).expect("conversion failed");
        assert!(matches!(
            ops.params,
            geoengine_operators::processing::TimeShiftParams::Relative {
                granularity: geoengine_datatypes::primitives::TimeGranularity::Days,
                value: 1,
            }
        ));
    }

    #[test]
    fn it_converts_vector_join_params() {
        let api = VectorJoin {
            r#type: Default::default(),
            params: VectorJoinParameters {
                join_type: VectorJoinType::EquiGeoToData(VectorJoinTypeEquiGeoToData {
                    r#type: Default::default(),
                    left_column: "id".to_string(),
                    right_column: "id".to_string(),
                    right_column_suffix: Some("_right".to_string()),
                }),
            },
            sources: Box::new(VectorJoinSources {
                left: Box::new(VectorOperator::MockPointSource(MockPointSource {
                    r#type: Default::default(),
                    params: MockPointSourceParameters {
                        points: vec![Coordinate2D { x: 0.0, y: 0.0 }],
                        spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                    },
                })),
                right: Box::new(VectorOperator::OgrSource(OgrSource {
                    r#type: Default::default(),
                    params: OgrSourceParameters {
                        data: NamedData::with_system_name("metadata"),
                        attribute_projection: None,
                        attribute_filters: None,
                    },
                })),
            }),
        };

        let ops = OperatorsVectorJoin::try_from(api).expect("conversion failed");
        assert!(matches!(
            ops.params.join_type,
            geoengine_operators::processing::VectorJoinType::EquiGeoToData {
                left_column,
                right_column,
                right_column_suffix,
            } if left_column == "id" && right_column == "id" && right_column_suffix == Some("_right".to_string())
        ));
    }

    #[test]
    fn it_converts_visual_point_clustering_params() {
        let api = VisualPointClustering {
            r#type: Default::default(),
            params: VisualPointClusteringParameters {
                min_radius_px: 2.0,
                delta_px: 1.0,
                resolution: 0.5,
                radius_column: "radius".to_string(),
                count_column: "count".to_string(),
                column_aggregates: std::collections::HashMap::from([(
                    "population".to_string(),
                    AttributeAggregateDef {
                        column_name: "population".to_string(),
                        aggregate_type: AttributeAggregateType::MeanNumber,
                        measurement: None,
                    },
                )]),
            },
            sources: Box::new(SingleVectorSource {
                vector: VectorOperator::MockPointSource(MockPointSource {
                    r#type: Default::default(),
                    params: MockPointSourceParameters {
                        points: vec![Coordinate2D { x: 0.0, y: 0.0 }],
                        spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                    },
                }),
            }),
        };

        let ops = OperatorsVisualPointClustering::try_from(api).expect("conversion failed");
        assert_eq!(ops.params.radius_column, "radius");
        assert_eq!(ops.params.count_column, "count");
        assert!(ops.params.column_aggregates.contains_key("population"));
        assert!(matches!(
            ops.params.column_aggregates["population"].aggregate_type,
            geoengine_operators::processing::AttributeAggregateType::MeanNumber
        ));
    }

    #[test]
    #[allow(clippy::too_many_lines, reason = "test covers multiple cases")]
    fn it_converts_vector_expression_params() {
        // Test with column output and default geometry column
        let api_column = VectorExpression {
            r#type: Default::default(),
            params: VectorExpressionParameters {
                input_columns: vec!["temperature".to_string(), "humidity".to_string()],
                expression: "temperature * (1 + humidity / 100)".to_string(),
                output_column: OutputColumn::Column("adjusted_temperature".to_string()),
                geometry_column_name: "geom".to_string(),
                output_measurement: Measurement::Unitless(Default::default()),
            },
            sources: Box::new(SingleVectorSource {
                vector: VectorOperator::MockPointSource(MockPointSource {
                    r#type: Default::default(),
                    params: MockPointSourceParameters {
                        points: vec![Coordinate2D { x: 0.0, y: 0.0 }],
                        spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                    },
                }),
            }),
        };

        let json_column = json!({
            "type": "VectorExpression",
            "params": {
                "inputColumns": ["temperature", "humidity"],
                "expression": "temperature * (1 + humidity / 100)",
                "outputColumn": {
                    "type": "column",
                    "value": "adjusted_temperature"
                },
                "geometryColumnName": "geom",
                "outputMeasurement": {
                    "type": "unitless"
                }
            },
            "sources": {
                "vector": {
                    "type": "MockPointSource",
                    "params": {
                        "points": [{"x": 0.0, "y": 0.0}],
                        "spatialBounds": {"type": "derive"}
                    }
                }
            }
        });

        assert_eq!(serde_json::to_value(&api_column).unwrap(), json_column);
        assert_eq!(
            serde_json::from_value::<VectorExpression>(json_column).unwrap(),
            api_column
        );
        OperatorsVectorExpression::try_from(api_column).expect("it converts to operator pendant");

        // Test with geometry output
        let api_geometry = VectorExpression {
            r#type: Default::default(),
            params: VectorExpressionParameters {
                input_columns: vec!["x".to_string(), "y".to_string()],
                expression: "create_point(x, y)".to_string(),
                output_column: OutputColumn::Geometry(VectorDataType::MultiPolygon),
                geometry_column_name: "geom".to_string(),
                output_measurement: Measurement::Unitless(Default::default()),
            },
            sources: Box::new(SingleVectorSource {
                vector: VectorOperator::OgrSource(OgrSource {
                    r#type: Default::default(),
                    params: OgrSourceParameters {
                        data: NamedData::with_system_name("weather_stations"),
                        attribute_projection: None,
                        attribute_filters: None,
                    },
                }),
            }),
        };

        let json_geometry = json!({
            "type": "VectorExpression",
            "params": {
                "inputColumns": ["x", "y"],
                "expression": "create_point(x, y)",
                "outputColumn": {
                    "type": "geometry",
                    "value": "MultiPolygon"
                },
                "geometryColumnName": "geom",
                "outputMeasurement": {
                    "type": "unitless"
                }
            },
            "sources": {
                "vector": {
                    "type": "OgrSource",
                    "params": {
                        "data": "weather_stations",
                    }
                }
            }
        });

        assert_eq!(serde_json::to_value(&api_geometry).unwrap(), json_geometry);
        assert_eq!(
            serde_json::from_value::<VectorExpression>(json_geometry).unwrap(),
            api_geometry
        );
        OperatorsVectorExpression::try_from(api_geometry).expect("it converts to operator pendant");

        // Test with custom geometry column
        let api_custom_geom = VectorExpression {
            r#type: Default::default(),
            params: VectorExpressionParameters {
                input_columns: vec!["value".to_string()],
                expression: "value * 2".to_string(),
                output_column: OutputColumn::Column("doubled".to_string()),
                geometry_column_name: "my_geometry".to_string(),
                output_measurement: Measurement::Unitless(Default::default()),
            },
            sources: Box::new(SingleVectorSource {
                vector: VectorOperator::MockPointSource(MockPointSource {
                    r#type: Default::default(),
                    params: MockPointSourceParameters {
                        points: vec![Coordinate2D { x: 1.0, y: 2.0 }],
                        spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                    },
                }),
            }),
        };

        let json_custom_geom = json!({
            "type": "VectorExpression",
            "params": {
                "inputColumns": ["value"],
                "expression": "value * 2",
                "outputColumn": {
                    "type": "column",
                    "value": "doubled"
                },
                "geometryColumnName": "my_geometry",
                "outputMeasurement": {
                    "type": "unitless"
                }
            },
            "sources": {
                "vector": {
                    "type": "MockPointSource",
                    "params": {
                        "points": [{"x": 1.0, "y": 2.0}],
                        "spatialBounds": {"type": "derive"}
                    }
                }
            }
        });

        assert_eq!(
            serde_json::to_value(&api_custom_geom).unwrap(),
            json_custom_geom
        );
        assert_eq!(
            serde_json::from_value::<VectorExpression>(json_custom_geom).unwrap(),
            api_custom_geom
        );
        OperatorsVectorExpression::try_from(api_custom_geom)
            .expect("it converts to operator pendant");

        // Test with continuous measurement
        let api_continuous = VectorExpression {
            r#type: Default::default(),
            params: VectorExpressionParameters {
                input_columns: vec!["rainfall".to_string()],
                expression: "rainfall / 10".to_string(),
                output_column: OutputColumn::Column("rainfall_mm".to_string()),
                geometry_column_name: "geom".to_string(),
                output_measurement: Measurement::Continuous(ContinuousMeasurement {
                    r#type: Default::default(),
                    measurement: "precipitation".to_string(),
                    unit: Some("mm".to_string()),
                }),
            },
            sources: Box::new(SingleVectorSource {
                vector: VectorOperator::MockPointSource(MockPointSource {
                    r#type: Default::default(),
                    params: MockPointSourceParameters {
                        points: vec![Coordinate2D { x: 0.0, y: 0.0 }],
                        spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                    },
                }),
            }),
        };

        let json_continuous = json!({
            "type": "VectorExpression",
            "params": {
                "inputColumns": ["rainfall"],
                "expression": "rainfall / 10",
                "outputColumn": {
                    "type": "column",
                    "value": "rainfall_mm"
                },
                "geometryColumnName": "geom",
                "outputMeasurement": {
                    "type": "continuous",
                    "measurement": "precipitation",
                    "unit": "mm"
                }
            },
            "sources": {
                "vector": {
                    "type": "MockPointSource",
                    "params": {
                        "points": [{"x": 0.0, "y": 0.0}],
                        "spatialBounds": {"type": "derive"}
                    }
                }
            }
        });

        assert_eq!(
            serde_json::to_value(&api_continuous).unwrap(),
            json_continuous
        );
        assert_eq!(
            serde_json::from_value::<VectorExpression>(json_continuous).unwrap(),
            api_continuous
        );
        OperatorsVectorExpression::try_from(api_continuous)
            .expect("it converts to operator pendant");

        // Test with classification measurement
        let api_classification = VectorExpression {
            r#type: Default::default(),
            params: VectorExpressionParameters {
                input_columns: vec!["severity_score".to_string()],
                expression:
                    "if(severity_score > 70, 'high', if(severity_score > 40, 'medium', 'low'))"
                        .to_string(),
                output_column: OutputColumn::Column("severity".to_string()),
                geometry_column_name: "geom".to_string(),
                output_measurement: Measurement::Classification(ClassificationMeasurement {
                    r#type: Default::default(),
                    measurement: "severity".to_string(),
                    classes: [
                        (0, "low".to_string()),
                        (1, "medium".to_string()),
                        (2, "high".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                }),
            },
            sources: Box::new(SingleVectorSource {
                vector: VectorOperator::MockPointSource(MockPointSource {
                    r#type: Default::default(),
                    params: MockPointSourceParameters {
                        points: vec![Coordinate2D { x: 0.0, y: 0.0 }],
                        spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                    },
                }),
            }),
        };

        let json_classification = json!({
            "type": "VectorExpression",
            "params": {
                "inputColumns": ["severity_score"],
                "expression": "if(severity_score > 70, 'high', if(severity_score > 40, 'medium', 'low'))",
                "outputColumn": {
                    "type": "column",
                    "value": "severity"
                },
                "geometryColumnName": "geom",
                "outputMeasurement": {
                    "type": "classification",
                    "measurement": "severity",
                    "classes": {
                        "0": "low",
                        "1": "medium",
                        "2": "high"
                    }
                }
            },
            "sources": {
                "vector": {
                    "type": "MockPointSource",
                    "params": {
                        "points": [{"x": 0.0, "y": 0.0}],
                        "spatialBounds": {"type": "derive"}
                    }
                }
            }
        });

        assert_eq!(
            serde_json::to_value(&api_classification).unwrap(),
            json_classification
        );
        assert_eq!(
            serde_json::from_value::<VectorExpression>(json_classification).unwrap(),
            api_classification
        );
        OperatorsVectorExpression::try_from(api_classification)
            .expect("it converts to operator pendant");
    }
}
