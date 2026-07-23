use crate::api::model::{
    datatypes::{SpatialReference, TimeInstance, TimeStep},
    processing_graphs::{
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
use geoengine_operators::processing::{
    Aggregation as OperatorsAggregation, BandFilter as OperatorsBandFilter,
    BandFilterParams as OperatorsBandFilterParameters,
    DeriveOutRasterSpecsSource as OperatorsDeriveOutRasterSpecsSource,
    Downsampling as OperatorsDownsampling, DownsamplingMethod as OperatorsDownsamplingMethod,
    DownsamplingParams as OperatorsDownsamplingParameters,
    DownsamplingResolution as OperatorsDownsamplingResolution, Expression as OperatorsExpression,
    ExpressionParams as OperatorsExpressionParameters, Fraction as OperatorsFraction,
    Interpolation as OperatorsInterpolation, InterpolationMethod as OperatorsInterpolationMethod,
    InterpolationParams as OperatorsInterpolationParameters,
    InterpolationResolution as OperatorsInterpolationResolution,
    RasterStacker as OperatorsRasterStacker,
    RasterStackerParams as OperatorsRasterStackerParameters,
    RasterTypeConversion as OperatorsRasterTypeConversion,
    RasterTypeConversionParams as OperatorsRasterTypeConversionParameters,
    RasterVectorJoin as OperatorsRasterVectorJoin,
    RasterVectorJoinParams as OperatorsRasterVectorJoinParameters,
    Reprojection as OperatorsReprojection, ReprojectionParams as OperatorsReprojectionParameters,
    TemporalRasterAggregation as OperatorsTemporalRasterAggregation,
    TemporalRasterAggregationParameters as OperatorsTemporalRasterAggregationParameters,
    VectorExpression as OperatorsVectorExpression,
    VectorExpressionParams as OperatorsVectorExpressionParameters,
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

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum InterpolationResolution {
    #[schema(title = "Resolution")]
    /// Explicit output resolution (`x`, `y`) in target coordinates.
    Resolution { x: f64, y: f64 },
    #[schema(title = "Fraction")]
    /// Scaling factor in x/y direction.
    Fraction { x: f64, y: f64 },
}

impl From<InterpolationResolution> for OperatorsInterpolationResolution {
    fn from(value: InterpolationResolution) -> Self {
        match value {
            InterpolationResolution::Resolution { x, y } => {
                Self::Resolution(geoengine_datatypes::primitives::SpatialResolution { x, y })
            }
            InterpolationResolution::Fraction { x, y } => {
                Self::Fraction(OperatorsFraction { x, y })
            }
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
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum Aggregation {
    #[schema(title = "MinAggregation")]
    #[serde(rename_all = "camelCase")]
    Min { ignore_no_data: bool },
    #[schema(title = "MaxAggregation")]
    #[serde(rename_all = "camelCase")]
    Max { ignore_no_data: bool },
    #[schema(title = "FirstAggregation")]
    #[serde(rename_all = "camelCase")]
    First { ignore_no_data: bool },
    #[schema(title = "LastAggregation")]
    #[serde(rename_all = "camelCase")]
    Last { ignore_no_data: bool },
    #[schema(title = "MeanAggregation")]
    #[serde(rename_all = "camelCase")]
    Mean { ignore_no_data: bool },
    #[schema(title = "SumAggregation")]
    #[serde(rename_all = "camelCase")]
    Sum { ignore_no_data: bool },
    #[schema(title = "CountAggregation")]
    #[serde(rename_all = "camelCase")]
    Count { ignore_no_data: bool },
    #[schema(title = "PercentileEstimateAggregation")]
    #[serde(rename_all = "camelCase")]
    PercentileEstimate {
        ignore_no_data: bool,
        percentile: f64,
    },
}

impl From<Aggregation> for OperatorsAggregation {
    fn from(value: Aggregation) -> Self {
        match value {
            Aggregation::Min { ignore_no_data } => Self::Min { ignore_no_data },
            Aggregation::Max { ignore_no_data } => Self::Max { ignore_no_data },
            Aggregation::First { ignore_no_data } => Self::First { ignore_no_data },
            Aggregation::Last { ignore_no_data } => Self::Last { ignore_no_data },
            Aggregation::Mean { ignore_no_data } => Self::Mean { ignore_no_data },
            Aggregation::Sum { ignore_no_data } => Self::Sum { ignore_no_data },
            Aggregation::Count { ignore_no_data } => Self::Count { ignore_no_data },
            Aggregation::PercentileEstimate {
                ignore_no_data,
                percentile,
            } => Self::PercentileEstimate {
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
/// ## Inputs
///
/// The `Reprojection` operator expects exactly one _raster_ or _vector_ input.
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
/// ## Inputs
///
/// The `TemporalRasterAggregation` operator expects exactly one _raster_ input.
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
    pub window_reference: Option<TimeInstance>,
    /// Optional output raster data type.
    #[schema(examples("F32"))]
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
/// ## Inputs
///
/// The `RasterStacker` operator expects multiple raster inputs.
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
/// ## Inputs
///
/// The `RasterTypeConversion` operator expects exactly one _raster_ input.
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
/// ## Inputs
///
/// The `Interpolation` operator expects exactly one _raster_ input.
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
    pub output_origin_reference: Option<crate::api::model::datatypes::Coordinate2D>,
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
/// ## Inputs
///
/// The `Downsampling` operator expects exactly one _raster_ input.
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
    pub output_origin_reference: Option<crate::api::model::datatypes::Coordinate2D>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum DownsamplingMethod {
    /// Nearest-neighbor downsampling.
    NearestNeighbor,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum DownsamplingResolution {
    #[schema(title = "Resolution")]
    /// Explicit output resolution (`x`, `y`) in target coordinates.
    Resolution { x: f64, y: f64 },
    #[schema(title = "Fraction")]
    /// Scaling factor in x/y direction.
    Fraction { x: f64, y: f64 },
}

impl From<DownsamplingResolution> for OperatorsDownsamplingResolution {
    fn from(value: DownsamplingResolution) -> Self {
        match value {
            DownsamplingResolution::Resolution { x, y } => {
                Self::Resolution(geoengine_datatypes::primitives::SpatialResolution { x, y })
            }
            DownsamplingResolution::Fraction { x, y } => Self::Fraction(OperatorsFraction { x, y }),
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
/// ## Inputs
///
/// The `BandFilter` operator expects exactly one _raster_ input.
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

impl TryFrom<BandFilter> for OperatorsBandFilter {
    type Error = anyhow::Error;

    fn try_from(value: BandFilter) -> Result<Self, Self::Error> {
        let params = serde_json::from_value::<OperatorsBandFilterParameters>(
            serde_json::to_value(value.params)?,
        )?;

        Ok(OperatorsBandFilter {
            params,
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
/// ## Inputs
///
/// The `RasterVectorJoin` operator expects one _vector_ input and one or more _raster_ inputs.
///
/// | Parameter | Type                                |
/// | --------- | ----------------------------------- |
/// | `sources` | `SingleVectorMultipleRasterSources` |
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
/// ```rust
/// fn (A: f64, B: f64) -> f64
/// ```
///
/// As a start, expressions contain algebraic operations and mathematical functions.
///
/// ```rust
/// (A + B) / 2
/// ```
///
/// In addition, branches can be used to check for conditions.
///
/// ```rust
/// if A IS NODATA {
///     B
/// } else {
///     A
/// }
/// ```
///
/// To generate more complex expressions, it is possible to have variable assignments.
///
/// ```rust
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
/// ```rust
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
/// ```rust
/// centroid(geom)
/// ```
///
/// ## Inputs
///
/// The `VectorExpression` operator expects one vector input with at most 8 bands.
///
/// | Parameter | Type                 |
/// | --------- | -------------------- |
/// | `vector`  | `SingleVectorSource` |
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
        json!({"type": "geometry", "value": "multiPolygon"}),
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
    pub output_measurement: Measurement,
}

fn geometry_default_column_name() -> String {
    "geom".into()
}

impl TryFrom<VectorExpression> for OperatorsVectorExpression {
    type Error = anyhow::Error;

    fn try_from(value: VectorExpression) -> Result<Self, Self::Error> {
        // Convert the API OutputColumn to the operators OutputColumn via JSON serialization
        let output_column_json = serde_json::to_value(&value.params.output_column)?;
        let output_column = serde_json::from_value(output_column_json)?;

        Ok(OperatorsVectorExpression {
            params: OperatorsVectorExpressionParameters {
                input_columns: value.params.input_columns,
                expression: value.params.expression,
                output_column,
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
        datatypes::{Coordinate2D, TimeGranularity, VectorDataType},
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
                        data: "example_data".to_string(),
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
                        data: "example_data".to_string(),
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
                aggregation: Aggregation::Mean {
                    ignore_no_data: true,
                },
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
                        data: "example_data".to_string(),
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
                            data: "example_data_a".to_string(),
                            overview_level: None,
                        },
                    }),
                    RasterOperator::GdalSource(GdalSource {
                        r#type: Default::default(),
                        params: GdalSourceParameters {
                            data: "example_data_b".to_string(),
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
                        data: "example_data".to_string(),
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
                output_resolution: InterpolationResolution::Fraction { x: 2.0, y: 2.0 },
                output_origin_reference: None,
            },
            sources: Box::new(SingleRasterSource {
                raster: RasterOperator::MultiBandGdalSource(MultiBandGdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: "example_data".to_string(),
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
                output_resolution: DownsamplingResolution::Fraction { x: 2.0, y: 2.0 },
                output_origin_reference: Some(crate::api::model::datatypes::Coordinate2D {
                    x: 0.0,
                    y: 0.0,
                }),
            },
            sources: Box::new(SingleRasterSource {
                raster: RasterOperator::MultiBandGdalSource(MultiBandGdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: "example_data".to_string(),
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
                        data: "example_data".to_string(),
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
                        data: "weather_stations".to_string(),
                        attribute_projection: None,
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
                        "attributeProjection": null
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
