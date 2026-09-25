use crate::api::model::processing_graphs::{
    BandFilter, BandFilterParameters, BandNeighborhoodAggregate,
    BandNeighborhoodAggregateParameters, BandsByNameOrIndex, BandwiseExpression,
    BandwiseExpressionParameters, Downsampling, DownsamplingMethod, DownsamplingParameters,
    DownsamplingResolution, Expression, ExpressionParameters, GdalSource, GdalSourceParameters,
    Interpolation, InterpolationMethod, InterpolationParameters, InterpolationResolution,
    MultiBandGdalSource, NeighborhoodAggregate, NeighborhoodAggregateParameters,
    NeighborhoodKernel, Onnx, OnnxParameters, Radiance, RadianceParameters, RasterOperator,
    RasterScaling, RasterScalingParameters, RasterStacker, RasterStackerParameters,
    RasterTypeConversion, RasterTypeConversionParameters, Rasterization, RasterizationParameters,
    Reflectance, ReflectanceParameters, RenameBands, Temperature, TemperatureParameters,
    TemporalRasterAggregation, TemporalRasterAggregationParameters, TimeShift, TimeShiftParameters,
    back_conversion::downcast_runtime_operator,
    processing::{
        Aggregation, AggregationCount, AggregationFirst, AggregationLast, AggregationMax,
        AggregationMean, AggregationMin, AggregationPercentileEstimate, AggregationSum,
        BandDistance, BandDistanceEquallySpaced, BandNeighborhoodAggregateMethod,
        BandNeighborhoodAggregateMethodAverage, BandNeighborhoodAggregateMethodFirstDerivative,
        DensityParams, DownsamplingResolutionFraction, DownsamplingResolutionResolution,
        InterpolationResolutionFraction, InterpolationResolutionResolution,
        NeighborhoodAggregateMethod, NeighborhoodAggregateMethodStandardDeviation,
        NeighborhoodAggregateMethodSum, NeighborhoodKernelRectangle,
        NeighborhoodKernelWeightsMatrix, ScalingMode, ScalingModeMulSlopeAddOffset,
        ScalingModeSubOffsetDivSlope, SlopeOffsetSelection, SlopeOffsetSelectionAuto,
        SlopeOffsetSelectionConstant, SlopeOffsetSelectionMetadataKey, TimeShiftParametersAbsolute,
        TimeShiftParametersRelative,
    },
};
use geoengine_datatypes::util::AsAny;
use geoengine_operators::{
    engine::{OperatorName, RasterOperator as OperatorsRasterOperator},
    machine_learning::onnx::Onnx as OperatorsOnnx,
    processing::{
        AggregateFunctionParams, BandDistance as OperatorsBandDistance,
        BandFilter as OperatorsBandFilter,
        BandNeighborhoodAggregate as OperatorsBandNeighborhoodAggregate,
        BandNeighborhoodAggregateMethod as OperatorsBandNeighborhoodAggregateMethod,
        BandwiseExpression as OperatorsBandwiseExpression, Downsampling as OperatorsDownsampling,
        DownsamplingMethod as OperatorsDownsamplingMethod,
        DownsamplingResolution as OperatorsDownsamplingResolution,
        Expression as OperatorsExpression, Interpolation as OperatorsInterpolation,
        InterpolationMethod as OperatorsInterpolationMethod,
        InterpolationResolution as OperatorsInterpolationResolution,
        NeighborhoodAggregate as OperatorsNeighborhoodAggregate, NeighborhoodParams,
        Radiance as OperatorsRadiance, RasterScaling as OperatorsRasterScaling,
        RasterStacker as OperatorsRasterStacker,
        RasterTypeConversion as OperatorsRasterTypeConversion,
        Rasterization as OperatorsRasterization, Reflectance as OperatorsReflectance,
        Reprojection as OperatorsReprojection, ScalingMode as OperatorsScalingMode,
        SlopeOffsetSelection as OperatorsSlopeOffsetSelection, Temperature as OperatorsTemperature,
        TemporalRasterAggregation as OperatorsTemporalRasterAggregation,
        TimeShift as OperatorsTimeShift, TimeShiftParams as OperatorsTimeShiftParams,
    },
    source::{
        GdalSource as OperatorsGdalSource, MultiBandGdalSource as OperatorsMultiBandGdalSource,
    },
};

macro_rules! convert {
    ($type_name:expr, $operator:expr, $($runtime:ty => $variant:ident),+ $(,)?) => {{
        match $type_name {
            $(
                <$runtime>::TYPE_NAME => {
                    let op = downcast_runtime_raster_operator::<$runtime>($operator, $type_name)?;
                    return Ok(RasterOperator::$variant(op.try_into()?));
                }
            )+
            _ => anyhow::bail!(
                "cannot convert runtime raster operator {type_name} to ProcessingGraph",
                type_name = $type_name
            ),
        }
    }};
}

/// Converts a `&dyn RasterOperator` raster operator from `operators` into an API raster operator.
pub fn raster_operator_from_runtime(
    operator: &dyn OperatorsRasterOperator,
) -> anyhow::Result<RasterOperator> {
    let type_name = operator.typetag_name();

    convert!(type_name, operator,
        OperatorsBandFilter => BandFilter,
        OperatorsBandNeighborhoodAggregate => BandNeighborhoodAggregate,
        OperatorsBandwiseExpression => BandwiseExpression,
        OperatorsDownsampling => Downsampling,
        OperatorsExpression => Expression,
        OperatorsGdalSource => GdalSource,
        OperatorsInterpolation => Interpolation,
        OperatorsMultiBandGdalSource => MultiBandGdalSource,
        OperatorsNeighborhoodAggregate => NeighborhoodAggregate,
        OperatorsOnnx => Onnx,
        OperatorsRadiance => Radiance,
        OperatorsRasterScaling => RasterScaling,
        OperatorsRasterStacker => RasterStacker,
        OperatorsRasterTypeConversion => RasterTypeConversion,
        OperatorsRasterization => Rasterization,
        OperatorsReprojection => Reprojection,
        OperatorsReflectance => Reflectance,
        OperatorsTemperature => Temperature,
        OperatorsTemporalRasterAggregation => TemporalRasterAggregation,
        OperatorsTimeShift => TimeShift,
    );
}

fn downcast_runtime_raster_operator<'a, T: 'static>(
    operator: &'a dyn AsAny,
    type_name: &'a str,
) -> anyhow::Result<&'a T> {
    downcast_runtime_operator::<T>("raster", operator, type_name)
}

impl TryFrom<&OperatorsGdalSource> for GdalSource {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsGdalSource) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: GdalSourceParameters {
                data: value.params.data.clone().into(),
                overview_level: value.params.overview_level,
            },
        })
    }
}

impl TryFrom<&OperatorsMultiBandGdalSource> for MultiBandGdalSource {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsMultiBandGdalSource) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: GdalSourceParameters {
                data: value.params.data.clone().into(),
                overview_level: value.params.overview_level,
            },
        })
    }
}

impl TryFrom<&OperatorsExpression> for Expression {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsExpression) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: ExpressionParameters {
                expression: value.params.expression.clone(),
                output_type: value.params.output_type.into(),
                output_band: value.params.output_band.clone().map(Into::into),
                map_no_data: value.params.map_no_data,
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterSource {
                raster: raster_operator_from_runtime(value.sources.raster.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsTemporalRasterAggregation> for TemporalRasterAggregation {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsTemporalRasterAggregation) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: TemporalRasterAggregationParameters {
                aggregation: match value.params.aggregation {
                    geoengine_operators::processing::Aggregation::Min { ignore_no_data } => {
                        Aggregation::Min(AggregationMin {
                            r#type: Default::default(),
                            ignore_no_data,
                        })
                    }
                    geoengine_operators::processing::Aggregation::Max { ignore_no_data } => {
                        Aggregation::Max(AggregationMax {
                            r#type: Default::default(),
                            ignore_no_data,
                        })
                    }
                    geoengine_operators::processing::Aggregation::First { ignore_no_data } => {
                        Aggregation::First(AggregationFirst {
                            r#type: Default::default(),
                            ignore_no_data,
                        })
                    }
                    geoengine_operators::processing::Aggregation::Last { ignore_no_data } => {
                        Aggregation::Last(AggregationLast {
                            r#type: Default::default(),
                            ignore_no_data,
                        })
                    }
                    geoengine_operators::processing::Aggregation::Mean { ignore_no_data } => {
                        Aggregation::Mean(AggregationMean {
                            r#type: Default::default(),
                            ignore_no_data,
                        })
                    }
                    geoengine_operators::processing::Aggregation::Sum { ignore_no_data } => {
                        Aggregation::Sum(AggregationSum {
                            r#type: Default::default(),
                            ignore_no_data,
                        })
                    }
                    geoengine_operators::processing::Aggregation::Count { ignore_no_data } => {
                        Aggregation::Count(AggregationCount {
                            r#type: Default::default(),
                            ignore_no_data,
                        })
                    }
                    geoengine_operators::processing::Aggregation::PercentileEstimate {
                        ignore_no_data,
                        percentile,
                    } => Aggregation::PercentileEstimate(AggregationPercentileEstimate {
                        r#type: Default::default(),
                        ignore_no_data,
                        percentile,
                    }),
                },
                window: value.params.window.into(),
                window_reference: value.params.window_reference.map(Into::into),
                output_type: value.params.output_type.map(Into::into),
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterSource {
                raster: raster_operator_from_runtime(value.sources.raster.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsRasterStacker> for RasterStacker {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsRasterStacker) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: RasterStackerParameters {
                rename_bands: match &value.params.rename_bands {
                    geoengine_datatypes::raster::RenameBands::Default => RenameBands::Default,
                    geoengine_datatypes::raster::RenameBands::Suffix(values) => {
                        RenameBands::Suffix(values.clone())
                    }
                    geoengine_datatypes::raster::RenameBands::Rename(values) => {
                        RenameBands::Rename(values.clone())
                    }
                },
            },
            sources:
                crate::api::model::processing_graphs::source_parameters::MultipleRasterSources {
                    rasters: value
                        .sources
                        .rasters
                        .iter()
                        .map(|raster| raster_operator_from_runtime(raster.as_ref()))
                        .collect::<anyhow::Result<Vec<_>>>()?,
                }
                .into(),
        })
    }
}

impl TryFrom<&OperatorsRasterTypeConversion> for RasterTypeConversion {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsRasterTypeConversion) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: RasterTypeConversionParameters {
                output_data_type: value.params.output_data_type.into(),
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterSource {
                raster: raster_operator_from_runtime(value.sources.raster.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsInterpolation> for Interpolation {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsInterpolation) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: InterpolationParameters {
                interpolation: match value.params.interpolation {
                    OperatorsInterpolationMethod::NearestNeighbor => {
                        InterpolationMethod::NearestNeighbor
                    }
                    OperatorsInterpolationMethod::BiLinear => InterpolationMethod::BiLinear,
                },
                output_resolution: match &value.params.output_resolution {
                    OperatorsInterpolationResolution::Resolution(resolution) => {
                        InterpolationResolution::Resolution(InterpolationResolutionResolution {
                            r#type: Default::default(),
                            x: resolution.x,
                            y: resolution.y,
                        })
                    }
                    OperatorsInterpolationResolution::Fraction(fraction) => {
                        InterpolationResolution::Fraction(InterpolationResolutionFraction {
                            r#type: Default::default(),
                            x: fraction.x,
                            y: fraction.y,
                        })
                    }
                },
                output_origin_reference: value.params.output_origin_reference.map(Into::into),
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterSource {
                raster: raster_operator_from_runtime(value.sources.raster.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsDownsampling> for Downsampling {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsDownsampling) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: DownsamplingParameters {
                sampling_method: match value.params.sampling_method {
                    OperatorsDownsamplingMethod::NearestNeighbor => {
                        DownsamplingMethod::NearestNeighbor
                    }
                },
                output_resolution: match &value.params.output_resolution {
                    OperatorsDownsamplingResolution::Resolution(resolution) => {
                        DownsamplingResolution::Resolution(DownsamplingResolutionResolution {
                            r#type: Default::default(),
                            x: resolution.x,
                            y: resolution.y,
                        })
                    }
                    OperatorsDownsamplingResolution::Fraction(fraction) => {
                        DownsamplingResolution::Fraction(DownsamplingResolutionFraction {
                            r#type: Default::default(),
                            x: fraction.x,
                            y: fraction.y,
                        })
                    }
                },
                output_origin_reference: value.params.output_origin_reference.map(Into::into),
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterSource {
                raster: raster_operator_from_runtime(value.sources.raster.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsBandFilter> for BandFilter {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsBandFilter) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: BandFilterParameters {
                bands: match &value.params.bands {
                    geoengine_operators::processing::BandsByNameOrIndex::Name(names) => {
                        BandsByNameOrIndex::Name(names.clone())
                    }
                    geoengine_operators::processing::BandsByNameOrIndex::Index(indices) => {
                        BandsByNameOrIndex::Index(indices.clone())
                    }
                },
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterSource {
                raster: raster_operator_from_runtime(value.sources.raster.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsBandNeighborhoodAggregate> for BandNeighborhoodAggregate {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsBandNeighborhoodAggregate) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: BandNeighborhoodAggregateParameters {
                aggregate: match &value.params.aggregate {
                    OperatorsBandNeighborhoodAggregateMethod::FirstDerivative { band_distance } => {
                        BandNeighborhoodAggregateMethod::FirstDerivative(
                            BandNeighborhoodAggregateMethodFirstDerivative {
                                r#type: Default::default(),
                                band_distance: match band_distance {
                                    OperatorsBandDistance::EquallySpaced { distance } => {
                                        BandDistance::EquallySpaced(BandDistanceEquallySpaced {
                                            r#type: Default::default(),
                                            distance: *distance,
                                        })
                                    }
                                },
                            },
                        )
                    }
                    OperatorsBandNeighborhoodAggregateMethod::Average { window_size } => {
                        BandNeighborhoodAggregateMethod::Average(
                            BandNeighborhoodAggregateMethodAverage {
                                r#type: Default::default(),
                                window_size: *window_size,
                            },
                        )
                    }
                },
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterSource {
                raster: raster_operator_from_runtime(value.sources.raster.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsBandwiseExpression> for BandwiseExpression {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsBandwiseExpression) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: BandwiseExpressionParameters {
                expression: value.params.expression.clone(),
                output_type: value.params.output_type.into(),
                map_no_data: value.params.map_no_data,
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterSource {
                raster: raster_operator_from_runtime(value.sources.raster.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsNeighborhoodAggregate> for NeighborhoodAggregate {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsNeighborhoodAggregate) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: NeighborhoodAggregateParameters {
                neighborhood: match &value.params.neighborhood {
                    NeighborhoodParams::Rectangle { dimensions } => {
                        NeighborhoodKernel::Rectangle(NeighborhoodKernelRectangle {
                            r#type: Default::default(),
                            dimensions: *dimensions,
                        })
                    }
                    NeighborhoodParams::WeightsMatrix { weights } => {
                        NeighborhoodKernel::WeightsMatrix(NeighborhoodKernelWeightsMatrix {
                            r#type: Default::default(),
                            weights: weights.clone(),
                        })
                    }
                },
                aggregate_function: match &value.params.aggregate_function {
                    AggregateFunctionParams::Sum => {
                        NeighborhoodAggregateMethod::Sum(NeighborhoodAggregateMethodSum {
                            r#type: Default::default(),
                        })
                    }
                    AggregateFunctionParams::StandardDeviation => {
                        NeighborhoodAggregateMethod::StandardDeviation(
                            NeighborhoodAggregateMethodStandardDeviation {
                                r#type: Default::default(),
                            },
                        )
                    }
                },
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterSource {
                raster: raster_operator_from_runtime(value.sources.raster.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsOnnx> for Onnx {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsOnnx) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: OnnxParameters {
                model: value.params.model.clone().into(),
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterSource {
                raster: raster_operator_from_runtime(value.sources.raster.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsRadiance> for Radiance {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsRadiance) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: RadianceParameters {},
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterSource {
                raster: raster_operator_from_runtime(value.sources.raster.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsRasterScaling> for RasterScaling {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsRasterScaling) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: RasterScalingParameters {
                slope: match &value.params.slope {
                    OperatorsSlopeOffsetSelection::Auto => {
                        SlopeOffsetSelection::Auto(SlopeOffsetSelectionAuto {
                            r#type: Default::default(),
                        })
                    }
                    OperatorsSlopeOffsetSelection::MetadataKey(key) => {
                        SlopeOffsetSelection::MetadataKey(SlopeOffsetSelectionMetadataKey {
                            r#type: Default::default(),
                            domain: key.domain.clone(),
                            key: key.key.clone(),
                        })
                    }
                    OperatorsSlopeOffsetSelection::Constant { value } => {
                        SlopeOffsetSelection::Constant(SlopeOffsetSelectionConstant {
                            r#type: Default::default(),
                            value: *value,
                        })
                    }
                },
                offset: match &value.params.offset {
                    OperatorsSlopeOffsetSelection::Auto => {
                        SlopeOffsetSelection::Auto(SlopeOffsetSelectionAuto {
                            r#type: Default::default(),
                        })
                    }
                    OperatorsSlopeOffsetSelection::MetadataKey(key) => {
                        SlopeOffsetSelection::MetadataKey(SlopeOffsetSelectionMetadataKey {
                            r#type: Default::default(),
                            domain: key.domain.clone(),
                            key: key.key.clone(),
                        })
                    }
                    OperatorsSlopeOffsetSelection::Constant { value } => {
                        SlopeOffsetSelection::Constant(SlopeOffsetSelectionConstant {
                            r#type: Default::default(),
                            value: *value,
                        })
                    }
                },
                output_measurement: value.params.output_measurement.clone().map(Into::into),
                scaling_mode: match value.params.scaling_mode {
                    OperatorsScalingMode::MulSlopeAddOffset => {
                        ScalingMode::MulSlopeAddOffset(ScalingModeMulSlopeAddOffset {
                            r#type: Default::default(),
                        })
                    }
                    OperatorsScalingMode::SubOffsetDivSlope => {
                        ScalingMode::SubOffsetDivSlope(ScalingModeSubOffsetDivSlope {
                            r#type: Default::default(),
                        })
                    }
                },
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterSource {
                raster: raster_operator_from_runtime(value.sources.raster.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsRasterization> for Rasterization {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsRasterization) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: RasterizationParameters {
                spatial_resolution: value.params.spatial_resolution.into(),
                origin_coordinate: value.params.origin_coordinate.into(),
                density_params: value.params.density_params.map(|p| DensityParams {
                    cutoff: p.cutoff,
                    stddev: p.stddev,
                }),
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleVectorSource {
                vector: crate::api::model::processing_graphs::back_conversion::vector::vector_operator_from_runtime(
                    value.sources.vector.as_ref(),
                )?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsReflectance> for Reflectance {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsReflectance) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: ReflectanceParameters {
                solar_correction: value.params.solar_correction,
                force_hrv: value.params.force_hrv,
                force_satellite: value.params.force_satellite,
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterSource {
                raster: raster_operator_from_runtime(value.sources.raster.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsTemperature> for Temperature {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsTemperature) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: TemperatureParameters {
                force_satellite: value.params.force_satellite,
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterSource {
                raster: raster_operator_from_runtime(value.sources.raster.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsTimeShift> for TimeShift {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsTimeShift) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: match &value.params {
                OperatorsTimeShiftParams::Relative { granularity, value } => {
                    TimeShiftParameters::Relative(TimeShiftParametersRelative {
                        r#type: Default::default(),
                        granularity: (*granularity).into(),
                        value: *value,
                    })
                }
                OperatorsTimeShiftParams::Absolute { time_interval } => {
                    TimeShiftParameters::Absolute(TimeShiftParametersAbsolute {
                        r#type: Default::default(),
                        time_interval: (*time_interval).into(),
                    })
                }
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorSource {
                source: match &value.sources.source {
                    geoengine_operators::util::input::RasterOrVectorOperator::Raster(raster) => {
                        crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorOperator::Raster(
                            raster_operator_from_runtime(raster.as_ref())?,
                        )
                    }
                    geoengine_operators::util::input::RasterOrVectorOperator::Vector(vector) => {
                        crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorOperator::Vector(
                            crate::api::model::processing_graphs::back_conversion::vector::vector_operator_from_runtime(vector.as_ref())?,
                        )
                    }
                },
            }
            .into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::{
        dataset::NamedData,
        machine_learning::MlModelName,
        primitives::{Coordinate2D, SpatialResolution, TimeGranularity, TimeStep},
        raster::RenameBands as RuntimeRenameBands,
    };
    use geoengine_operators::{
        engine::{
            MultipleRasterSources, SingleRasterOrVectorSource, SingleRasterSource,
            SingleVectorSource,
        },
        machine_learning::onnx::OnnxParams as OperatorsOnnxParameters,
        processing::{
            AggregateFunctionParams, BandFilter as OperatorsBandFilter,
            BandFilterParams as OperatorsBandFilterParams,
            BandNeighborhoodAggregate as OperatorsBandNeighborhoodAggregate,
            BandNeighborhoodAggregateMethod as OperatorsBandNeighborhoodAggregateMethod,
            BandNeighborhoodAggregateParams as OperatorsBandNeighborhoodAggregateParameters,
            BandwiseExpression as OperatorsBandwiseExpression,
            BandwiseExpressionParams as OperatorsBandwiseExpressionParams,
            DeriveOutRasterSpecsSource, Downsampling as OperatorsDownsampling,
            DownsamplingMethod as OperatorsDownsamplingMethod,
            DownsamplingParams as OperatorsDownsamplingParams,
            DownsamplingResolution as OperatorsDownsamplingResolution,
            Expression as OperatorsExpression, ExpressionParams as OperatorsExpressionParams,
            Interpolation as OperatorsInterpolation,
            InterpolationMethod as OperatorsInterpolationMethod,
            InterpolationParams as OperatorsInterpolationParams,
            InterpolationResolution as OperatorsInterpolationResolution,
            NeighborhoodAggregate as OperatorsNeighborhoodAggregate,
            NeighborhoodAggregateParams as OperatorsNeighborhoodAggregateParams,
            NeighborhoodParams, Radiance as OperatorsRadiance,
            RadianceParams as OperatorsRadianceParameters, RasterScaling as OperatorsRasterScaling,
            RasterScalingParams as OperatorsRasterScalingParams,
            RasterStacker as OperatorsRasterStacker,
            RasterStackerParams as OperatorsRasterStackerParams,
            RasterTypeConversion as OperatorsRasterTypeConversion,
            RasterTypeConversionParams as OperatorsRasterTypeConversionParams,
            Rasterization as OperatorsRasterization,
            RasterizationParams as OperatorsRasterizationParams,
            Reflectance as OperatorsReflectance, ReflectanceParams as OperatorsReflectanceParams,
            Reprojection as OperatorsReprojection,
            ReprojectionParams as OperatorsReprojectionParams, ScalingMode as OperatorsScalingMode,
            SlopeOffsetSelection as OperatorsSlopeOffsetSelection,
            Temperature as OperatorsTemperature, TemperatureParams as OperatorsTemperatureParams,
            TemporalRasterAggregation as OperatorsTemporalRasterAggregation,
            TemporalRasterAggregationParameters as OperatorsTemporalRasterAggregationParams,
            TimeShift as OperatorsTimeShift, TimeShiftParams as OperatorsTimeShiftParams,
        },
        source::{
            GdalSource as OperatorsGdalSource,
            GdalSourceParameters as OperatorsGdalSourceParameters,
            MultiBandGdalSource as OperatorsMultiBandGdalSource,
            MultiBandGdalSourceParameters as OperatorsMultiBandGdalSourceParameters,
        },
    };

    fn raster_source() -> Box<dyn OperatorsRasterOperator> {
        Box::new(OperatorsGdalSource {
            params: OperatorsGdalSourceParameters::new(NamedData::with_system_name("test-raster")),
        })
    }

    fn multi_band_raster_source() -> Box<dyn OperatorsRasterOperator> {
        Box::new(OperatorsMultiBandGdalSource {
            params: OperatorsMultiBandGdalSourceParameters::new(NamedData::with_system_name(
                "test-raster-multi",
            )),
        })
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "This test has to handle many different raster operator types."
    )]
    fn it_converts_runtime_raster_operators_to_processing_graph() {
        type RasterOperatorCase = (Box<dyn OperatorsRasterOperator>, fn(RasterOperator) -> bool);

        let cases: Vec<RasterOperatorCase> = vec![
            (
                Box::new(OperatorsExpression {
                    params: OperatorsExpressionParams {
                        expression: "A + B".to_string(),
                        output_type: geoengine_datatypes::raster::RasterDataType::F32,
                        output_band: None,
                        map_no_data: false,
                    },
                    sources: SingleRasterSource {
                        raster: raster_source(),
                    },
                }),
                |graph| matches!(graph, RasterOperator::Expression(_)),
            ),
            (
                Box::new(OperatorsReprojection {
                    params: OperatorsReprojectionParams {
                        target_spatial_reference:
                            geoengine_datatypes::spatial_reference::SpatialReference::epsg_4326(),
                        derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
                    },
                    sources: SingleRasterOrVectorSource {
                        source: geoengine_operators::util::input::RasterOrVectorOperator::Raster(
                            raster_source(),
                        ),
                    },
                }),
                |graph| matches!(graph, RasterOperator::Reprojection(_)),
            ),
            (
                Box::new(OperatorsTemporalRasterAggregation {
                    params: OperatorsTemporalRasterAggregationParams {
                        aggregation: geoengine_operators::processing::Aggregation::Mean {
                            ignore_no_data: false,
                        },
                        window: TimeStep::new(TimeGranularity::Months, 1)
                            .expect("valid monthly time step"),
                        window_reference: None,
                        output_type: None,
                    },
                    sources: SingleRasterSource {
                        raster: raster_source(),
                    },
                }),
                |graph| matches!(graph, RasterOperator::TemporalRasterAggregation(_)),
            ),
            (
                Box::new(OperatorsRasterStacker {
                    params: OperatorsRasterStackerParams {
                        rename_bands: RuntimeRenameBands::Default,
                    },
                    sources: MultipleRasterSources {
                        rasters: vec![raster_source(), raster_source()],
                    },
                }),
                |graph| matches!(graph, RasterOperator::RasterStacker(_)),
            ),
            (
                Box::new(OperatorsRasterTypeConversion {
                    params: OperatorsRasterTypeConversionParams {
                        output_data_type: geoengine_datatypes::raster::RasterDataType::U8,
                    },
                    sources: SingleRasterSource {
                        raster: raster_source(),
                    },
                }),
                |graph| matches!(graph, RasterOperator::RasterTypeConversion(_)),
            ),
            (
                Box::new(OperatorsInterpolation {
                    params: OperatorsInterpolationParams {
                        interpolation: OperatorsInterpolationMethod::NearestNeighbor,
                        output_resolution: OperatorsInterpolationResolution::Fraction(
                            geoengine_operators::processing::Fraction { x: 2.0, y: 2.0 },
                        ),
                        output_origin_reference: Some(Coordinate2D::new(0.0, 0.0)),
                    },
                    sources: SingleRasterSource {
                        raster: multi_band_raster_source(),
                    },
                }),
                |graph| matches!(graph, RasterOperator::Interpolation(_)),
            ),
            (
                Box::new(OperatorsDownsampling {
                    params: OperatorsDownsamplingParams {
                        sampling_method: OperatorsDownsamplingMethod::NearestNeighbor,
                        output_resolution: OperatorsDownsamplingResolution::Fraction(
                            geoengine_operators::processing::Fraction { x: 2.0, y: 2.0 },
                        ),
                        output_origin_reference: None,
                    },
                    sources: SingleRasterSource {
                        raster: multi_band_raster_source(),
                    },
                }),
                |graph| matches!(graph, RasterOperator::Downsampling(_)),
            ),
            (
                Box::new(OperatorsBandFilter {
                    params: OperatorsBandFilterParams {
                        bands: geoengine_operators::processing::BandsByNameOrIndex::Index(vec![0]),
                    },
                    sources: SingleRasterSource {
                        raster: multi_band_raster_source(),
                    },
                }),
                |graph| matches!(graph, RasterOperator::BandFilter(_)),
            ),
            (
                Box::new(OperatorsBandNeighborhoodAggregate {
                    params: OperatorsBandNeighborhoodAggregateParameters {
                        aggregate: OperatorsBandNeighborhoodAggregateMethod::Average {
                            window_size: 3,
                        },
                    },
                    sources: SingleRasterSource {
                        raster: multi_band_raster_source(),
                    },
                }),
                |graph| matches!(graph, RasterOperator::BandNeighborhoodAggregate(_)),
            ),
            (
                Box::new(OperatorsBandwiseExpression {
                    params: OperatorsBandwiseExpressionParams {
                        expression: "x * 2.0".to_string(),
                        output_type: geoengine_datatypes::raster::RasterDataType::F64,
                        map_no_data: false,
                    },
                    sources: SingleRasterSource {
                        raster: multi_band_raster_source(),
                    },
                }),
                |graph| matches!(graph, RasterOperator::BandwiseExpression(_)),
            ),
            (
                Box::new(OperatorsNeighborhoodAggregate {
                    params: OperatorsNeighborhoodAggregateParams {
                        neighborhood: NeighborhoodParams::Rectangle { dimensions: [3, 3] },
                        aggregate_function: AggregateFunctionParams::Sum,
                    },
                    sources: SingleRasterSource {
                        raster: raster_source(),
                    },
                }),
                |graph| matches!(graph, RasterOperator::NeighborhoodAggregate(_)),
            ),
            (
                Box::new(OperatorsOnnx {
                    params: OperatorsOnnxParameters {
                        model: MlModelName::try_new(None::<&str>, "test-model").unwrap(),
                    },
                    sources: SingleRasterSource {
                        raster: raster_source(),
                    },
                }),
                |graph| matches!(graph, RasterOperator::Onnx(_)),
            ),
            (
                Box::new(OperatorsRadiance {
                    params: OperatorsRadianceParameters {},
                    sources: SingleRasterSource {
                        raster: raster_source(),
                    },
                }),
                |graph| matches!(graph, RasterOperator::Radiance(_)),
            ),
            (
                Box::new(OperatorsRasterScaling {
                    params: OperatorsRasterScalingParams {
                        slope: OperatorsSlopeOffsetSelection::Constant { value: 1.0 },
                        offset: OperatorsSlopeOffsetSelection::Constant { value: 0.0 },
                        output_measurement: None,
                        scaling_mode: OperatorsScalingMode::MulSlopeAddOffset,
                    },
                    sources: SingleRasterSource {
                        raster: raster_source(),
                    },
                }),
                |graph| matches!(graph, RasterOperator::RasterScaling(_)),
            ),
            (
                Box::new(OperatorsRasterization {
                    params: OperatorsRasterizationParams {
                        spatial_resolution: SpatialResolution::new_unchecked(10.0, 10.0),
                        origin_coordinate: Coordinate2D::new(0.0, 0.0),
                        density_params: None,
                    },
                    sources: SingleVectorSource {
                        vector: Box::new(geoengine_operators::mock::MockPointSource {
                            params: geoengine_operators::mock::MockPointSourceParams {
                                points: vec![(1.0, 2.0).into()],
                                spatial_bounds:
                                    geoengine_operators::mock::SpatialBoundsDerive::None,
                            },
                        }),
                    },
                }),
                |graph| matches!(graph, RasterOperator::Rasterization(_)),
            ),
            (
                Box::new(OperatorsReflectance {
                    params: OperatorsReflectanceParams {
                        solar_correction: true,
                        force_hrv: false,
                        force_satellite: None,
                    },
                    sources: SingleRasterSource {
                        raster: raster_source(),
                    },
                }),
                |graph| matches!(graph, RasterOperator::Reflectance(_)),
            ),
            (
                Box::new(OperatorsTemperature {
                    params: OperatorsTemperatureParams {
                        force_satellite: None,
                    },
                    sources: SingleRasterSource {
                        raster: raster_source(),
                    },
                }),
                |graph| matches!(graph, RasterOperator::Temperature(_)),
            ),
            (
                Box::new(OperatorsTimeShift {
                    params: OperatorsTimeShiftParams::Relative {
                        granularity: geoengine_datatypes::primitives::TimeGranularity::Months,
                        value: -1,
                    },
                    sources: SingleRasterOrVectorSource {
                        source: geoengine_operators::util::input::RasterOrVectorOperator::Raster(
                            raster_source(),
                        ),
                    },
                }),
                |graph| matches!(graph, RasterOperator::TimeShift(_)),
            ),
        ];

        for (operator, assert_variant) in cases {
            let converted = raster_operator_from_runtime(operator.as_ref()).unwrap_or_else(|err| {
                panic!(
                    "runtime raster conversion failed for {}: {err}",
                    operator.typetag_name()
                )
            });
            assert!(
                assert_variant(converted),
                "runtime raster conversion failed for {}",
                operator.typetag_name()
            );
        }
    }
}
