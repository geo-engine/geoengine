use geoengine_operators::{
    engine::{
        PlotOperator as OperatorsPlotOperator, RasterOperator as OperatorsRasterOperator,
        TypedOperator as OperatorsTypedOperator, VectorOperator as OperatorsVectorOperator,
    },
    machine_learning::onnx::Onnx as OperatorsOnnx,
    mock::MockPointSource as OperatorsMockPointSource,
    plot::{
        BoxPlot as OperatorsBoxPlot, ClassHistogram as OperatorsClassHistogram,
        FeatureAttributeValuesOverTime as OperatorsFeatureAttributeValuesOverTime,
        Histogram as OperatorsHistogram,
        MeanRasterPixelValuesOverTime as OperatorsMeanRasterPixelValuesOverTime,
        PieChart as OperatorsPieChart, ScatterPlot as OperatorsScatterPlot,
        Statistics as OperatorsStatistics,
    },
    processing::{
        BandFilter as OperatorsBandFilter,
        BandNeighborhoodAggregate as OperatorsBandNeighborhoodAggregate,
        BandwiseExpression as OperatorsBandwiseExpression,
        ColumnRangeFilter as OperatorsColumnRangeFilter, Downsampling as OperatorsDownsampling,
        Expression as OperatorsExpression, Interpolation as OperatorsInterpolation,
        LineSimplification as OperatorsLineSimplification,
        NeighborhoodAggregate as OperatorsNeighborhoodAggregate,
        PointInPolygonFilter as OperatorsPointInPolygonFilter, Radiance as OperatorsRadiance,
        RasterScaling as OperatorsRasterScaling, RasterStacker as OperatorsRasterStacker,
        RasterTypeConversion as OperatorsRasterTypeConversion,
        RasterVectorJoin as OperatorsRasterVectorJoin, Rasterization as OperatorsRasterization,
        Reflectance as OperatorsReflectance, Reprojection as OperatorsReprojection,
        Temperature as OperatorsTemperature,
        TemporalRasterAggregation as OperatorsTemporalRasterAggregation,
        TimeProjection as OperatorsTimeProjection, TimeShift as OperatorsTimeShift,
        VectorExpression as OperatorsVectorExpression, VectorJoin as OperatorsVectorJoin,
        VisualPointClustering as OperatorsVisualPointClustering,
    },
    source::{
        GdalSource as OperatorsGdalSource, MultiBandGdalSource as OperatorsMultiBandGdalSource,
        OgrSource as OperatorsOgrSource,
    },
};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

mod macros;
mod parameters;
mod plots;
pub(crate) mod processing;
mod source;
mod source_parameters;

// TODO: avoid exporting them to outside of API module
#[cfg(test)]
pub(crate) use crate::api::model::processing_graphs::parameters::SpatialBoundsDerive;
pub use crate::api::model::processing_graphs::{
    plots::{
        BoxPlot, BoxPlotParameters, ClassHistogram, ClassHistogramParameters,
        FeatureAttributeValuesOverTime, FeatureAttributeValuesOverTimeParameters, Histogram,
        HistogramBounds, HistogramBoundsValues, HistogramBuckets, HistogramBucketsNumber,
        HistogramBucketsSquareRootChoiceRule, HistogramParameters, MeanRasterPixelValuesOverTime,
        MeanRasterPixelValuesOverTimeParameters, MeanRasterPixelValuesOverTimePosition, PieChart,
        PieChartParameters, ScatterPlot, ScatterPlotParameters, Statistics, StatisticsParameters,
    },
    processing::{
        Aggregation, AggregationMin, BandFilter, BandFilterParameters, BandNeighborhoodAggregate,
        BandNeighborhoodAggregateMethod, BandNeighborhoodAggregateParameters, BandsByNameOrIndex,
        BandwiseExpression, BandwiseExpressionParameters, ColumnRangeFilter,
        ColumnRangeFilterParameters, DeriveOutRasterSpecsSource, Downsampling, DownsamplingMethod,
        DownsamplingParameters, DownsamplingResolution, Expression, ExpressionParameters,
        Interpolation, InterpolationMethod, InterpolationParameters, InterpolationResolution,
        InterpolationResolutionFraction, LineSimplification, LineSimplificationAlgorithm,
        LineSimplificationParameters, NeighborhoodAggregate, NeighborhoodAggregateParameters,
        NeighborhoodKernel, Onnx, OnnxParameters, PointInPolygonFilter,
        PointInPolygonFilterParameters, PointInPolygonFilterSource, Radiance, RadianceParameters,
        RasterScaling, RasterScalingParameters, RasterStacker, RasterStackerParameters,
        RasterTypeConversion, RasterTypeConversionParameters, RasterVectorJoin,
        RasterVectorJoinParameters, Rasterization, RasterizationParameters, Reflectance,
        ReflectanceParameters, RenameBands, Reprojection, ReprojectionParameters, Temperature,
        TemperatureParameters, TemporalRasterAggregation, TemporalRasterAggregationParameters,
        TimeProjection, TimeProjectionParameters, TimeShift, TimeShiftParameters, VectorExpression,
        VectorExpressionParameters, VectorJoin, VectorJoinParameters, VectorJoinSources,
        VisualPointClustering, VisualPointClusteringParameters,
    },
    source::{
        AttributeFilter, GdalSource, GdalSourceParameters, MockPointSource,
        MockPointSourceParameters, MultiBandGdalSource, OgrSource, OgrSourceParameters,
    },
    source_parameters::{
        MultipleRasterOrSingleVectorOperator, MultipleRasterOrSingleVectorSource,
        MultipleRasterSources, SingleRasterOrVectorOperator, SingleRasterOrVectorSource,
        SingleRasterSource, SingleVectorMultipleRasterSources,
    },
};

/// Operator outputs are distinguished by their data type.
/// There are `raster`, `vector` and `plot` operators.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[schema(no_recursion)]
#[serde(tag = "type", content = "operator")]
pub enum TypedOperator {
    #[schema(title = "TypedVectorOperator")]
    Vector(VectorOperator),
    #[schema(title = "TypedRasterOperator")]
    Raster(RasterOperator),
    #[schema(title = "TypedPlotOperator")]
    Plot(PlotOperator),
}

/// An operator that produces raster data.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[schema(no_recursion)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum RasterOperator {
    BandFilter(BandFilter),
    BandNeighborhoodAggregate(BandNeighborhoodAggregate),
    BandwiseExpression(BandwiseExpression),
    Downsampling(Downsampling),
    Expression(Expression),
    GdalSource(GdalSource),
    Interpolation(Interpolation),
    MultiBandGdalSource(MultiBandGdalSource),
    NeighborhoodAggregate(NeighborhoodAggregate),
    Onnx(Onnx),
    RasterScaling(RasterScaling),
    RasterStacker(RasterStacker),
    RasterTypeConversion(RasterTypeConversion),
    Rasterization(Rasterization),
    Reprojection(Reprojection),
    Reflectance(Reflectance),
    Radiance(Radiance),
    TemporalRasterAggregation(TemporalRasterAggregation),
    Temperature(Temperature),
    TimeShift(TimeShift),
}

/// An operator that produces vector data.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[schema(no_recursion)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum VectorOperator {
    ColumnRangeFilter(ColumnRangeFilter),
    LineSimplification(LineSimplification),
    MockPointSource(MockPointSource),
    OgrSource(OgrSource),
    PointInPolygonFilter(PointInPolygonFilter),
    RasterVectorJoin(RasterVectorJoin),
    Reprojection(Reprojection),
    TimeProjection(TimeProjection),
    VectorExpression(VectorExpression),
    VectorJoin(VectorJoin),
    VisualPointClustering(VisualPointClustering),
}

/// An operator that produces plot data.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[schema(no_recursion)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum PlotOperator {
    BoxPlot(BoxPlot),
    ClassHistogram(ClassHistogram),
    FeatureAttributeValuesOverTime(FeatureAttributeValuesOverTime),
    Histogram(Histogram),
    MeanRasterPixelValuesOverTime(MeanRasterPixelValuesOverTime),
    PieChart(PieChart),
    ScatterPlot(ScatterPlot),
    Statistics(Statistics),
}

impl TryFrom<RasterOperator> for Box<dyn OperatorsRasterOperator> {
    type Error = anyhow::Error;
    fn try_from(operator: RasterOperator) -> Result<Self, Self::Error> {
        match operator {
            RasterOperator::BandFilter(band_filter) => {
                OperatorsBandFilter::try_from(band_filter).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::BandNeighborhoodAggregate(op) => {
                OperatorsBandNeighborhoodAggregate::try_from(op).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::BandwiseExpression(op) => {
                OperatorsBandwiseExpression::try_from(op).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::Downsampling(downsampling) => {
                OperatorsDownsampling::try_from(downsampling).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::Expression(expression) => {
                OperatorsExpression::try_from(expression).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::GdalSource(gdal_source) => {
                OperatorsGdalSource::try_from(gdal_source).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::Interpolation(interpolation) => {
                OperatorsInterpolation::try_from(interpolation).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::MultiBandGdalSource(gdal_source) => {
                OperatorsMultiBandGdalSource::try_from(gdal_source)
                    .map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::NeighborhoodAggregate(op) => {
                OperatorsNeighborhoodAggregate::try_from(op).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::Onnx(op) => {
                OperatorsOnnx::try_from(op).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::RasterScaling(op) => {
                OperatorsRasterScaling::try_from(op).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::RasterStacker(raster_stacker) => {
                OperatorsRasterStacker::try_from(raster_stacker).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::RasterTypeConversion(type_conversion) => {
                OperatorsRasterTypeConversion::try_from(type_conversion)
                    .map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::Rasterization(op) => {
                OperatorsRasterization::try_from(op).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::Reprojection(reprojection) => {
                OperatorsReprojection::try_from(reprojection).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::Reflectance(op) => {
                OperatorsReflectance::try_from(op).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::Radiance(op) => {
                OperatorsRadiance::try_from(op).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::TemporalRasterAggregation(aggregation) => {
                OperatorsTemporalRasterAggregation::try_from(aggregation)
                    .map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::Temperature(op) => {
                OperatorsTemperature::try_from(op).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::TimeShift(op) => {
                OperatorsTimeShift::try_from(op).map(OperatorsRasterOperator::boxed)
            }
        }
    }
}

impl TryFrom<VectorOperator> for Box<dyn OperatorsVectorOperator> {
    type Error = anyhow::Error;
    fn try_from(operator: VectorOperator) -> Result<Self, Self::Error> {
        match operator {
            VectorOperator::ColumnRangeFilter(op) => {
                OperatorsColumnRangeFilter::try_from(op).map(OperatorsVectorOperator::boxed)
            }
            VectorOperator::LineSimplification(op) => {
                OperatorsLineSimplification::try_from(op).map(OperatorsVectorOperator::boxed)
            }
            VectorOperator::MockPointSource(mock_point_source) => {
                OperatorsMockPointSource::try_from(mock_point_source)
                    .map(OperatorsVectorOperator::boxed)
            }
            VectorOperator::OgrSource(ogr_source) => {
                OperatorsOgrSource::try_from(ogr_source).map(OperatorsVectorOperator::boxed)
            }
            VectorOperator::PointInPolygonFilter(op) => {
                OperatorsPointInPolygonFilter::try_from(op).map(OperatorsVectorOperator::boxed)
            }
            VectorOperator::RasterVectorJoin(rvj) => {
                OperatorsRasterVectorJoin::try_from(rvj).map(OperatorsVectorOperator::boxed)
            }
            VectorOperator::Reprojection(reprojection) => {
                OperatorsReprojection::try_from(reprojection).map(OperatorsVectorOperator::boxed)
            }
            VectorOperator::TimeProjection(op) => {
                OperatorsTimeProjection::try_from(op).map(OperatorsVectorOperator::boxed)
            }
            VectorOperator::VectorExpression(vector_expression) => {
                OperatorsVectorExpression::try_from(vector_expression)
                    .map(OperatorsVectorOperator::boxed)
            }
            VectorOperator::VectorJoin(op) => {
                OperatorsVectorJoin::try_from(op).map(OperatorsVectorOperator::boxed)
            }
            VectorOperator::VisualPointClustering(op) => {
                OperatorsVisualPointClustering::try_from(op).map(OperatorsVectorOperator::boxed)
            }
        }
    }
}

impl TryFrom<PlotOperator> for Box<dyn OperatorsPlotOperator> {
    type Error = anyhow::Error;
    fn try_from(operator: PlotOperator) -> Result<Self, Self::Error> {
        match operator {
            PlotOperator::BoxPlot(box_plot) => {
                OperatorsBoxPlot::try_from(box_plot).map(OperatorsPlotOperator::boxed)
            }
            PlotOperator::ClassHistogram(class_histogram) => {
                OperatorsClassHistogram::try_from(class_histogram).map(OperatorsPlotOperator::boxed)
            }
            PlotOperator::FeatureAttributeValuesOverTime(feature_attribute_values_over_time) => {
                OperatorsFeatureAttributeValuesOverTime::try_from(
                    feature_attribute_values_over_time,
                )
                .map(OperatorsPlotOperator::boxed)
            }
            PlotOperator::Histogram(histogram) => {
                OperatorsHistogram::try_from(histogram).map(OperatorsPlotOperator::boxed)
            }
            PlotOperator::MeanRasterPixelValuesOverTime(mean_raster_pixel_values_over_time) => {
                OperatorsMeanRasterPixelValuesOverTime::try_from(mean_raster_pixel_values_over_time)
                    .map(OperatorsPlotOperator::boxed)
            }
            PlotOperator::PieChart(pie_chart) => {
                OperatorsPieChart::try_from(pie_chart).map(OperatorsPlotOperator::boxed)
            }
            PlotOperator::ScatterPlot(scatter_plot) => {
                OperatorsScatterPlot::try_from(scatter_plot).map(OperatorsPlotOperator::boxed)
            }
            PlotOperator::Statistics(statistics) => {
                OperatorsStatistics::try_from(statistics).map(OperatorsPlotOperator::boxed)
            }
        }
    }
}

impl TryFrom<TypedOperator> for OperatorsTypedOperator {
    type Error = anyhow::Error;
    fn try_from(operator: TypedOperator) -> Result<Self, Self::Error> {
        match operator {
            TypedOperator::Raster(raster_operator) => Ok(Self::Raster(raster_operator.try_into()?)),
            TypedOperator::Vector(vector_operator) => Ok(Self::Vector(vector_operator.try_into()?)),
            TypedOperator::Plot(plot_operator) => Ok(Self::Plot(plot_operator.try_into()?)),
        }
    }
}

#[derive(OpenApi)]
#[openapi(components(schemas(
    // General
    PlotOperator,
    TypedOperator,
    VectorOperator,
    // Source
    GdalSource,
    GdalSourceParameters,
    MockPointSource,
    MockPointSourceParameters,
    MultiBandGdalSource,
    OgrSource,
    OgrSourceParameters,
    AttributeFilter,
    // Processing
    Aggregation,
    BandFilter,
    BandFilterParameters,
    BandNeighborhoodAggregate,
    BandNeighborhoodAggregateMethod,
    BandNeighborhoodAggregateParameters,
    BandwiseExpression,
    BandwiseExpressionParameters,
    BandsByNameOrIndex,
    ColumnRangeFilter,
    ColumnRangeFilterParameters,
    DeriveOutRasterSpecsSource,
    Downsampling,
    DownsamplingMethod,
    DownsamplingParameters,
    DownsamplingResolution,
    Expression,
    ExpressionParameters,
    Interpolation,
    InterpolationMethod,
    InterpolationParameters,
    InterpolationResolution,
    LineSimplification,
    LineSimplificationAlgorithm,
    LineSimplificationParameters,
    NeighborhoodAggregate,
    NeighborhoodAggregateParameters,
    NeighborhoodKernel,
    Onnx,
    OnnxParameters,
    PointInPolygonFilter,
    PointInPolygonFilterParameters,
    PointInPolygonFilterSource,
    RasterOperator,
    RasterScaling,
    RasterScalingParameters,
    RasterStacker,
    RasterStackerParameters,
    RasterTypeConversion,
    RasterTypeConversionParameters,
    RasterVectorJoin,
    RasterVectorJoinParameters,
    Rasterization,
    RasterizationParameters,
    Reflectance,
    ReflectanceParameters,
    Radiance,
    RadianceParameters,
    RenameBands,
    Reprojection,
    ReprojectionParameters,
    TemporalRasterAggregation,
    TemporalRasterAggregationParameters,
    Temperature,
    TemperatureParameters,
    TimeProjection,
    TimeProjectionParameters,
    TimeShift,
    TimeShiftParameters,
    VectorExpression,
    VectorExpressionParameters,
    VectorJoin,
    VectorJoinParameters,
    VectorJoinSources,
    VisualPointClustering,
    VisualPointClusteringParameters,
    // Plots
    BoxPlot,
    BoxPlotParameters,
    ClassHistogram,
    ClassHistogramParameters,
    FeatureAttributeValuesOverTime,
    FeatureAttributeValuesOverTimeParameters,
    Histogram,
    HistogramParameters,
    MeanRasterPixelValuesOverTime,
    MeanRasterPixelValuesOverTimeParameters,
    MeanRasterPixelValuesOverTimePosition,
    PieChart,
    PieChartParameters,
    ScatterPlot,
    ScatterPlotParameters,
    Statistics,
    StatisticsParameters,
    // Source Parameters
    MultipleRasterOrSingleVectorOperator,
    MultipleRasterOrSingleVectorSource,
    MultipleRasterSources,
    SingleRasterOrVectorOperator,
    SingleRasterOrVectorSource,
    SingleRasterSource,
    SingleVectorMultipleRasterSources,

)))]
pub struct OperatorsApi;
