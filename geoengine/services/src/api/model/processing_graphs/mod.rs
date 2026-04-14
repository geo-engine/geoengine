#![allow(clippy::needless_for_each)] // TODO: remove when clippy is fixed for utoipa <https://github.com/juhaku/utoipa/issues/1420>

// use crate::api::model::processing_graphs::{
//     processing::{Expression, ExpressionParameters, RasterVectorJoin, RasterVectorJoinParameters},
//     source::{GdalSource, GdalSourceParameters, MockPointSource, MockPointSourceParameters},
// };
use geoengine_operators::{
    engine::{
        PlotOperator as OperatorsPlotOperator, RasterOperator as OperatorsRasterOperator,
        TypedOperator as OperatorsTypedOperator, VectorOperator as OperatorsVectorOperator,
    },
    mock::MockPointSource as OperatorsMockPointSource,
    plot::{Histogram as OperatorsHistogram, Statistics as OperatorsStatistics},
    processing::{
        BandFilter as OperatorsBandFilter, Expression as OperatorsExpression,
        Interpolation as OperatorsInterpolation, RasterStacker as OperatorsRasterStacker,
        RasterTypeConversion as OperatorsRasterTypeConversion,
        RasterVectorJoin as OperatorsRasterVectorJoin, Reprojection as OperatorsReprojection,
        TemporalRasterAggregation as OperatorsTemporalRasterAggregation,
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
mod processing;
mod source;
mod source_parameters;

// TODO: avoid exporting them to outside of API module
#[cfg(test)]
pub(crate) use crate::api::model::processing_graphs::parameters::SpatialBoundsDerive;
pub(crate) use crate::api::model::processing_graphs::{
    plots::{Histogram, HistogramParameters, Statistics, StatisticsParameters},
    processing::{
        Aggregation, BandFilter, BandFilterParameters, BandsByNameOrIndex,
        DeriveOutRasterSpecsSource, Expression, ExpressionParameters, Interpolation,
        InterpolationMethod, InterpolationParameters, InterpolationResolution, RasterStacker,
        RasterStackerParameters, RasterTypeConversion, RasterTypeConversionParameters,
        RasterVectorJoin, RasterVectorJoinParameters, RenameBands, Reprojection,
        ReprojectionParameters, TemporalRasterAggregation, TemporalRasterAggregationParameters,
    },
    source::{
        GdalSource, GdalSourceParameters, MockPointSource, MockPointSourceParameters,
        MultiBandGdalSource, OgrSource, OgrSourceParameters,
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
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum RasterOperator {
    BandFilter(BandFilter),
    Expression(Expression),
    GdalSource(GdalSource),
    Interpolation(Interpolation),
    MultiBandGdalSource(MultiBandGdalSource),
    RasterStacker(RasterStacker),
    RasterTypeConversion(RasterTypeConversion),
    Reprojection(Reprojection),
    TemporalRasterAggregation(TemporalRasterAggregation),
}

/// An operator that produces vector data.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum VectorOperator {
    MockPointSource(MockPointSource),
    OgrSource(OgrSource),
    RasterVectorJoin(RasterVectorJoin),
    Reprojection(Reprojection),
}

/// An operator that produces plot data.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum PlotOperator {
    Histogram(Histogram),
    Statistics(Statistics),
}

impl TryFrom<RasterOperator> for Box<dyn OperatorsRasterOperator> {
    type Error = anyhow::Error;
    fn try_from(operator: RasterOperator) -> Result<Self, Self::Error> {
        // TODO: Missing raster operator mappings (operators crate -> OpenAPI model):
        // [ ] BandNeighborhoodAggregate
        // [ ] BandwiseExpression
        // [ ] Downsampling
        // [ ] NeighborhoodAggregate
        // [ ] Onnx
        // [ ] RasterScaling
        // [ ] Rasterization
        // [ ] Reflectance
        // [ ] Radiance
        // [ ] Temperature
        // [ ] TimeShift
        match operator {
            RasterOperator::BandFilter(band_filter) => {
                OperatorsBandFilter::try_from(band_filter).map(OperatorsRasterOperator::boxed)
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
            RasterOperator::RasterStacker(raster_stacker) => {
                OperatorsRasterStacker::try_from(raster_stacker).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::RasterTypeConversion(type_conversion) => {
                OperatorsRasterTypeConversion::try_from(type_conversion)
                    .map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::Reprojection(reprojection) => {
                OperatorsReprojection::try_from(reprojection).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::TemporalRasterAggregation(aggregation) => {
                OperatorsTemporalRasterAggregation::try_from(aggregation)
                    .map(OperatorsRasterOperator::boxed)
            }
        }
    }
}

impl TryFrom<VectorOperator> for Box<dyn OperatorsVectorOperator> {
    type Error = anyhow::Error;
    fn try_from(operator: VectorOperator) -> Result<Self, Self::Error> {
        // TODO: Missing vector operator mappings (operators crate -> OpenAPI model):
        // [ ] ColumnRangeFilter
        // [ ] CsvSource
        // [ ] LineSimplification
        // [ ] MockDatasetDataSource
        // [ ] PointInPolygonFilter
        // [ ] TimeProjection
        // [ ] TimeShift
        // [ ] VectorExpression
        // [ ] VectorJoin
        // [ ] VisualPointClustering
        match operator {
            VectorOperator::MockPointSource(mock_point_source) => {
                OperatorsMockPointSource::try_from(mock_point_source)
                    .map(OperatorsVectorOperator::boxed)
            }
            VectorOperator::OgrSource(ogr_source) => {
                OperatorsOgrSource::try_from(ogr_source).map(OperatorsVectorOperator::boxed)
            }
            VectorOperator::RasterVectorJoin(rvj) => {
                OperatorsRasterVectorJoin::try_from(rvj).map(OperatorsVectorOperator::boxed)
            }
            VectorOperator::Reprojection(reprojection) => {
                OperatorsReprojection::try_from(reprojection).map(OperatorsVectorOperator::boxed)
            }
        }
    }
}

impl TryFrom<PlotOperator> for Box<dyn OperatorsPlotOperator> {
    type Error = anyhow::Error;
    fn try_from(operator: PlotOperator) -> Result<Self, Self::Error> {
        // TODO: Missing plot operator mappings (operators crate -> OpenAPI model):
        // [ ] BoxPlot
        // [ ] ClassHistogram
        // [ ] FeatureAttributeValuesOverTime
        // [ ] MeanRasterPixelValuesOverTime
        // [ ] PieChart
        // [ ] ScatterPlot
        match operator {
            PlotOperator::Histogram(histogram) => {
                OperatorsHistogram::try_from(histogram).map(OperatorsPlotOperator::boxed)
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
    // Processing
    Aggregation,
    BandFilter,
    BandFilterParameters,
    BandsByNameOrIndex,
    DeriveOutRasterSpecsSource,
    Expression,
    ExpressionParameters,
    Interpolation,
    InterpolationMethod,
    InterpolationParameters,
    InterpolationResolution,
    RasterOperator,
    RasterStacker,
    RasterStackerParameters,
    RasterTypeConversion,
    RasterTypeConversionParameters,
    RasterVectorJoin,
    RasterVectorJoinParameters,
    RenameBands,
    Reprojection,
    ReprojectionParameters,
    TemporalRasterAggregation,
    TemporalRasterAggregationParameters,
    // Plots
    Histogram,
    HistogramParameters,
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
