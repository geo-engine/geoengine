use geoengine_operators::engine::{
    TypedOperator as OperatorsTypedOperator, TypedPlotOperator as OperatorsTypedPlotOperator,
    TypedRasterOperator as OperatorsTypedRasterOperator,
    TypedVectorOperator as OperatorsTypedVectorOperator,
};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

mod back_conversion;
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
        MultipleRasterSources, SingleRasterOrVectorOperator, SingleRasterOrVectorSource,
        SingleRasterSource, SingleVectorMultipleRasterSources,
    },
};
use crate::identifier;
use crate::processing_graphs::processing_graph::{
    ProcessingGraph as ServicesProcessingGraph, ProcessingGraphId as ServicesProcessingGraphId,
};

// Identifier for registered processing graphs
identifier!(ProcessingGraphId);

impl From<ServicesProcessingGraphId> for ProcessingGraphId {
    fn from(value: ServicesProcessingGraphId) -> Self {
        Self(value.0)
    }
}

impl From<ProcessingGraphId> for ServicesProcessingGraphId {
    fn from(value: ProcessingGraphId) -> Self {
        Self(value.0)
    }
}

/// Operator outputs are distinguished by their data type.
/// There are `raster`, `vector` and `plot` operators.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[schema(no_recursion)]
#[serde(tag = "type", content = "operator")]
pub enum ProcessingGraph {
    #[schema(title = "TypedVectorOperator")]
    Vector(VectorOperator),
    #[schema(title = "TypedRasterOperator")]
    Raster(RasterOperator),
    #[schema(title = "TypedPlotOperator")]
    Plot(PlotOperator),
}

impl TryFrom<ProcessingGraph> for ServicesProcessingGraph {
    type Error = anyhow::Error;

    fn try_from(value: ProcessingGraph) -> Result<Self, Self::Error> {
        let operator = match value {
            ProcessingGraph::Vector(operator) => {
                OperatorsTypedOperator::Vector(operator.try_into()?)
            }
            ProcessingGraph::Raster(operator) => {
                OperatorsTypedOperator::Raster(operator.try_into()?)
            }
            ProcessingGraph::Plot(operator) => OperatorsTypedOperator::Plot(operator.try_into()?),
        };
        Ok(Self { operator })
    }
}

impl TryFrom<&ServicesProcessingGraph> for ProcessingGraph {
    type Error = anyhow::Error;

    fn try_from(value: &ServicesProcessingGraph) -> Result<Self, Self::Error> {
        Ok(
            match back_conversion::services_processing_graph_to_api(value)? {
                TypedOperator::Vector(operator) => ProcessingGraph::Vector(operator),
                TypedOperator::Raster(operator) => ProcessingGraph::Raster(operator),
                TypedOperator::Plot(operator) => ProcessingGraph::Plot(operator),
            },
        )
    }
}

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
    TimeShift(TimeShift),
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

/// Generates the conversions between an API operator enum and its counterpart in `operators`.
/// Both enums use the same variant names.
/// Operators that only exist in `operators`, like mocks, cannot be converted to API operators.
macro_rules! operator_conversions {
    ($api:ident <=> $operators:ident { $($variant:ident),+ $(,)? }) => {
        impl TryFrom<$api> for $operators {
            type Error = anyhow::Error;

            fn try_from(operator: $api) -> Result<Self, Self::Error> {
                Ok(match operator {
                    $($api::$variant(operator) => Self::$variant(Box::new(operator.try_into()?)),)+
                })
            }
        }

        impl TryFrom<&$operators> for $api {
            type Error = anyhow::Error;

            #[allow(unreachable_patterns)] // if all operators have an API counterpart
            fn try_from(operator: &$operators) -> Result<Self, Self::Error> {
                match operator {
                    $($operators::$variant(operator) => Ok(Self::$variant(operator.as_ref().try_into()?)),)+
                    operator => anyhow::bail!(
                        "cannot convert runtime operator {} to ProcessingGraph",
                        operator.type_name()
                    ),
                }
            }
        }
    };
}

operator_conversions!(RasterOperator <=> OperatorsTypedRasterOperator {
    BandFilter,
    BandNeighborhoodAggregate,
    BandwiseExpression,
    Downsampling,
    Expression,
    GdalSource,
    Interpolation,
    MultiBandGdalSource,
    NeighborhoodAggregate,
    Onnx,
    RasterScaling,
    RasterStacker,
    RasterTypeConversion,
    Rasterization,
    Reprojection,
    Reflectance,
    Radiance,
    TemporalRasterAggregation,
    Temperature,
    TimeShift,
});

operator_conversions!(VectorOperator <=> OperatorsTypedVectorOperator {
    ColumnRangeFilter,
    LineSimplification,
    MockPointSource,
    OgrSource,
    PointInPolygonFilter,
    RasterVectorJoin,
    Reprojection,
    TimeProjection,
    TimeShift,
    VectorExpression,
    VectorJoin,
    VisualPointClustering,
});

operator_conversions!(PlotOperator <=> OperatorsTypedPlotOperator {
    BoxPlot,
    ClassHistogram,
    FeatureAttributeValuesOverTime,
    Histogram,
    MeanRasterPixelValuesOverTime,
    PieChart,
    ScatterPlot,
    Statistics,
});

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
    ProcessingGraph,
    RasterOperator,
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
    MultipleRasterSources,
    SingleRasterOrVectorOperator,
    SingleRasterOrVectorSource,
    SingleRasterSource,
    SingleVectorMultipleRasterSources,

)))]
pub struct OperatorsApi;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::model::datatypes::Coordinate2D;
    use crate::processing_graphs::processing_graph::ProcessingGraph as ServicesProcessingGraph;
    use geoengine_operators::engine::TypedVectorOperator;

    #[test]
    fn it_serializes_processing_graphs() {
        let processing_graph =
            ProcessingGraph::Vector(VectorOperator::MockPointSource(MockPointSource {
                r#type: Default::default(),
                params: MockPointSourceParameters {
                    points: vec![Coordinate2D { x: 1., y: 2. }; 3],
                    spatial_bounds: SpatialBoundsDerive::None(Default::default()),
                },
            }));

        let serialized_processing_graph = serde_json::to_value(&processing_graph).unwrap();

        assert_eq!(
            serialized_processing_graph,
            serde_json::json!({
                "type": "Vector",
                "operator": {
                    "type": "MockPointSource",
                    "params": {
                        "points": [{
                            "x": 1.0,
                            "y": 2.0
                        }, {
                            "x": 1.0,
                            "y": 2.0
                        }, {
                            "x": 1.0,
                            "y": 2.0
                        }],
                    }
                }
            })
        );

        let deserialized_processing_graph: ProcessingGraph =
            serde_json::from_value(serialized_processing_graph).unwrap();

        assert_eq!(processing_graph, deserialized_processing_graph);
    }

    #[test]
    fn it_converts_services_processing_graph_to_api_for_mock_point_source() {
        let processing_graph = ServicesProcessingGraph {
            operator: OperatorsTypedOperator::Vector(TypedVectorOperator::from(
                geoengine_operators::mock::MockPointSource {
                    params: geoengine_operators::mock::MockPointSourceParams {
                        points: vec![(1., 2.).into()],
                        spatial_bounds: geoengine_operators::mock::SpatialBoundsDerive::None,
                    },
                },
            )),
        };

        let processing_graph = ProcessingGraph::try_from(&processing_graph).unwrap();

        assert!(matches!(
            processing_graph,
            ProcessingGraph::Vector(VectorOperator::MockPointSource(_))
        ));
    }
}
