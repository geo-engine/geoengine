use crate::api::model::processing_graphs::{
    BoxPlot, BoxPlotParameters, ClassHistogram, ClassHistogramParameters,
    FeatureAttributeValuesOverTime, FeatureAttributeValuesOverTimeParameters, Histogram,
    HistogramBounds, HistogramBoundsValues, HistogramBuckets, HistogramBucketsNumber,
    HistogramBucketsSquareRootChoiceRule, HistogramParameters, MeanRasterPixelValuesOverTime,
    MeanRasterPixelValuesOverTimeParameters, MeanRasterPixelValuesOverTimePosition, PieChart,
    PieChartParameters, PlotOperator, ScatterPlot, ScatterPlotParameters, Statistics,
    StatisticsParameters, plots::PieChartCountType,
};
use geoengine_operators::{
    engine::{OperatorName, TypedPlotOperator as OperatorsTypedPlotOperator},
    plot::{
        BoxPlot as OperatorsBoxPlot, ClassHistogram as OperatorsClassHistogram,
        FeatureAttributeValuesOverTime as OperatorsFeatureAttributeValuesOverTime,
        Histogram as OperatorsHistogram,
        MeanRasterPixelValuesOverTime as OperatorsMeanRasterPixelValuesOverTime,
        PieChart as OperatorsPieChart, ScatterPlot as OperatorsScatterPlot,
        Statistics as OperatorsStatistics,
    },
};
use typed_floats::tf64::NonNaN;

/// Converts a plot operator from `operators` into an API plot operator.
pub fn plot_operator_from_runtime(
    operator: &OperatorsTypedPlotOperator,
) -> anyhow::Result<PlotOperator> {
    PlotOperator::try_from(operator)
}

fn raster_operator_from_runtime(
    operator: &geoengine_operators::engine::TypedRasterOperator,
) -> anyhow::Result<crate::api::model::processing_graphs::RasterOperator> {
    crate::api::model::processing_graphs::back_conversion::raster::raster_operator_from_runtime(
        operator,
    )
}

fn vector_operator_from_runtime(
    operator: &geoengine_operators::engine::TypedVectorOperator,
) -> anyhow::Result<crate::api::model::processing_graphs::VectorOperator> {
    crate::api::model::processing_graphs::back_conversion::vector::vector_operator_from_runtime(
        operator,
    )
}

impl TryFrom<&OperatorsBoxPlot> for BoxPlot {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsBoxPlot) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: BoxPlotParameters {
                attributes: value.params.column_names.clone().try_into()?,
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorSource {
                source: match &value.sources.source {
                    geoengine_operators::util::input::RasterOrVectorOperator::Raster(raster) => {
                        crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorOperator::Raster(
                            raster_operator_from_runtime(raster)?,
                        )
                    }
                    geoengine_operators::util::input::RasterOrVectorOperator::Vector(vector) => {
                        crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorOperator::Vector(
                            vector_operator_from_runtime(vector)?,
                        )
                    }
                },
            },
        })
    }
}

impl TryFrom<&OperatorsClassHistogram> for ClassHistogram {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsClassHistogram) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: ClassHistogramParameters {
                column_name: value.params.column_name.clone(),
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorSource {
                source: match &value.sources.source {
                    geoengine_operators::util::input::RasterOrVectorOperator::Raster(raster) => {
                        crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorOperator::Raster(
                            raster_operator_from_runtime(raster)?,
                        )
                    }
                    geoengine_operators::util::input::RasterOrVectorOperator::Vector(vector) => {
                        crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorOperator::Vector(
                            vector_operator_from_runtime(vector)?,
                        )
                    }
                },
            },
        })
    }
}

impl TryFrom<&OperatorsFeatureAttributeValuesOverTime> for FeatureAttributeValuesOverTime {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsFeatureAttributeValuesOverTime) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: FeatureAttributeValuesOverTimeParameters {
                id_column: value.params.id_column.clone(),
                value_column: value.params.value_column.clone(),
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleVectorSource {
                vector: vector_operator_from_runtime(&value.sources.vector)?,
            },
        })
    }
}

impl TryFrom<&OperatorsHistogram> for Histogram {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsHistogram) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: HistogramParameters {
                column_name: value.params.attribute_name.clone(),
                bounds: match &value.params.bounds {
                    geoengine_operators::plot::HistogramBounds::Data(_) => {
                        HistogramBounds::Data(Default::default())
                    }
                    geoengine_operators::plot::HistogramBounds::Values { min, max } => {
                        HistogramBounds::Values(HistogramBoundsValues {
                            min: NonNaN::new(*min).map_err(|_| {
                                anyhow::anyhow!("histogram min bound for {} must be finite", OperatorsHistogram::TYPE_NAME)
                            })?,
                            max: NonNaN::new(*max).map_err(|_| {
                                anyhow::anyhow!("histogram max bound for {} must be finite", OperatorsHistogram::TYPE_NAME)
                            })?,
                        })
                    }
                },
                buckets: match &value.params.buckets {
                    geoengine_operators::plot::HistogramBuckets::Number { value } => {
                        HistogramBuckets::Number(HistogramBucketsNumber {
                            r#type: Default::default(),
                            value: *value,
                        })
                    }
                    geoengine_operators::plot::HistogramBuckets::SquareRootChoiceRule {
                        max_number_of_buckets,
                    } => HistogramBuckets::SquareRootChoiceRule(HistogramBucketsSquareRootChoiceRule {
                        r#type: Default::default(),
                        max_number_of_buckets: *max_number_of_buckets,
                    }),
                },
                interactive: value.params.interactive,
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorSource {
                source: match &value.sources.source {
                    geoengine_operators::util::input::RasterOrVectorOperator::Raster(raster) => {
                        crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorOperator::Raster(
                            raster_operator_from_runtime(raster)?,
                        )
                    }
                    geoengine_operators::util::input::RasterOrVectorOperator::Vector(vector) => {
                        crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorOperator::Vector(
                            vector_operator_from_runtime(vector)?,
                        )
                    }
                },
            },
        })
    }
}

impl TryFrom<&OperatorsMeanRasterPixelValuesOverTime> for MeanRasterPixelValuesOverTime {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsMeanRasterPixelValuesOverTime) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: MeanRasterPixelValuesOverTimeParameters {
                time_position: match value.params.time_position {
                    geoengine_operators::plot::MeanRasterPixelValuesOverTimePosition::Start => {
                        MeanRasterPixelValuesOverTimePosition::Start
                    }
                    geoengine_operators::plot::MeanRasterPixelValuesOverTimePosition::Center => {
                        MeanRasterPixelValuesOverTimePosition::Center
                    }
                    geoengine_operators::plot::MeanRasterPixelValuesOverTimePosition::End => {
                        MeanRasterPixelValuesOverTimePosition::End
                    }
                },
                area: value.params.area,
                attributes: value.params.band_names.clone().try_into()?,
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterSource {
                raster: raster_operator_from_runtime(&value.sources.raster)?,
            },
        })
    }
}

impl TryFrom<&OperatorsPieChart> for PieChart {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsPieChart) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: match &value.params {
                geoengine_operators::plot::PieChartParams::Count { column_name, donut } => {
                    PieChartParameters::Count(PieChartCountType {
                        r#type: Default::default(),
                        column_name: column_name.clone(),
                        donut: *donut,
                    })
                }
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleVectorSource {
                vector: vector_operator_from_runtime(&value.sources.vector)?,
            },
        })
    }
}

impl TryFrom<&OperatorsScatterPlot> for ScatterPlot {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsScatterPlot) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: ScatterPlotParameters {
                column_x: value.params.column_x.clone(),
                column_y: value.params.column_y.clone(),
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleVectorSource {
                vector: vector_operator_from_runtime(&value.sources.vector)?,
            },
        })
    }
}

impl TryFrom<&OperatorsStatistics> for Statistics {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsStatistics) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: StatisticsParameters {
                attributes: value.params.column_names.clone().try_into()?,
                percentiles: value.params.percentiles.clone(),
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorSource {
                source: match &value.sources.source {
                    geoengine_operators::util::input::RasterOrVectorOperator::Raster(raster) => {
                        crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorOperator::Raster(
                            raster_operator_from_runtime(raster)?,
                        )
                    }
                    geoengine_operators::util::input::RasterOrVectorOperator::Vector(vector) => {
                        crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorOperator::Vector(
                            vector_operator_from_runtime(vector)?,
                        )
                    }
                },
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::dataset::NamedData;
    use geoengine_operators::{
        engine::{
            SingleRasterOrVectorSource, SingleRasterSource, SingleVectorSource, TypedPlotOperator,
            TypedRasterOperator, TypedVectorOperator,
        },
        mock::{MockPointSource, MockPointSourceParams, SpatialBoundsDerive},
        plot::{
            BoxPlot as OperatorsBoxPlot, BoxPlotParams, ClassHistogram as OperatorsClassHistogram,
            ClassHistogramParams,
            FeatureAttributeValuesOverTime as OperatorsFeatureAttributeValuesOverTime,
            FeatureAttributeValuesOverTimeParams, Histogram as OperatorsHistogram, HistogramBounds,
            HistogramBuckets, HistogramParams,
            MeanRasterPixelValuesOverTime as OperatorsMeanRasterPixelValuesOverTime,
            MeanRasterPixelValuesOverTimeParams, MeanRasterPixelValuesOverTimePosition,
            PieChart as OperatorsPieChart, PieChartParams, ScatterPlot as OperatorsScatterPlot,
            ScatterPlotParams, Statistics as OperatorsStatistics, StatisticsParams,
        },
        source::{
            GdalSource as OperatorsGdalSource,
            GdalSourceParameters as OperatorsGdalSourceParameters,
        },
        util::input::RasterOrVectorOperator,
    };

    fn raster_source() -> geoengine_operators::engine::TypedRasterOperator {
        TypedRasterOperator::from(OperatorsGdalSource {
            params: OperatorsGdalSourceParameters::new(NamedData::with_system_name("test-raster")),
        })
    }

    fn vector_source() -> geoengine_operators::engine::TypedVectorOperator {
        TypedVectorOperator::from(MockPointSource {
            params: MockPointSourceParams {
                points: vec![(1., 2.).into()],
                spatial_bounds: SpatialBoundsDerive::None,
            },
        })
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "Test contains many cases for different plot operators"
    )]
    fn it_converts_runtime_plot_operators_to_processing_graph() {
        type PlotOperatorCase = (OperatorsTypedPlotOperator, fn(PlotOperator) -> bool);

        let cases: Vec<PlotOperatorCase> = vec![
            (
                TypedPlotOperator::from(OperatorsBoxPlot {
                    params: BoxPlotParams {
                        column_names: vec![],
                    },
                    sources: SingleRasterOrVectorSource {
                        source: RasterOrVectorOperator::Raster(raster_source()),
                    },
                }),
                |graph| matches!(graph, PlotOperator::BoxPlot(_)),
            ),
            (
                TypedPlotOperator::from(OperatorsClassHistogram {
                    params: ClassHistogramParams { column_name: None },
                    sources: SingleRasterOrVectorSource {
                        source: RasterOrVectorOperator::Raster(raster_source()),
                    },
                }),
                |graph| matches!(graph, PlotOperator::ClassHistogram(_)),
            ),
            (
                TypedPlotOperator::from(OperatorsFeatureAttributeValuesOverTime {
                    params: FeatureAttributeValuesOverTimeParams {
                        id_column: "id".to_string(),
                        value_column: "value".to_string(),
                    },
                    sources: SingleVectorSource {
                        vector: vector_source(),
                    },
                }),
                |graph| matches!(graph, PlotOperator::FeatureAttributeValuesOverTime(_)),
            ),
            (
                TypedPlotOperator::from(OperatorsHistogram {
                    params: HistogramParams {
                        attribute_name: "value".to_string(),
                        bounds: HistogramBounds::Data(Default::default()),
                        buckets: HistogramBuckets::Number { value: 10 },
                        interactive: false,
                    },
                    sources: SingleRasterOrVectorSource {
                        source: RasterOrVectorOperator::Raster(raster_source()),
                    },
                }),
                |graph| matches!(graph, PlotOperator::Histogram(_)),
            ),
            (
                TypedPlotOperator::from(OperatorsMeanRasterPixelValuesOverTime {
                    params: MeanRasterPixelValuesOverTimeParams {
                        time_position: MeanRasterPixelValuesOverTimePosition::Start,
                        area: true,
                        band_names: vec![],
                    },
                    sources: SingleRasterSource {
                        raster: raster_source(),
                    },
                }),
                |graph| matches!(graph, PlotOperator::MeanRasterPixelValuesOverTime(_)),
            ),
            (
                TypedPlotOperator::from(OperatorsPieChart {
                    params: PieChartParams::Count {
                        column_name: "type".to_string(),
                        donut: false,
                    },
                    sources: SingleVectorSource {
                        vector: vector_source(),
                    },
                }),
                |graph| matches!(graph, PlotOperator::PieChart(_)),
            ),
            (
                TypedPlotOperator::from(OperatorsScatterPlot {
                    params: ScatterPlotParams {
                        column_x: "x".to_string(),
                        column_y: "y".to_string(),
                    },
                    sources: SingleVectorSource {
                        vector: vector_source(),
                    },
                }),
                |graph| matches!(graph, PlotOperator::ScatterPlot(_)),
            ),
            (
                TypedPlotOperator::from(OperatorsStatistics {
                    params: StatisticsParams {
                        column_names: vec![],
                        percentiles: vec![],
                    },
                    sources: SingleRasterOrVectorSource {
                        source: RasterOrVectorOperator::Raster(raster_source()),
                    },
                }),
                |graph| matches!(graph, PlotOperator::Statistics(_)),
            ),
        ];

        for (operator, assert_variant) in cases {
            let converted = plot_operator_from_runtime(&operator).unwrap();
            assert!(
                assert_variant(converted),
                "runtime plot conversion failed for {}",
                operator.type_name()
            );
        }
    }
}
