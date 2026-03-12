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
        Expression as OperatorsExpression, RasterVectorJoin as OperatorsRasterVectorJoin,
    },
    source::GdalSource as OperatorsGdalSource,
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
    processing::{Expression, ExpressionParameters, RasterVectorJoin, RasterVectorJoinParameters},
    source::{GdalSource, GdalSourceParameters, MockPointSource, MockPointSourceParameters},
    source_parameters::{
        MultipleRasterOrSingleVectorOperator, MultipleRasterOrSingleVectorSource,
        SingleRasterOrVectorOperator, SingleRasterOrVectorSource, SingleRasterSource,
        SingleVectorMultipleRasterSources,
    },
};

/// Operator outputs are distinguished by their data type.
/// There are `raster`, `vector` and `plot` operators.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(tag = "type", content = "operator")]
pub enum TypedOperator {
    Vector(VectorOperator),
    Raster(RasterOperator),
    Plot(PlotOperator),
}

/// An operator that produces raster data.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum RasterOperator {
    Expression(Expression),
    GdalSource(GdalSource),
}

/// An operator that produces vector data.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum VectorOperator {
    MockPointSource(MockPointSource),
    RasterVectorJoin(RasterVectorJoin),
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
        match operator {
            RasterOperator::Expression(expression) => {
                OperatorsExpression::try_from(expression).map(OperatorsRasterOperator::boxed)
            }
            RasterOperator::GdalSource(gdal_source) => {
                OperatorsGdalSource::try_from(gdal_source).map(OperatorsRasterOperator::boxed)
            }
        }
    }
}

impl TryFrom<VectorOperator> for Box<dyn OperatorsVectorOperator> {
    type Error = anyhow::Error;
    fn try_from(operator: VectorOperator) -> Result<Self, Self::Error> {
        match operator {
            VectorOperator::MockPointSource(mock_point_source) => {
                OperatorsMockPointSource::try_from(mock_point_source)
                    .map(OperatorsVectorOperator::boxed)
            }
            VectorOperator::RasterVectorJoin(rvj) => {
                OperatorsRasterVectorJoin::try_from(rvj).map(OperatorsVectorOperator::boxed)
            }
        }
    }
}

impl TryFrom<PlotOperator> for Box<dyn OperatorsPlotOperator> {
    type Error = anyhow::Error;
    fn try_from(operator: PlotOperator) -> Result<Self, Self::Error> {
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
    // Processing
    Expression,
    ExpressionParameters,
    RasterOperator,
    RasterVectorJoin,
    RasterVectorJoinParameters,
    // Plots
    Histogram,
    HistogramParameters,
    Statistics,
    StatisticsParameters,
    // Source Parameters
    MultipleRasterOrSingleVectorOperator,
    MultipleRasterOrSingleVectorSource,
    SingleRasterOrVectorOperator,
    SingleRasterOrVectorSource,
    SingleRasterSource,
    SingleVectorMultipleRasterSources,

)))]
pub struct OperatorsApi;
