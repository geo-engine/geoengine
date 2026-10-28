pub mod plot;
pub mod raster;
pub mod vector;

use crate::{
    api::model::processing_graphs::TypedOperator as ApiTypedOperator,
    processing_graphs::processing_graph::ProcessingGraph as ServicesProcessingGraph,
};
use geoengine_operators::engine::TypedOperator as OperatorsTypedOperator;

/// Converts a [`ServicesProcessingGraph`] into an API processing graph represented by an `ApiTypedOperator`.
pub fn services_processing_graph_to_api(
    processing_graph: &ServicesProcessingGraph,
) -> anyhow::Result<ApiTypedOperator> {
    match &processing_graph.operator {
        OperatorsTypedOperator::Raster(raster_operator) => Ok(ApiTypedOperator::Raster(
            raster::raster_operator_from_runtime(raster_operator)?,
        )),
        OperatorsTypedOperator::Vector(vector_operator) => Ok(ApiTypedOperator::Vector(
            vector::vector_operator_from_runtime(vector_operator)?,
        )),
        OperatorsTypedOperator::Plot(plot_operator) => Ok(ApiTypedOperator::Plot(
            plot::plot_operator_from_runtime(plot_operator)?,
        )),
    }
}
