pub mod plot;
pub mod raster;
pub mod vector;

use crate::{
    api::model::processing_graphs::TypedOperator as ApiTypedOperator, workflows::workflow::Workflow,
};
use geoengine_operators::engine::TypedOperator as OperatorsTypedOperator;

/// Converts a [`Workflow`] into a `ProcessingGraph` represented by an `ApiTypedOperator`.
pub fn workflow_to_processing_graph(workflow: &Workflow) -> anyhow::Result<ApiTypedOperator> {
    match &workflow.operator {
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
