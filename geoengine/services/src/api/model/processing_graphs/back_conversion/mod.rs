pub mod plot;
pub mod raster;
pub mod vector;

use crate::{
    api::model::processing_graphs::TypedOperator as ApiTypedOperator, workflows::workflow::Workflow,
};
use geoengine_datatypes::util::AsAny;
use geoengine_operators::engine::TypedOperator as OperatorsTypedOperator;

/// Converts a [`Workflow`] into a `ProcessingGraph` represented by an `ApiTypedOperator`.
pub fn workflow_to_processing_graph(workflow: &Workflow) -> anyhow::Result<ApiTypedOperator> {
    match &workflow.operator {
        OperatorsTypedOperator::Raster(raster_operator) => Ok(ApiTypedOperator::Raster(
            raster::raster_operator_from_runtime(raster_operator.as_ref())?,
        )),
        OperatorsTypedOperator::Vector(vector_operator) => Ok(ApiTypedOperator::Vector(
            vector::vector_operator_from_runtime(vector_operator.as_ref())?,
        )),
        OperatorsTypedOperator::Plot(plot_operator) => Ok(ApiTypedOperator::Plot(
            plot::plot_operator_from_runtime(plot_operator.as_ref())?,
        )),
    }
}

/// Downcasts a `&dyn` runtime operator to a specific type based on `type_tag`.
fn downcast_runtime_operator<'a, T: 'static>(
    operator_type: &'static str,
    operator: &'a dyn AsAny,
    type_name: &'a str,
) -> anyhow::Result<&'a T> {
    operator.as_any().downcast_ref::<T>().ok_or_else(|| {
        anyhow::anyhow!(
            "cannot convert runtime {operator_type} operator {type_name} to ProcessingGraph"
        )
    })
}
