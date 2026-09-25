use crate::{error::Result, workflows::workflow::Workflow};
use geoengine_operators::engine::{ExecutionContext, TypedOperator, WorkflowOperatorPath};

/// ensure the workflow is valid by initializing it
pub async fn validate_workflow<E: ExecutionContext>(
    workflow: &Workflow,
    execution_context: &E,
) -> Result<()> {
    let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

    match workflow.clone().operator {
        TypedOperator::Vector(o) => {
            o.initialize(workflow_operator_path_root, execution_context)
                .await?;
        }
        TypedOperator::Raster(o) => {
            o.initialize(workflow_operator_path_root, execution_context)
                .await?;
        }
        TypedOperator::Plot(o) => {
            o.initialize(workflow_operator_path_root, execution_context)
                .await?;
        }
    }

    Ok(())
}
