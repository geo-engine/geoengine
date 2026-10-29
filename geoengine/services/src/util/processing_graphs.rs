use crate::{error::Result, processing_graphs::processing_graph::ProcessingGraph};
use geoengine_operators::engine::{ExecutionContext, ProcessingGraphOperatorPath, TypedOperator};

/// ensure the processing graph is valid by initializing it
pub async fn validate_processing_graph<E: ExecutionContext>(
    processing_graph: &ProcessingGraph,
    execution_context: &E,
) -> Result<()> {
    let processing_graph_operator_path_root = ProcessingGraphOperatorPath::initialize_root();

    match processing_graph.clone().operator {
        TypedOperator::Vector(o) => {
            o.initialize(processing_graph_operator_path_root, execution_context)
                .await?;
        }
        TypedOperator::Raster(o) => {
            o.initialize(processing_graph_operator_path_root, execution_context)
                .await?;
        }
        TypedOperator::Plot(o) => {
            o.initialize(processing_graph_operator_path_root, execution_context)
                .await?;
        }
    }

    Ok(())
}
