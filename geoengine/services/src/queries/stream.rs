//! Websocket streams: raster tiles and vector chunks as Arrow IPC files.

use crate::{
    api::handlers::processing_graphs::error,
    contexts::SessionContext,
    error::Result,
    quota::ComputationId,
    workflows::{WebsocketStreamTask, registry::WorkflowRegistry, workflow::WorkflowId},
};
use geoengine_datatypes::{
    error::BoxedResultExt,
    primitives::{
        BandSelection, RasterQueryRectangle, SpatialPartition2D, TimeInterval, VectorQueryRectangle,
    },
    util::Identifier,
};
use geoengine_operators::engine::{ExecutionContext, WorkflowOperatorPath};

/// Initializes a raster workflow and starts streaming the tiles within `spatial_bounds`
pub async fn raster_stream<C: SessionContext>(
    ctx: C,
    workflow_id: WorkflowId,
    spatial_bounds: SpatialPartition2D,
    time_interval: TimeInterval,
    bands: BandSelection,
) -> Result<WebsocketStreamTask> {
    let operator = ctx
        .db()
        .load_workflow(&workflow_id)
        .await?
        .operator
        .get_raster()
        .boxed_context(error::ProcessingGraphMustBeOfTypeRaster)?;

    let execution_context = ctx.execution_context()?;

    let initialized_operator = operator
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await?;

    let query_bounds = initialized_operator
        .result_descriptor()
        .tiling_grid_definition(execution_context.tiling_specification())
        .tiling_geo_transform()
        .spatial_to_grid_bounds(&spatial_bounds);
    let query_rectangle = RasterQueryRectangle::new(query_bounds, time_interval, bands);

    let query_ctx = ctx.query_context(workflow_id, ComputationId::new())?;

    WebsocketStreamTask::new_raster_initialized(
        initialized_operator.as_ref(),
        query_rectangle,
        query_ctx,
    )
}

/// Initializes a vector workflow and starts streaming its chunks
pub async fn vector_stream<C: SessionContext>(
    ctx: C,
    workflow_id: WorkflowId,
    query_rectangle: VectorQueryRectangle,
) -> Result<WebsocketStreamTask> {
    let operator = ctx
        .db()
        .load_workflow(&workflow_id)
        .await?
        .operator
        .get_vector()
        .boxed_context(error::ProcessingGraphMustBeOfTypeVector)?;

    WebsocketStreamTask::new_vector::<C>(
        operator,
        query_rectangle,
        ctx.execution_context()?,
        ctx.query_context(workflow_id, ComputationId::new())?,
    )
    .await
}
