//! Plots of a plot workflow.

use crate::{
    api::handlers::plots::{GetPlot, WrappedPlotOutput},
    contexts::SessionContext,
    error::{self, Result},
    quota::ComputationId,
    workflows::{registry::WorkflowRegistry, workflow::WorkflowId},
};
use base64::Engine;
use geoengine_datatypes::{
    operations::reproject::{reproject_spatial_query, suggest_pixel_size_like_gdal_helper},
    plots::PlotOutputFormat,
    primitives::{BoundingBox2D, PlotQueryRectangle, PlotSeriesSelection, SpatialResolution},
    spatial_reference::{DefaultCoordinateProjector, SpatialReference},
    util::Identifier,
};
use geoengine_operators::engine::{
    ExecutionContext, InitializedPlotOperator, PlotOperator, ResultDescriptor,
    TypedPlotQueryProcessor, WorkflowOperatorPath,
};
use snafu::ResultExt;
use tracing::debug;

/// Computes the plot of a workflow
pub(crate) async fn plot<C: SessionContext>(
    ctx: C,
    workflow_id: WorkflowId,
    params: GetPlot,
) -> Result<(WrappedPlotOutput, ComputationId)> {
    let workflow = ctx.db().load_workflow(&workflow_id).await?;

    let operator = workflow.operator.get_plot()?;

    let execution_context = ctx.execution_context()?;

    let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

    let initialized = operator
        .initialize(workflow_operator_path_root, &execution_context)
        .await?;

    // handle request and workflow crs matching
    let workflow_spatial_ref: Option<SpatialReference> =
        initialized.result_descriptor().spatial_reference().into();
    let workflow_spatial_ref = workflow_spatial_ref.ok_or(error::Error::InvalidSpatialReference)?;

    // TODO: use a default spatial reference if it is not set?
    let request_spatial_ref: SpatialReference =
        params.crs.ok_or(error::Error::MissingSpatialReference)?;

    let initialized = if let Some(request_resolution) = params.spatial_resolution {
        optimize_for_request_resolution(
            initialized,
            &execution_context,
            params.bbox,
            request_resolution,
            request_spatial_ref,
            workflow_spatial_ref,
        )
        .await?
    } else {
        initialized
    };

    let query_rect =
        PlotQueryRectangle::new(params.bbox, params.time.into(), PlotSeriesSelection::all());

    let query_rect = if request_spatial_ref == workflow_spatial_ref {
        Some(query_rect)
    } else {
        let repr_spatial_query = reproject_spatial_query::<_, DefaultCoordinateProjector>(
            query_rect.spatial_bounds(),
            workflow_spatial_ref,
            request_spatial_ref,
            true,
        )?;
        repr_spatial_query.map(|r| {
            PlotQueryRectangle::new(r, query_rect.time_interval(), *query_rect.attributes())
        })
    };

    let Some(query_rect) = query_rect else {
        return Err(error::Error::UnresolvableQueryBoundingBox2DInSrs {
            query_bbox: params.bbox.into(),
            query_srs: workflow_spatial_ref.into(),
        });
    };

    let processor = initialized.query_processor()?;

    let computation_id = ComputationId::new();

    let query_ctx = ctx.query_context(workflow_id, computation_id)?;

    let output_format = PlotOutputFormat::from(&processor);
    let plot_type = processor.plot_type();

    let data = match processor {
        TypedPlotQueryProcessor::JsonPlain(processor) => {
            processor.plot_query(query_rect, &query_ctx).await?
        }
        TypedPlotQueryProcessor::JsonVega(processor) => {
            let chart = processor.plot_query(query_rect, &query_ctx).await?;

            serde_json::to_value(chart).context(error::SerdeJson)?
        }
        TypedPlotQueryProcessor::ImagePng(processor) => {
            let png_bytes = processor.plot_query(query_rect, &query_ctx).await?;

            let data_uri = format!(
                "data:image/png;base64,{}",
                base64::engine::general_purpose::STANDARD.encode(png_bytes)
            );

            serde_json::to_value(data_uri).context(error::SerdeJson)?
        }
    };

    let output = WrappedPlotOutput {
        output_format,
        plot_type,
        data,
    };

    Ok((output, computation_id))
}

/// Optimizes the plot for the `request_resolution` (in the `request_spatial_ref`), so that raster
/// inputs are read at a coarser overview level.
///
/// Falls back to the unoptimized operator if the processing graph cannot be optimized,
/// e.g., because a source explicitly loads an overview level.
async fn optimize_for_request_resolution(
    initialized: Box<dyn InitializedPlotOperator>,
    execution_context: &dyn ExecutionContext,
    request_bbox: BoundingBox2D,
    request_resolution: SpatialResolution,
    request_spatial_ref: SpatialReference,
    workflow_spatial_ref: SpatialReference,
) -> Result<Box<dyn InitializedPlotOperator>> {
    let resolution = if request_spatial_ref == workflow_spatial_ref {
        Ok(request_resolution)
    } else {
        suggest_pixel_size_like_gdal_helper::<_, DefaultCoordinateProjector>(
            request_bbox,
            request_resolution,
            request_spatial_ref,
            workflow_spatial_ref,
        )
    };

    let optimized: Result<Box<dyn PlotOperator>, String> = match resolution {
        Ok(resolution) => initialized
            .optimize(resolution)
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    };

    match optimized {
        Ok(optimized) => Ok(optimized
            .initialize(WorkflowOperatorPath::initialize_root(), execution_context)
            .await?),
        Err(error) => {
            debug!("Computing plot without optimization: {error}");
            Ok(initialized)
        }
    }
}
