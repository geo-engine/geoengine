use actix_web::{web, FromRequest, Responder};
use uuid::Uuid;

use crate::error::Result;
use crate::handlers::plots::GetPlot;
use crate::pro::contexts::ProContext;
use crate::pro::executor::PlotDescription;
use crate::workflows::workflow::WorkflowId;

pub(crate) fn init_plot_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProContext,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/plot/{id}").route(web::get().to(get_plot_handler::<C>)));
}

/// Generates a [plot](crate::handlers::plots::WrappedPlotOutput).
/// This handler behaves the same as the standard [plot handler](crate::handlers::plots::get_plot_handler),
/// except that it uses an [executor](crate::pro::contexts::TaskManager) for query execution.
async fn get_plot_handler<C: ProContext>(
    id: web::Path<Uuid>,
    params: web::Query<GetPlot>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let workflow_id = WorkflowId(*id.as_ref());
    let task_manager = ctx.task_manager();

    let desc = PlotDescription {
        id: workflow_id,
        spatial_bounds: params.bbox,
        temporal_bounds: params.time,
    };

    let task = crate::handlers::plots::process_plot_request(id, params, session, ctx);

    let result = task_manager.plot_executor().submit(desc, task).await?;

    Ok(web::Json(result?))
}
