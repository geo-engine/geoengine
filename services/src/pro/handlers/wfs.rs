use crate::error::Result;
use crate::ogc::wfs::request::{GetFeature, WfsRequest};
use crate::pro::contexts::{GetFeatureScheduler, ProContext};
use crate::pro::executor::FeatureCollectionTaskDescription;
use crate::util::user_input::QueryEx;
use crate::workflows::workflow::WorkflowId;
use actix_web::{web, FromRequest, HttpResponse};
use geoengine_operators::call_on_generic_vector_processor;

pub(crate) fn init_wfs_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProContext,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/wfs/{workflow}").route(web::get().to(wfs_handler::<C>)));
}

async fn wfs_handler<C: ProContext>(
    workflow: web::Path<WorkflowId>,
    request: QueryEx<WfsRequest>,
    ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse> {
    match request.into_inner() {
        WfsRequest::GetCapabilities(request) => {
            crate::handlers::wfs::get_capabilities(
                &request,
                ctx.get_ref(),
                session,
                workflow.into_inner(),
            )
            .await
        }
        WfsRequest::GetFeature(request) => {
            get_feature(&request, ctx.get_ref(), session, workflow.into_inner()).await
        }
        _ => Ok(HttpResponse::NotImplemented().finish()),
    }
}

/// Retrieves feature data objects. See [wfs handler](`crate::handlers::wfs::get_feature`).
async fn get_feature<C: ProContext>(
    request: &GetFeature,
    ctx: &C,
    session: C::Session,
    endpoint: WorkflowId,
) -> Result<HttpResponse> {
    let (processor, query_rect) =
        crate::handlers::wfs::extract_operator_and_bounding_box(request, ctx, session, endpoint)
            .await?;

    let query_ctx = ctx.query_context()?;

    let tm = ctx.task_manager();
    let json = call_on_generic_vector_processor!(processor, p => {
        let desc = FeatureCollectionTaskDescription::new(endpoint,
                query_rect, p, query_ctx);
        let stream = tm.get_feature_scheduler().schedule_stream(desc).await?;
        crate::handlers::wfs::vector_stream_to_geojson(Box::pin(stream)).await
    })?;
    Ok(HttpResponse::Ok().json(json))
}
