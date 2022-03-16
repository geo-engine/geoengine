use crate::error::Result;
use crate::error::{self, Error};
use crate::ogc::wms::request::{GetMap, WmsRequest};
use crate::util::user_input::QueryEx;
use crate::workflows::workflow::WorkflowId;
use actix_web::{web, FromRequest, HttpResponse};
use std::sync::Arc;

use crate::pro::contexts::{GetRasterScheduler, ProContext};
use crate::pro::executor::RasterTaskDescription;
use geoengine_operators::call_on_generic_raster_processor;
use geoengine_operators::engine::RasterQueryProcessor;
use geoengine_operators::util::raster_stream_to_png::raster_stream_to_png_bytes_stream;
use num_traits::AsPrimitive;

pub(crate) fn init_wms_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProContext,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/wms/{workflow}").route(web::get().to(wms_handler::<C>)));
}

async fn wms_handler<C: ProContext>(
    workflow: web::Path<WorkflowId>,
    request: QueryEx<WmsRequest>,
    ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse> {
    match request.into_inner() {
        WmsRequest::GetCapabilities(request) => {
            let external_address =
                crate::util::config::get_config_element::<crate::util::config::Web>()?
                    .external_address
                    .ok_or(Error::ExternalAddressNotConfigured)?;
            crate::handlers::wms::get_capabilities(
                &request,
                &external_address,
                ctx.get_ref(),
                session,
                workflow.into_inner(),
            )
            .await
        }
        WmsRequest::GetMap(request) => {
            get_map(&request, ctx.get_ref(), session, workflow.into_inner()).await
        }
        WmsRequest::GetLegendGraphic(request) => {
            crate::handlers::wms::get_legend_graphic(&request, ctx.get_ref(), workflow.into_inner())
        }
        _ => Ok(HttpResponse::NotImplemented().finish()),
    }
}

/// Renders a map as raster image.
///
/// # Example
///
/// ```text
/// GET /wms/df756642-c5a3-4d72-8ad7-629d312ae993?request=GetMap&service=WMS&version=2.0.0&layers=df756642-c5a3-4d72-8ad7-629d312ae993&bbox=1,2,3,4&width=100&height=100&crs=EPSG%3A4326&styles=ssss&format=image%2Fpng
/// ```
/// Response:
/// PNG image
async fn get_map<C: ProContext>(
    request: &GetMap,
    ctx: &C,
    session: C::Session,
    endpoint: WorkflowId,
) -> Result<HttpResponse> {
    let (processor, query_rect, colorizer, no_data_value) =
        crate::handlers::wms::extract_operator_and_bounding_box_and_colorizer(
            request, ctx, session, endpoint,
        )
        .await?;

    let query_ctx = Arc::new(ctx.query_context()?);

    let image_bytes = call_on_generic_raster_processor!(
        processor,
        p => {
            let proc: Arc<dyn RasterQueryProcessor<RasterType = _>> = p.into();

            let desc = RasterTaskDescription::new(
                endpoint,
                query_rect,
                proc.clone(),
                query_ctx.clone(),
            );
            let stream = ctx.task_manager().get_raster_scheduler().submit_stream(desc).await?;
            raster_stream_to_png_bytes_stream(Box::pin(stream), query_rect, request.width, request.height, request.time, colorizer, no_data_value.map(AsPrimitive::as_)).await
        }
    ).map_err(error::Error::from)?;

    Ok(HttpResponse::Ok()
        .content_type(mime::IMAGE_PNG)
        .body(image_bytes))
}
