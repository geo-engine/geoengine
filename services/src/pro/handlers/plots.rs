use actix_web::{web, FromRequest, Responder};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::handlers::plots::GetPlot;
use crate::pro::contexts::ProContext;
use crate::workflows::workflow::WorkflowId;

pub(crate) fn init_plot_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProContext,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/plot/{id}").route(web::get().to(get_plot_handler::<C>)));
}

/// Generates a [plot](WrappedPlotOutput).
///
/// # Example
///
/// 1. Create a statistics workflow.
///
/// ```text
/// POST /workflow
/// Authorization: Bearer 4f0d02f9-68e8-46fb-9362-80f862b7db54
///
/// {
///   "type": "Plot",
///   "operator": {
///     "type": "Statistics",
///     "params": {},
///     "rasterSources": [
///       {
///         "type": "MockRasterSource",
///         "params": {
///           "data": [
///             {
///               "time": {
///                 "start": -8334632851200000,
///                 "end": 8210298412799999
///               },
///               "tilePosition": [0, 0],
///               "globalGeoTransform": {
///                 "originCoordinate": { "x": 0.0, "y": 0.0 },
///                 "xPixelSize": 1.0,
///                 "yPixelSize": -1.0
///               },
///               "gridArray": {
///                 "shape": {
///                   "shapeArray": [3, 2]
///                 },
///                 "data": [1, 2, 3, 4, 5, 6]
///               }
///             }
///           ],
///           "resultDescriptor": {
///             "dataType": "U8",
///             "spatialReference": "EPSG:4326",
///             "measurement": "unitless"
///           }
///         }
///       }
///     ],
///     "vectorSources": []
///   }
/// }
/// ```
/// Response:
/// ```text
/// {
///   "id": "504ed8a4-e0a4-5cef-9f91-b2ffd4a2b56b"
/// }
/// ```
///
/// 2. Generate the plot.
/// ```text
/// GET /plot/504ed8a4-e0a4-5cef-9f91-b2ffd4a2b56b?bbox=-180,-90,180,90&crs=EPSG:4326&time=2020-01-01T00%3A00%3A00.0Z&spatialResolution=0.1,0.1
/// Authorization: Bearer 4f0d02f9-68e8-46fb-9362-80f862b7db54
/// ```
/// Response:
/// ```text
/// {
///   "outputFormat": "JsonPlain",
///   "plotType": "Statistics",
///   "data": [
///     {
///       "pixelCount": 6,
///       "nanCount": 0,
///       "min": 1.0,
///       "max": 6.0,
///       "mean": 3.5,
///       "stddev": 1.707825127659933
///     }
///   ]
/// }
/// ```
async fn get_plot_handler<C: ProContext>(
    id: web::Path<Uuid>,
    params: web::Query<GetPlot>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let workflow_id = WorkflowId(id.as_ref().clone());
    let task_manager = ctx.task_manager();

    let task = crate::handlers::plots::process_plot_request(id, params, session, ctx);

    let result = task_manager
        .plot_executor()
        .submit_ref(&workflow_id, task)
        .await?;

    log::debug!("Received result from task manager.");

    match result.as_ref() {
        Ok(v) => Ok(web::Json(v.clone())),
        Err(_) => Err(Error::NotYetImplemented),
    }
}
