//! GEO BON EBV Portal catalog lookup service
//!
//! Connects to <https://portal.geobon.org/api/v1/>.

use super::tasks::TaskResponse;
use crate::contexts::AdminSession;
use crate::datasets::external::netcdfcf::{
    error, EbvPortalDataProvider, NetCdfCf4DProviderError, OverviewGeneration, EBV_PROVIDER_ID,
    NETCDF_CF_PROVIDER_ID,
};
use crate::error::Result;
use crate::layers::external::DataProvider;
use crate::layers::storage::LayerProviderDb;
use crate::tasks::{Task, TaskContext, TaskId, TaskManager, TaskStatus, TaskStatusInfo};
use crate::{contexts::Context, datasets::external::netcdfcf::NetCdfCfDataProvider};
use actix_web::{
    web::{self, ServiceConfig},
    FromRequest, Responder,
};
use futures::channel::oneshot;
use futures::lock::Mutex;
use geoengine_datatypes::error::{BoxedResultExt, ErrorSource};
use geoengine_datatypes::util::gdal::ResamplingMethod;
use log::debug;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

/// Initialize ebv routes
///
/// # Panics
/// This route initializer panics if `base_url` is `None` and the `ebv` config is not defined.
///
pub(crate) fn init_ebv_routes<C>() -> Box<dyn FnOnce(&mut ServiceConfig)>
where
    C: Context,
    C::Session: FromRequest,
{
    Box::new(move |cfg: &mut web::ServiceConfig| {
        cfg.service(
            web::resource("/create_overviews").route(web::post().to(create_overviews::<C>)),
        )
        .service(
            web::scope("/overviews")
                .route("/all", web::put().to(create_overviews::<C>))
                .service(
                    web::scope("/{path:[^{}]+}")
                        .route("", web::put().to(create_overview::<C>))
                        .route("", web::delete().to(remove_overview::<C>)),
                ),
        );
    })
}

/// returns the `EbvPortalDataProvider` if it is defined, otherwise the `NetCdfCfDataProvider` if it is defined.
/// Otherwise an erorr is returned.
async fn with_netcdfcf_provider<C: Context, T, F>(
    ctx: &C,
    _session: &C::Session,
    f: F,
) -> Result<T, NetCdfCf4DProviderError>
where
    T: Send + 'static,
    F: FnOnce(&NetCdfCfDataProvider) -> Result<T, NetCdfCf4DProviderError> + Send + 'static,
{
    let ebv_provider: Result<Box<dyn DataProvider>> = ctx
        .layer_provider_db_ref()
        .layer_provider(EBV_PROVIDER_ID)
        .await;

    let netcdf_provider = ctx
        .layer_provider_db_ref()
        .layer_provider(NETCDF_CF_PROVIDER_ID)
        .await;

    match (ebv_provider, netcdf_provider) {
        (Ok(ebv_provider), _) => crate::util::spawn_blocking(move || {
            let concrete_provider = ebv_provider
                .as_any()
                .downcast_ref::<EbvPortalDataProvider>()
                .ok_or(NetCdfCf4DProviderError::NoNetCdfCfProviderAvailable)?;

            f(&concrete_provider.netcdf_cf_provider)
        })
        .await
        .boxed_context(crate::datasets::external::netcdfcf::error::Internal)?,
        (Err(_), Ok(netcdf_provider)) => crate::util::spawn_blocking(move || {
            let concrete_provider = netcdf_provider
                .as_any()
                .downcast_ref::<NetCdfCfDataProvider>()
                .ok_or(NetCdfCf4DProviderError::NoNetCdfCfProviderAvailable)?;

            f(concrete_provider)
        })
        .await
        .boxed_context(crate::datasets::external::netcdfcf::error::Internal)?,
        _ => Err(NetCdfCf4DProviderError::NoNetCdfCfProviderAvailable),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct NetCdfCfOverviewResponse {
    success: Vec<PathBuf>,
    skip: Vec<PathBuf>,
    error: Vec<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateOverviewsParams {
    resampling_method: Option<ResamplingMethod>,
}

/// Create overviews for a all `NetCDF` files of the provider
async fn create_overviews<C: Context>(
    session: AdminSession,
    ctx: web::Data<C>,
    params: Option<web::Json<CreateOverviewsParams>>,
) -> Result<impl Responder> {
    let ctx = ctx.into_inner();

    let task: Box<dyn Task<C::TaskContext>> = EvbMultiOverviewTask::<C> {
        session,
        ctx: ctx.clone(),
        resampling_method: params.as_ref().and_then(|p| p.resampling_method),
        current_subtask_id: Arc::new(Mutex::new(None)),
    }
    .boxed();

    let task_id = ctx.tasks_ref().schedule(task, None).await?;

    Ok(web::Json(TaskResponse::new(task_id)))
}

struct EvbMultiOverviewTask<C: Context> {
    session: AdminSession,
    ctx: Arc<C>,
    resampling_method: Option<ResamplingMethod>,
    current_subtask_id: Arc<Mutex<Option<TaskId>>>,
}

impl<C: Context> EvbMultiOverviewTask<C> {
    fn update_pct(task_ctx: Arc<C::TaskContext>, pct: u8, status: NetCdfCfOverviewResponse) {
        crate::util::spawn(async move {
            task_ctx.set_completion(pct, status.boxed()).await;
        });
    }
}

#[async_trait::async_trait]
impl<C: Context> Task<C::TaskContext> for EvbMultiOverviewTask<C> {
    async fn run(
        &self,
        task_ctx: C::TaskContext,
    ) -> Result<Box<dyn crate::tasks::TaskStatusInfo>, Box<dyn ErrorSource>> {
        let task_ctx = Arc::new(task_ctx);
        let session = self.session.clone();
        let resampling_method = self.resampling_method;
        let current_subtask_id = self.current_subtask_id.clone();

        let files = with_netcdfcf_provider(
            self.ctx.as_ref(),
            &session.clone().into(),
            move |provider| {
                provider.list_files().map_err(|_| {
                    NetCdfCf4DProviderError::CdfCfProviderCannotListFiles {
                        id: NETCDF_CF_PROVIDER_ID,
                    }
                })
            },
        )
        .await
        .map_err(ErrorSource::boxed)?;
        let num_files = files.len();

        let mut status = NetCdfCfOverviewResponse {
            success: vec![],
            skip: vec![],
            error: vec![],
        };

        for (i, file) in files.into_iter().enumerate() {
            let subtask: Box<dyn Task<C::TaskContext>> = EvbOverviewTask::<C> {
                session: session.clone(),
                ctx: self.ctx.clone(),
                file: file.clone(),
                params: CreateOverviewParams { resampling_method },
            }
            .boxed();

            let (notification_tx, notification_rx) = oneshot::channel();

            let subtask_id = self
                .ctx
                .tasks_ref()
                .schedule(subtask, Some(notification_tx))
                .await
                .map_err(ErrorSource::boxed)?;

            current_subtask_id.lock().await.replace(subtask_id);

            if let Ok(subtask_status) = notification_rx.await {
                match subtask_status {
                    TaskStatus::Completed { info } => {
                        current_subtask_id.lock().await.take();

                        if let Ok(response) =
                            info.as_any_arc().downcast::<NetCdfCfOverviewResponse>()
                        {
                            status.success.extend(response.success.clone());
                            status.skip.extend(response.skip.clone());
                            status.error.extend(response.error.clone());
                        } else {
                            // must not happen, since we spawned a task that only returns a `NetCdfCfOverviewResponse`
                        }
                    }
                    TaskStatus::Aborted { .. } => {
                        debug!("Subtask aborted");

                        status.error.push(file);
                    }
                    TaskStatus::Failed { error, .. } => {
                        debug!("{:?}", error);

                        status.error.push(file);
                    }
                    TaskStatus::Running(_) => {
                        // must not happen, since we used the callback
                        debug!("Ran into task status that must not happend: running/aborted after finish");
                    }
                }
            } else {
                // TODO: can we ignore this?
            };

            // TODO: grab finished pct from subtasks
            Self::update_pct(
                task_ctx.clone(),
                ((i + 1) / num_files) as u8,
                status.clone(),
            );
        }

        Ok(status.boxed())
    }

    async fn cleanup_on_error(&self, _ctx: C::TaskContext) -> Result<(), Box<dyn ErrorSource>> {
        // Abort is propagated to current subtask
        // i.e. clean-up is performed by subtasks themselves

        Ok(())
    }

    fn task_type(&self) -> &'static str {
        "evb-multi-overview"
    }

    async fn subtasks(&self) -> Vec<TaskId> {
        self.current_subtask_id
            .lock()
            .await
            .as_ref()
            .map(|id| vec![*id])
            .unwrap_or_default()
    }
}

#[derive(Debug, Deserialize, Default)]
struct CreateOverviewParams {
    resampling_method: Option<ResamplingMethod>,
}

/// Creates overview for a single `NetCDF` file
async fn create_overview<C: Context>(
    session: AdminSession,
    ctx: web::Data<C>,
    path: web::Path<PathBuf>,
    params: Option<web::Json<CreateOverviewParams>>,
) -> Result<impl Responder> {
    let ctx = ctx.into_inner();

    let task: Box<dyn Task<C::TaskContext>> = EvbOverviewTask::<C> {
        session,
        ctx: ctx.clone(),
        file: path.into_inner(),
        params: params.map(web::Json::into_inner).unwrap_or_default(),
    }
    .boxed();

    let task_id = ctx.tasks_ref().schedule(task, None).await?;

    Ok(web::Json(TaskResponse::new(task_id)))
}

struct EvbOverviewTask<C: Context> {
    session: AdminSession,
    ctx: Arc<C>,
    file: PathBuf,
    params: CreateOverviewParams,
}

#[async_trait::async_trait]
impl<C: Context> Task<C::TaskContext> for EvbOverviewTask<C> {
    async fn run(
        &self,
        _ctx: C::TaskContext,
    ) -> Result<Box<dyn crate::tasks::TaskStatusInfo>, Box<dyn ErrorSource>> {
        let file = self.file.clone();
        let session = self.session.clone();
        let resampling_method = self.params.resampling_method;

        let response =
            with_netcdfcf_provider(self.ctx.as_ref(), &session.into(), move |provider| {
                // TODO: provide some detailed pct status

                match provider.create_overviews(&file, resampling_method) {
                    Ok(OverviewGeneration::Created) => Ok(NetCdfCfOverviewResponse {
                        success: vec![file],
                        skip: vec![],
                        error: vec![],
                    }),
                    Ok(OverviewGeneration::Skipped) => Ok(NetCdfCfOverviewResponse {
                        success: vec![],
                        skip: vec![file],
                        error: vec![],
                    }),
                    Err(e) => Err(NetCdfCf4DProviderError::CannotCreateOverview {
                        dataset: file,
                        source: Box::new(e),
                    }),
                }
            })
            .await;

        response
            .map(TaskStatusInfo::boxed)
            .map_err(ErrorSource::boxed)
    }

    async fn cleanup_on_error(&self, _ctx: C::TaskContext) -> Result<(), Box<dyn ErrorSource>> {
        let file = self.file.clone();
        let session = self.session.clone();

        let response =
            with_netcdfcf_provider(self.ctx.as_ref(), &session.into(), move |provider| {
                provider
                    .remove_overviews(&file, false)
                    .boxed_context(error::CannotRemoveOverviews)
            })
            .await;

        response.map_err(ErrorSource::boxed)
    }

    fn task_type(&self) -> &'static str {
        "evb-overview"
    }

    fn task_unique_id(&self) -> Option<String> {
        Some(self.file.to_string_lossy().to_string())
    }
}

impl TaskStatusInfo for NetCdfCfOverviewResponse {}

#[derive(Debug, Deserialize, Default)]
struct RemoveOverviewParams {
    #[serde(default)]
    force: bool,
}

/// Removes an overview for a single `NetCDF` file
async fn remove_overview<C: Context>(
    session: AdminSession,
    ctx: web::Data<C>,
    path: web::Path<PathBuf>,
    params: web::Query<RemoveOverviewParams>,
) -> Result<impl Responder> {
    let ctx = ctx.into_inner();

    let task: Box<dyn Task<C::TaskContext>> = EvbRemoveOverviewTask::<C> {
        session,
        ctx: ctx.clone(),
        file: path.into_inner(),
        params: params.into_inner(),
    }
    .boxed();

    let task_id = ctx.tasks_ref().schedule(task, None).await?;

    Ok(web::Json(TaskResponse::new(task_id)))
}

struct EvbRemoveOverviewTask<C: Context> {
    session: AdminSession,
    ctx: Arc<C>,
    file: PathBuf,
    params: RemoveOverviewParams,
}

#[async_trait::async_trait]
impl<C: Context> Task<C::TaskContext> for EvbRemoveOverviewTask<C> {
    async fn run(
        &self,
        _ctx: C::TaskContext,
    ) -> Result<Box<dyn crate::tasks::TaskStatusInfo>, Box<dyn ErrorSource>> {
        let file = self.file.clone();
        let session = self.session.clone();
        let force = self.params.force;

        let response =
            with_netcdfcf_provider(self.ctx.as_ref(), &session.into(), move |provider| {
                provider
                    .remove_overviews(&file, force)
                    .boxed_context(error::CannotRemoveOverviews)
            })
            .await;

        response
            .map(TaskStatusInfo::boxed)
            .map_err(ErrorSource::boxed)
    }

    async fn cleanup_on_error(&self, _ctx: C::TaskContext) -> Result<(), Box<dyn ErrorSource>> {
        Ok(())
    }

    fn task_type(&self) -> &'static str {
        "evb-remove-overview"
    }

    fn task_unique_id(&self) -> Option<String> {
        Some(self.file.to_string_lossy().to_string())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::{
        contexts::{InMemoryContext, Session, SimpleContext},
        datasets::external::netcdfcf::NetCdfCfDataProviderDefinition,
        server::{configure_extractors, render_404, render_405},
        tasks::util::test::wait_for_task_to_finish,
        util::tests::read_body_string,
    };
    use actix_web::{dev::ServiceResponse, http, http::header, middleware, test, web, App};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::{test_data, util::test::TestDefault};
    use std::path::Path;

    async fn send_test_request<C: SimpleContext>(
        req: test::TestRequest,
        ctx: C,
    ) -> ServiceResponse {
        let app = test::init_service({
            let app = App::new()
                .app_data(web::Data::new(ctx))
                .wrap(
                    middleware::ErrorHandlers::default()
                        .handler(http::StatusCode::NOT_FOUND, render_404)
                        .handler(http::StatusCode::METHOD_NOT_ALLOWED, render_405),
                )
                .wrap(middleware::NormalizePath::trim())
                .configure(configure_extractors)
                .service(web::scope("/ebv").configure(init_ebv_routes::<C>()));

            app
        })
        .await;
        test::call_service(&app, req.to_request())
            .await
            .map_into_boxed_body()
    }

    #[tokio::test]
    async fn test_remove_overviews() {
        fn is_empty(directory: &Path) -> bool {
            directory.read_dir().unwrap().next().is_none()
        }

        crate::util::config::set_config(
            "session.admin_session_token",
            "8aca8875-425a-4ef1-8ee6-cdfc62dd7525",
        )
        .unwrap();

        let ctx = InMemoryContext::test_default();
        let admin_session_id = AdminSession::default().id();

        let overview_folder = tempfile::tempdir().unwrap();

        ctx.layer_provider_db_ref()
            .add_layer_provider(Box::new(NetCdfCfDataProviderDefinition {
                name: "test".to_string(),
                path: test_data!("netcdf4d").to_path_buf(),
                overviews: overview_folder.path().to_path_buf(),
            }))
            .await
            .unwrap();

        let req = actix_web::test::TestRequest::put()
            .uri("/ebv/overviews/dataset_m.nc")
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session_id.to_string()),
            ));

        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());

        let task_response =
            serde_json::from_str::<TaskResponse>(&read_body_string(res).await).unwrap();

        wait_for_task_to_finish(ctx.tasks(), task_response.task_id).await;

        let status = ctx.tasks().status(task_response.task_id).await.unwrap();

        let mut response = if let TaskStatus::Completed { info } = status {
            info.as_any_arc()
                .downcast::<NetCdfCfOverviewResponse>()
                .unwrap()
                .as_ref()
                .clone()
        } else {
            panic!("Task must be completed");
        };

        response.success.sort();
        assert_eq!(
            response,
            NetCdfCfOverviewResponse {
                success: vec!["dataset_m.nc".into()],
                skip: vec![],
                error: vec![],
            }
        );

        // Now, delete the overviews

        let req = actix_web::test::TestRequest::delete()
            .uri("/ebv/overviews/dataset_m.nc")
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session_id.to_string()),
            ));

        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());

        let task_response =
            serde_json::from_str::<TaskResponse>(&read_body_string(res).await).unwrap();

        wait_for_task_to_finish(ctx.tasks(), task_response.task_id).await;

        let status = ctx.tasks().status(task_response.task_id).await.unwrap();

        assert_eq!(
            serde_json::to_value(status).unwrap(),
            serde_json::json!({
                "status": "completed",
                "info": null,
            })
        );

        assert!(is_empty(overview_folder.path()));
    }

    #[tokio::test]
    async fn test_remove_overviews_non_existing() {
        // setup

        crate::util::config::set_config(
            "session.admin_session_token",
            "8aca8875-425a-4ef1-8ee6-cdfc62dd7525",
        )
        .unwrap();

        let ctx = InMemoryContext::test_default();
        let admin_session_id = AdminSession::default().id();

        let overview_folder = tempfile::tempdir().unwrap();

        ctx.layer_provider_db_ref()
            .add_layer_provider(Box::new(NetCdfCfDataProviderDefinition {
                name: "test".to_string(),
                path: test_data!("netcdf4d").to_path_buf(),
                overviews: overview_folder.path().to_path_buf(),
            }))
            .await
            .unwrap();

        // remove overviews that don't exist

        let req = actix_web::test::TestRequest::delete()
            .uri("/ebv/overviews/path/to/dataset.nc?force=true")
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session_id.to_string()),
            ));

        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());

        let task_response =
            serde_json::from_str::<TaskResponse>(&read_body_string(res).await).unwrap();

        wait_for_task_to_finish(ctx.tasks(), task_response.task_id).await;

        let status = ctx.tasks().status(task_response.task_id).await.unwrap();

        assert_eq!(
            serde_json::to_value(status).unwrap(),
            serde_json::json!({
                "status": "completed",
                "info": null,
            })
        );
    }

    #[tokio::test]
    async fn test_create_non_existing_overview() {
        fn is_empty(directory: &Path) -> bool {
            directory.read_dir().unwrap().next().is_none()
        }

        crate::util::config::set_config(
            "session.admin_session_token",
            "8aca8875-425a-4ef1-8ee6-cdfc62dd7525",
        )
        .unwrap();

        let ctx = InMemoryContext::test_default();
        let admin_session_id = AdminSession::default().id();

        let overview_folder = tempfile::tempdir().unwrap();

        ctx.layer_provider_db_ref()
            .add_layer_provider(Box::new(NetCdfCfDataProviderDefinition {
                name: "test".to_string(),
                path: test_data!("netcdf4d").to_path_buf(),
                overviews: overview_folder.path().to_path_buf(),
            }))
            .await
            .unwrap();

        let req = actix_web::test::TestRequest::put()
            .uri("/ebv/overviews/foo/bar.nc")
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session_id.to_string()),
            ));

        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());

        let task_response =
            serde_json::from_str::<TaskResponse>(&read_body_string(res).await).unwrap();

        wait_for_task_to_finish(ctx.tasks(), task_response.task_id).await;

        let status = ctx.tasks().status(task_response.task_id).await.unwrap();

        let (error, clean_up) = if let TaskStatus::Failed { error, clean_up } = status {
            (error, serde_json::to_string(&clean_up).unwrap())
        } else {
            panic!("Task must be failed");
        };

        assert!(matches!(
            error.into_any_arc().downcast::<NetCdfCf4DProviderError>().unwrap().as_ref(),
            NetCdfCf4DProviderError::CannotCreateOverview { dataset, source }
            if dataset.to_string_lossy() == "foo/bar.nc" &&
            // TODO: use matches clause `NetCdfCf4DProviderError::CannotOpenNetCdfDataset { .. }`
            source.to_string().contains("CannotOpenNetCdfDataset")
        ));

        assert_eq!(clean_up, r#"{"status":"completed","info":null}"#);

        assert!(is_empty(overview_folder.path()));
    }
}
