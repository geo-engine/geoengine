//! GEO BON EBV Portal catalog lookup service
//!
//! Connects to <https://portal.geobon.org/api/v1/>.

use super::tasks::TaskResponse;
use crate::api::model::datatypes::ResamplingMethod;
use crate::contexts::ApplicationContext;
use crate::datasets::external::netcdfcf::{
    error, EbvPortalDataProvider, NetCdfCf4DProviderError, OverviewGeneration, EBV_PROVIDER_ID,
    NETCDF_CF_PROVIDER_ID,
};
use crate::error::Result;
use crate::layers::external::DataProvider;
use crate::layers::storage::LayerProviderDb;
use crate::tasks::{Task, TaskContext, TaskId, TaskManager, TaskStatus, TaskStatusInfo};
use crate::util::apidoc::OpenApiServerInfo;
use crate::{contexts::SessionContext, datasets::external::netcdfcf::NetCdfCfDataProvider};
use actix_web::{
    web::{self, ServiceConfig},
    FromRequest, Responder,
};
use futures::channel::oneshot;
use futures::lock::Mutex;
use geoengine_datatypes::error::{BoxedResultExt, ErrorSource};
use log::debug;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{IntoParams, Modify, OpenApi, ToSchema};

pub const EBV_OVERVIEW_TASK_TYPE: &str = "ebv-overview";
pub const EBV_MULTI_OVERVIEW_TASK_TYPE: &str = "ebv-multi-overview";
pub const EBV_REMOVE_OVERVIEW_TASK_TYPE: &str = "ebv-remove-overview";

#[derive(OpenApi)]
#[openapi(
    paths(
        create_overviews,
        create_overview,
        remove_overview
    ),
    components(
        schemas(
            TaskId,
            CreateOverviewsParams,
            CreateOverviewParams,
            RemoveOverviewParams,
            TaskResponse,
            ResamplingMethod
        ),
    ),
    modifiers(&SecurityAddon, &ApiDocInfo, &OpenApiServerInfo)
)]
pub struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.as_mut().unwrap();
        components.add_security_scheme(
            "admin_token",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("UUID")
                    .description(Some(
                        "Use the configured admin session token to authenticate.",
                    ))
                    .build(),
            ),
        );
    }
}

struct ApiDocInfo;

impl Modify for ApiDocInfo {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        openapi.info.title = "Geo Engine EBV API".to_string();

        openapi.info.contact = Some(
            utoipa::openapi::ContactBuilder::new()
                .name(Some("Geo Engine Developers"))
                .email(Some("dev@geoengine.de"))
                .build(),
        );

        openapi.info.license = Some(
            utoipa::openapi::LicenseBuilder::new()
                .name("Apache 2.0 (pro features excluded)")
                .url(Some(
                    "https://github.com/geo-engine/geoengine/blob/main/LICENSE",
                ))
                .build(),
        );
    }
}

/// Initialize ebv routes
///
/// # Panics
/// This route initializer panics if `base_url` is `None` and the `ebv` config is not defined.
///
pub(crate) fn init_ebv_routes<C>() -> Box<dyn FnOnce(&mut ServiceConfig)>
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    Box::new(move |cfg: &mut web::ServiceConfig| {
        cfg.service(
            web::scope("/overviews")
                .route("/all", web::put().to(create_overviews::<C>))
                .service(
                    web::scope("/{path}")
                        .route("", web::put().to(create_overview::<C>))
                        .route("", web::delete().to(remove_overview::<C>)),
                ),
        );
    })
}

/// returns the `EbvPortalDataProvider` if it is defined, otherwise the `NetCdfCfDataProvider` if it is defined.
/// Otherwise an erorr is returned.
async fn with_netcdfcf_provider<C: SessionContext, T, F>(
    ctx: &C,
    f: F,
) -> Result<T, NetCdfCf4DProviderError>
where
    T: Send + 'static,
    F: FnOnce(&NetCdfCfDataProvider) -> Result<T, NetCdfCf4DProviderError> + Send + 'static,
{
    let db = ctx.db();

    let ebv_provider: Result<Box<dyn DataProvider>> = db.load_layer_provider(EBV_PROVIDER_ID).await;

    let netcdf_provider = db.load_layer_provider(NETCDF_CF_PROVIDER_ID).await;

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

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(example = json!({
    "resamplingMethod": "NEAREST"
}))]
struct CreateOverviewsParams {
    resampling_method: Option<ResamplingMethod>,
}

/// Create overviews for all NetCDF files of the provider
#[utoipa::path(
    tag = "Overviews",
    put,
    path = "/ebv/overviews/all",
    request_body = Option<CreateOverviewsParams>,
    responses(
        (
            status = 200,
            description = "The id of the task that creates the overviews.", 
            body = TaskResponse,
            example = json!({"taskId": "ca0c86e0-04b2-47b6-9190-122c6f06c45c"})
        )
    ),
    security(
        ("admin_token" = [])
    )
)]
async fn create_overviews<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    params: Option<web::Json<CreateOverviewsParams>>,
) -> Result<impl Responder> {
    let ctx = Arc::new(app_ctx.into_inner().session_context(session.clone()));

    let task = EbvMultiOverviewTask::new(
        ctx.clone(),
        params.as_ref().and_then(|p| p.resampling_method),
    )
    .await?
    .boxed();

    let task_id = ctx.tasks().schedule_task(task, None).await?;

    Ok(web::Json(TaskResponse::new(task_id)))
}

struct EbvMultiOverviewTask<C: SessionContext> {
    ctx: Arc<C>,
    resampling_method: Option<ResamplingMethod>,
    current_subtask_id: Arc<Mutex<Option<TaskId>>>,
    files: Vec<PathBuf>,
}

impl<C: SessionContext> EbvMultiOverviewTask<C> {
    async fn new(
        ctx: Arc<C>,
        resampling_method: Option<ResamplingMethod>,
    ) -> Result<Self, NetCdfCf4DProviderError> {
        let files = with_netcdfcf_provider(ctx.as_ref(), move |provider| {
            provider.list_files().map_err(|_| {
                NetCdfCf4DProviderError::CdfCfProviderCannotListFiles {
                    id: NETCDF_CF_PROVIDER_ID,
                }
            })
        })
        .await?;
        Ok(Self {
            ctx,
            resampling_method,
            current_subtask_id: Arc::new(Mutex::new(None)),
            files,
        })
    }

    fn update_pct(task_ctx: Arc<C::TaskContext>, pct: f64, status: NetCdfCfOverviewResponse) {
        crate::util::spawn(async move {
            task_ctx.set_completion(pct, status.boxed()).await;
        });
    }
}

#[async_trait::async_trait]
impl<C: SessionContext> Task<C::TaskContext> for EbvMultiOverviewTask<C> {
    async fn run(
        &self,
        task_ctx: C::TaskContext,
    ) -> Result<Box<dyn crate::tasks::TaskStatusInfo>, Box<dyn ErrorSource>> {
        let task_ctx = Arc::new(task_ctx);
        let resampling_method = self.resampling_method;
        let current_subtask_id = self.current_subtask_id.clone();

        let num_files = self.files.len();

        let mut status = NetCdfCfOverviewResponse {
            success: vec![],
            skip: vec![],
            error: vec![],
        };

        for (i, file) in self.files.clone().into_iter().enumerate() {
            let subtask: Box<dyn Task<C::TaskContext>> = EbvOverviewTask::<C> {
                ctx: self.ctx.clone(),
                file: file.clone(),
                params: CreateOverviewParams { resampling_method },
            }
            .boxed();

            let (notification_tx, notification_rx) = oneshot::channel();

            let subtask_id = self
                .ctx
                .tasks()
                .schedule_task(subtask, Some(notification_tx))
                .await
                .map_err(ErrorSource::boxed)?;

            current_subtask_id.lock().await.replace(subtask_id);

            if let Ok(subtask_status) = notification_rx.await {
                match subtask_status {
                    TaskStatus::Completed { info, .. } => {
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
                ((i + 1) as f64) / (num_files as f64),
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
        EBV_MULTI_OVERVIEW_TASK_TYPE
    }

    fn task_description(&self) -> String {
        self.files
            .iter()
            .map(|path| path.to_string_lossy().to_string())
            .collect::<Vec<_>>()
            .join(", ")
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

#[derive(Debug, Deserialize, Default, ToSchema)]
#[schema(example = json!({
    "resamplingMethod": "NEAREST"
}))]
struct CreateOverviewParams {
    resampling_method: Option<ResamplingMethod>,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(names("path"))]
struct EbvPath(PathBuf);

/// Creates overview for a single NetCDF file
#[utoipa::path(
    tag = "Overviews",
    put,
    path = "/ebv/overviews/{path}",
    request_body = Option<CreateOverviewParams>,
    responses(
        (
            status = 200,
            description = "The id of the task that creates the overview.", 
            body = TaskResponse,
            example = json!({"taskId": "ca0c86e0-04b2-47b6-9190-122c6f06c45c"})
        )
    ),
    params(
        ("path" = String, description = "The local path to the NetCDF file.")
    ),
    security(
        ("admin_token" = [])
    )
)]
async fn create_overview<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<EbvPath>,
    params: Option<web::Json<CreateOverviewParams>>,
) -> Result<impl Responder> {
    let ctx = Arc::new(app_ctx.into_inner().session_context(session));

    let task = EbvOverviewTask::<C::SessionContext> {
        ctx: ctx.clone(),
        file: path.into_inner().0,
        params: params.map(web::Json::into_inner).unwrap_or_default(),
    }
    .boxed();

    let task_id = ctx.tasks().schedule_task(task, None).await?;

    Ok(web::Json(TaskResponse::new(task_id)))
}

struct EbvOverviewTask<C: SessionContext> {
    ctx: Arc<C>,
    file: PathBuf,
    params: CreateOverviewParams,
}

#[async_trait::async_trait]
impl<C: SessionContext> Task<C::TaskContext> for EbvOverviewTask<C> {
    async fn run(
        &self,
        ctx: C::TaskContext,
    ) -> Result<Box<dyn crate::tasks::TaskStatusInfo>, Box<dyn ErrorSource>> {
        let file = self.file.clone();
        let resampling_method = self.params.resampling_method;

        let response = with_netcdfcf_provider(self.ctx.as_ref(), move |provider| {
            // TODO: provide some detailed pct status

            match provider.create_overviews(&file, resampling_method, &ctx) {
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
                Err(e) => {
                    debug!("Error during overview creation: {:?}", &e);
                    Err(NetCdfCf4DProviderError::CannotCreateOverview {
                        dataset: file,
                        source: Box::new(e),
                    })
                }
            }
        })
        .await;

        response
            .map(TaskStatusInfo::boxed)
            .map_err(ErrorSource::boxed)
    }

    async fn cleanup_on_error(&self, _ctx: C::TaskContext) -> Result<(), Box<dyn ErrorSource>> {
        let file = self.file.clone();

        let response = with_netcdfcf_provider(self.ctx.as_ref(), move |provider| {
            provider
                .remove_overviews(&file, false)
                .boxed_context(error::CannotRemoveOverviews)
        })
        .await;

        response.map_err(ErrorSource::boxed)
    }

    fn task_type(&self) -> &'static str {
        EBV_OVERVIEW_TASK_TYPE
    }

    fn task_unique_id(&self) -> Option<String> {
        Some(self.file.to_string_lossy().to_string())
    }

    fn task_description(&self) -> String {
        self.file.to_string_lossy().to_string()
    }
}

impl TaskStatusInfo for NetCdfCfOverviewResponse {}

#[derive(Debug, Deserialize, Default, ToSchema, IntoParams)]
#[schema(example = json!({
    "force": "false"
}))]
struct RemoveOverviewParams {
    #[serde(default)]
    force: bool,
}

/// Removes an overview for a single NetCDF file.
/// - `force`: If true, the task will be aborted without calling clean-up functions.
#[utoipa::path(
    tag = "Overviews",
    delete,
    path = "/ebv/overviews/{path}",
    request_body = Option<RemoveOverviewParams>,
    responses(
        (
            status = 200,
            description = "The id of the task that removes the overview.", 
            body = TaskResponse,
            example = json!({"taskId": "ca0c86e0-04b2-47b6-9190-122c6f06c45c"})
        )
    ),
    params(
        ("path" = String, description = "The local path to the NetCDF file.")
    ),
    security(
        ("admin_token" = [])
    )
)]
async fn remove_overview<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<EbvPath>,
    params: web::Query<RemoveOverviewParams>,
) -> Result<impl Responder> {
    let ctx = Arc::new(app_ctx.into_inner().session_context(session));

    let task = EbvRemoveOverviewTask::<C::SessionContext> {
        ctx: ctx.clone(),
        file: path.into_inner().0,
        params: params.into_inner(),
    }
    .boxed();

    let task_id = ctx.tasks().schedule_task(task, None).await?;

    Ok(web::Json(TaskResponse::new(task_id)))
}

struct EbvRemoveOverviewTask<C: SessionContext> {
    ctx: Arc<C>,
    file: PathBuf,
    params: RemoveOverviewParams,
}

#[async_trait::async_trait]
impl<C: SessionContext> Task<C::TaskContext> for EbvRemoveOverviewTask<C> {
    async fn run(
        &self,
        _ctx: C::TaskContext,
    ) -> Result<Box<dyn crate::tasks::TaskStatusInfo>, Box<dyn ErrorSource>> {
        let file = self.file.clone();
        let force = self.params.force;

        let response = with_netcdfcf_provider(self.ctx.as_ref(), move |provider| {
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
        EBV_REMOVE_OVERVIEW_TASK_TYPE
    }

    fn task_unique_id(&self) -> Option<String> {
        Some(self.file.to_string_lossy().to_string())
    }

    fn task_description(&self) -> String {
        self.file.to_string_lossy().to_string()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::util::tests::with_temp_context;
    use crate::{
        contexts::SimpleApplicationContext,
        datasets::external::netcdfcf::NetCdfCfDataProviderDefinition,
        tasks::util::test::wait_for_task_to_finish,
        util::server::{configure_extractors, render_404, render_405},
        util::tests::read_body_string,
    };
    use actix_web::{dev::ServiceResponse, http, http::header, middleware, test, web, App};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::test_data;
    use serde_json::json;
    use std::path::Path;

    async fn send_test_request<C: SimpleApplicationContext>(
        req: test::TestRequest,
        app_ctx: C,
    ) -> ServiceResponse {
        let app = test::init_service({
            let app = App::new()
                .app_data(web::Data::new(app_ctx))
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_remove_overviews() {
        fn is_empty(directory: &Path) -> bool {
            directory.read_dir().unwrap().next().is_none()
        }

        with_temp_context(|app_ctx, _| async move {
            let ctx = app_ctx.default_session_context().await.unwrap();

            let session_id = app_ctx.default_session_id().await;

            let overview_folder = tempfile::tempdir().unwrap();

            ctx.db()
                .add_layer_provider(
                    NetCdfCfDataProviderDefinition {
                        name: "test".to_string(),
                        path: test_data!("netcdf4d").to_path_buf(),
                        overviews: overview_folder.path().to_path_buf(),
                        cache_ttl: Default::default(),
                    }
                    .into(),
                )
                .await
                .unwrap();

            let req = actix_web::test::TestRequest::put()
                .uri("/ebv/overviews/dataset_m.nc")
                .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

            let res = send_test_request(req, app_ctx.clone()).await;

            assert_eq!(res.status(), 200, "{:?}", res.response());

            let task_response =
                serde_json::from_str::<TaskResponse>(&read_body_string(res).await).unwrap();

            let tasks = Arc::new(ctx.tasks());

            wait_for_task_to_finish(tasks.clone(), task_response.task_id).await;

            let status = tasks.get_task_status(task_response.task_id).await.unwrap();

            let mut response = if let TaskStatus::Completed { info, .. } = status {
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
                .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

            let res = send_test_request(req, app_ctx.clone()).await;

            assert_eq!(res.status(), 200, "{:?}", res.response());

            let task_response =
                serde_json::from_str::<TaskResponse>(&read_body_string(res).await).unwrap();

            wait_for_task_to_finish(tasks.clone(), task_response.task_id).await;

            let status = tasks.get_task_status(task_response.task_id).await.unwrap();
            let status = serde_json::to_value(status).unwrap();

            assert_eq!(status["status"], json!("completed"));
            assert_eq!(status["info"], json!(null));
            assert_eq!(status["timeTotal"], json!("00:00:00"));
            assert!(status["timeStarted"].is_string());

            assert!(is_empty(overview_folder.path()));
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_remove_overviews_non_existing() {
        // setup

        with_temp_context(|app_ctx, _| async move {
            let ctx = app_ctx.default_session_context().await.unwrap();
            let session_id = app_ctx.default_session_id().await;

            let overview_folder = tempfile::tempdir().unwrap();

            ctx.db()
                .add_layer_provider(
                    NetCdfCfDataProviderDefinition {
                        name: "test".to_string(),
                        path: test_data!("netcdf4d").to_path_buf(),
                        overviews: overview_folder.path().to_path_buf(),
                        cache_ttl: Default::default(),
                    }
                    .into(),
                )
                .await
                .unwrap();

            // remove overviews that don't exist

            let req = actix_web::test::TestRequest::delete()
                .uri("/ebv/overviews/path%2Fto%2Fdataset.nc?force=true")
                .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

            let res = send_test_request(req, app_ctx.clone()).await;

            assert_eq!(res.status(), 200, "{:?}", res.response());

            let task_response =
                serde_json::from_str::<TaskResponse>(&read_body_string(res).await).unwrap();

            let tasks = Arc::new(ctx.tasks());

            wait_for_task_to_finish(tasks.clone(), task_response.task_id).await;

            let status = tasks.get_task_status(task_response.task_id).await.unwrap();
            let status = serde_json::to_value(status).unwrap();

            assert_eq!(status["status"], json!("completed"));
            assert_eq!(status["info"], json!(null));
            assert_eq!(status["timeTotal"], json!("00:00:00"));
            assert!(status["timeStarted"].is_string());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_create_non_existing_overview() {
        fn is_empty(directory: &Path) -> bool {
            directory.read_dir().unwrap().next().is_none()
        }

        with_temp_context(|app_ctx, _| async move {
        let ctx = app_ctx.default_session_context().await.unwrap();
        let session_id = app_ctx.default_session_id().await;

        let overview_folder = tempfile::tempdir().unwrap();

        ctx.db()
            .add_layer_provider(NetCdfCfDataProviderDefinition {
                name: "test".to_string(),
                path: test_data!("netcdf4d").to_path_buf(),
                overviews: overview_folder.path().to_path_buf(),
                cache_ttl: Default::default(),
            }.into())
            .await
            .unwrap();

        let req = actix_web::test::TestRequest::put()
            .uri("/ebv/overviews/foo%2Fbar.nc")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());

        let task_response =
            serde_json::from_str::<TaskResponse>(&read_body_string(res).await).unwrap();

        let tasks = Arc::new(ctx.tasks());
        wait_for_task_to_finish(tasks.clone(), task_response.task_id).await;

        let status = tasks.get_task_status(task_response.task_id).await.unwrap();

        let (error, clean_up) = if let TaskStatus::Failed { error, clean_up } = status {
            (error, serde_json::to_string(&clean_up).unwrap())
        } else {
            panic!("Task must be failed");
        };

        assert!(
            matches!(
                error.clone().into_any_arc().downcast::<NetCdfCf4DProviderError>().unwrap().as_ref(),
                NetCdfCf4DProviderError::CannotCreateOverview { dataset, source }
                if dataset.to_string_lossy() == "foo/bar.nc" &&
                // TODO: use matches clause `NetCdfCf4DProviderError::DatasetIsNotInProviderPath { .. }`
                source.to_string().contains("DatasetIsNotInProviderPath")
            ),
            "{error:?}"
        );

        assert_eq!(clean_up, r#"{"status":"completed","info":null}"#);

        assert!(is_empty(overview_folder.path()));

        })
        .await;
    }
}
