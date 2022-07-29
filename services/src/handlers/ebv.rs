//! GEO BON EBV Portal catalog lookup service
//!
//! Connects to <https://portal.geobon.org/api/v1/>.

use super::tasks::TaskResponse;
use crate::contexts::AdminSession;
use crate::datasets::external::netcdfcf::{
    EbvPortalDataProvider, NetCdfCf4DProviderError, OverviewGeneration, EBV_PROVIDER_ID,
    NETCDF_CF_PROVIDER_ID,
};
use crate::error::Result;
use crate::layers::external::DataProvider;
use crate::layers::storage::LayerProviderDb;
use crate::tasks::{Task, TaskContext, TaskManager, TaskStatusInfo};
use crate::{contexts::Context, datasets::external::netcdfcf::NetCdfCfDataProvider};
use actix_web::{
    web::{self, ServiceConfig},
    FromRequest, Responder,
};
use geoengine_datatypes::error::{BoxedResultExt, ErrorSource};
use log::warn;
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
        .service(web::resource("/create_overview").route(web::post().to(create_overview::<C>)));
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NetCdfCfOverviewResponse {
    success: Vec<PathBuf>,
    skip: Vec<PathBuf>,
    error: Vec<PathBuf>,
}

/// Create overviews for a all `NetCDF` files of the provider
async fn create_overviews<C: Context>(
    session: AdminSession,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let ctx = ctx.into_inner();

    let task: Box<dyn Task<C::TaskContext>> = EvbMultiOverviewTask::<C> {
        session,
        ctx: ctx.clone(),
    }
    .boxed();

    let task_id = ctx.tasks_ref().schedule(task).await?;

    Ok(web::Json(TaskResponse::new(task_id)))
}

struct EvbMultiOverviewTask<C: Context> {
    session: AdminSession,
    ctx: Arc<C>,
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
        self: Box<Self>,
        task_ctx: C::TaskContext,
    ) -> Result<Box<dyn crate::tasks::TaskStatusInfo>, Box<dyn ErrorSource>> {
        let task_ctx = Arc::new(task_ctx);

        let response =
            with_netcdfcf_provider(self.ctx.as_ref(), &self.session.into(), move |provider| {
                let mut status = NetCdfCfOverviewResponse {
                    success: vec![],
                    skip: vec![],
                    error: vec![],
                };

                let files = provider.list_files().map_err(|_| {
                    NetCdfCf4DProviderError::CdfCfProviderCannotListFiles {
                        id: NETCDF_CF_PROVIDER_ID,
                    }
                })?;

                let num_files = files.len();

                for (i, file) in files.into_iter().enumerate() {
                    // TODO: provide some more detailed pct status

                    match provider.create_overviews(&file) {
                        Ok(OverviewGeneration::Created) => status.success.push(file),
                        Ok(OverviewGeneration::Skipped) => status.skip.push(file),
                        Err(e) => {
                            warn!("Failed to create overviews for {}: {e}", file.display());
                            status.error.push(file);
                        }
                    }

                    Self::update_pct(
                        task_ctx.clone(),
                        ((i + 1) / num_files) as u8,
                        status.clone(),
                    );
                }

                Result::<_, NetCdfCf4DProviderError>::Ok(status.boxed())
            })
            .await;

        response.map_err(ErrorSource::boxed)
    }
}

#[derive(Debug, Deserialize)]
struct CreateOverviewParams {
    file: PathBuf,
}

/// Create overviews for a single `NetCDF` file
async fn create_overview<C: Context>(
    session: AdminSession,
    ctx: web::Data<C>,
    params: web::Json<CreateOverviewParams>,
) -> Result<impl Responder> {
    let ctx = ctx.into_inner();

    let task: Box<dyn Task<C::TaskContext>> = EvbOverviewTask::<C> {
        session,
        ctx: ctx.clone(),
        params: params.into_inner(),
    }
    .boxed();

    let task_id = ctx.tasks_ref().schedule(task).await?;

    Ok(web::Json(TaskResponse::new(task_id)))
}

struct EvbOverviewTask<C: Context> {
    session: AdminSession,
    ctx: Arc<C>,
    params: CreateOverviewParams,
}

#[async_trait::async_trait]
impl<C: Context> Task<C::TaskContext> for EvbOverviewTask<C> {
    async fn run(
        self: Box<Self>,
        _ctx: C::TaskContext,
    ) -> Result<Box<dyn crate::tasks::TaskStatusInfo>, Box<dyn ErrorSource>> {
        let file = self.params.file;

        let response =
            with_netcdfcf_provider(self.ctx.as_ref(), &self.session.into(), move |provider| {
                // TODO: provide some detailed pct status

                Ok(match provider.create_overviews(&file) {
                    Ok(OverviewGeneration::Created) => NetCdfCfOverviewResponse {
                        success: vec![file],
                        skip: vec![],
                        error: vec![],
                    },
                    Ok(OverviewGeneration::Skipped) => NetCdfCfOverviewResponse {
                        success: vec![],
                        skip: vec![file],
                        error: vec![],
                    },
                    Err(e) => {
                        warn!("Failed to create overviews for {}: {e}", file.display());
                        NetCdfCfOverviewResponse {
                            success: vec![],
                            skip: vec![],
                            error: vec![file],
                        }
                    }
                })
            })
            .await;

        response
            .map(TaskStatusInfo::boxed)
            .map_err(ErrorSource::boxed)
    }
}

impl TaskStatusInfo for NetCdfCfOverviewResponse {}
