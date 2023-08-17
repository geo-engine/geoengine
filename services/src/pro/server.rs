use crate::contexts::{ApplicationContext, SessionContext};
use crate::error::{Error, Result};
use crate::handlers;
use crate::pro;
use crate::pro::apidoc::ApiDoc;

use crate::pro::contexts::ProPostgresContext;
use crate::util::config::{self, get_config_element};
use crate::util::server::{
    calculate_max_blocking_threads_per_worker, configure_extractors, connection_init,
    log_server_info, render_404, render_405, serve_openapi_json, CustomRootSpanBuilder,
};
use actix_files::Files;
use actix_web::{http, middleware, web, App, FromRequest, HttpServer};

use bb8_postgres::tokio_postgres::NoTls;
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_operators::engine::ChunkByteSize;
use geoengine_operators::util::gdal::register_gdal_drivers_from_list;
use log::{info, warn};
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing_actix_web::TracingLogger;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use super::contexts::{ProApplicationContext, ProGeoEngineDb};

async fn start<C>(
    static_files_dir: Option<PathBuf>,
    bind_address: SocketAddr,
    api_prefix: String,
    version_api: bool,
    app_ctx: C,
) -> Result<(), Error>
where
    C: ProApplicationContext,
    C::Session: FromRequest,
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let wrapped_ctx = web::Data::new(app_ctx);

    let openapi = ApiDoc::openapi();

    HttpServer::new(move || {
        let mut api = web::scope(&api_prefix)
            .configure(configure_extractors)
            .configure(pro::handlers::datasets::init_dataset_routes::<C>)
            .configure(handlers::layers::init_layer_routes::<C>)
            .configure(pro::handlers::permissions::init_permissions_routes::<C>)
            .configure(handlers::plots::init_plot_routes::<C>)
            .configure(handlers::projects::init_project_routes::<C>)
            .configure(pro::handlers::users::init_user_routes::<C>)
            .configure(handlers::spatial_references::init_spatial_reference_routes::<C>)
            .configure(handlers::upload::init_upload_routes::<C>)
            .configure(handlers::tasks::init_task_routes::<C>)
            .configure(handlers::wcs::init_wcs_routes::<C>)
            .configure(handlers::wfs::init_wfs_routes::<C>)
            .configure(handlers::wms::init_wms_routes::<C>)
            .configure(handlers::workflows::init_workflow_routes::<C>)
            .route(
                "/available",
                web::get().to(crate::util::server::available_handler),
            );

        let mut api_urls = vec![];

        api = serve_openapi_json(
            api,
            &mut api_urls,
            "Geo Engine Pro",
            "../api-docs/openapi.json",
            "/api-docs/openapi.json",
            openapi.clone(),
        );

        // EBV endpoint
        {
            api = api.service(web::scope("/ebv").configure(handlers::ebv::init_ebv_routes::<C>()));

            api = serve_openapi_json(
                api,
                &mut api_urls,
                "EBV",
                "../api-docs/ebv/openapi.json",
                "/api-docs/ebv/openapi.json",
                crate::handlers::ebv::ApiDoc::openapi(),
            );
        }

        api = api.service(SwaggerUi::new("/swagger-ui/{_:.*}").urls(api_urls));

        if version_api {
            api = api.route(
                "/info",
                web::get().to(crate::util::server::server_info_handler),
            );
        }

        if let Some(static_files_dir) = static_files_dir.clone() {
            api = api.service(Files::new("/static", static_files_dir));
        }

        App::new()
            .app_data(wrapped_ctx.clone())
            .wrap(
                middleware::ErrorHandlers::default()
                    .handler(http::StatusCode::NOT_FOUND, render_404)
                    .handler(http::StatusCode::METHOD_NOT_ALLOWED, render_405),
            )
            .wrap(TracingLogger::<CustomRootSpanBuilder>::new())
            .service(api)
    })
    .worker_max_blocking_threads(calculate_max_blocking_threads_per_worker())
    .on_connect(connection_init)
    .bind(bind_address)?
    .run()
    .await
    .map_err(Into::into)
}

#[allow(clippy::print_stderr)]
fn print_pro_info_message() {
    eprintln!("|===========================================================================|");
    eprintln!("| Welcome to Geo Engine Pro Version: Please refer to our license agreement. |");
    eprintln!("| If you have any question: Visit https://www.geoengine.io.                 |");
    eprintln!("|===========================================================================|");
}

/// Starts the webserver for the Geo Engine API.
///
/// # Panics
///  * may panic if the `Postgres` backend is chosen without compiling the `postgres` feature
///
///
pub async fn start_pro_server(static_files_dir: Option<PathBuf>) -> Result<()> {
    print_pro_info_message();

    log_server_info()?;

    let user_config: crate::pro::util::config::User = get_config_element()?;
    let oidc_config: crate::pro::util::config::Oidc = get_config_element()?;
    let session_config: crate::util::config::Session = get_config_element()?;
    let web_config: crate::util::config::Web = get_config_element()?;
    let open_telemetry: crate::pro::util::config::OpenTelemetry = get_config_element()?;
    let cache_config: crate::pro::util::config::Cache = get_config_element()?;
    let quota_config: crate::pro::util::config::Quota = get_config_element()?;

    if user_config.user_registration {
        info!("User Registration: enabled");
    } else {
        info!("User Registration: disabled");
    }

    if oidc_config.enabled {
        info!("OIDC: enabled");
    } else {
        info!("OIDC: disabled");
    }

    if open_telemetry.enabled {
        info!("OpenTelemetry Tracing: enabled");
        info!(
            "OpenTelemetry Tracing Endpoint: {}",
            open_telemetry.endpoint
        );
    } else {
        info!("OpenTelemetry Tracing: disabled");
    }

    if session_config.fixed_session_token.is_some() {
        warn!("Fixed session token is set, but it will be ignored in Geo Engine Pro");
    }

    if cache_config.enabled {
        info!("Cache: enabled ({} MB)", cache_config.cache_size_in_mb);
    } else {
        info!("Cache: disabled");
    }

    info!("QuotaTracking: {:?}", quota_config.mode);

    let data_path_config: config::DataProvider = get_config_element()?;

    let chunk_byte_size = config::get_config_element::<config::QueryContext>()?
        .chunk_byte_size
        .into();

    let tiling_spec = config::get_config_element::<config::TilingSpecification>()?.into();

    register_gdal_drivers_from_list(config::get_config_element::<config::Gdal>()?.allowed_drivers);

    start_postgres(
        data_path_config,
        tiling_spec,
        oidc_config,
        chunk_byte_size,
        static_files_dir,
        web_config,
        cache_config,
        quota_config,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn start_postgres(
    data_path_config: config::DataProvider,
    tiling_spec: TilingSpecification,
    oidc_config: crate::pro::util::config::Oidc,
    chunk_byte_size: ChunkByteSize,
    static_files_dir: Option<PathBuf>,
    web_config: config::Web,
    cache_config: crate::pro::util::config::Cache,
    quota_config: crate::pro::util::config::Quota,
) -> Result<()> {
    {
        let db_config = config::get_config_element::<config::Postgres>()?;
        let mut pg_config = bb8_postgres::tokio_postgres::Config::new();
        pg_config
            .user(&db_config.user)
            .password(&db_config.password)
            .host(&db_config.host)
            .dbname(&db_config.database)
            // fix schema by providing `search_path` option
            .options(&format!("-c search_path={}", db_config.schema));

        let ctx = ProPostgresContext::new_with_data(
            pg_config,
            NoTls,
            data_path_config.dataset_defs_path,
            data_path_config.provider_defs_path,
            data_path_config.layer_defs_path,
            data_path_config.layer_collection_defs_path,
            tiling_spec,
            chunk_byte_size,
            oidc_config,
            cache_config,
            quota_config,
        )
        .await?;

        start(
            static_files_dir,
            web_config.bind_address,
            web_config.api_prefix,
            web_config.version_api,
            ctx,
        )
        .await
    }
}
