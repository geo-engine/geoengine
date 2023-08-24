use crate::apidoc::ApiDoc;
use crate::contexts::{PostgresContext, SimpleApplicationContext};
use crate::error::{Error, Result};
use crate::handlers;
use crate::util::config;
use crate::util::config::get_config_element;
use crate::util::server::{
    calculate_max_blocking_threads_per_worker, configure_extractors, connection_init,
    log_server_info, render_404, render_405, serve_openapi_json, CustomRootSpanBuilder,
};
use actix_files::Files;
use actix_web::{http, middleware, web, App, HttpServer};
use geoengine_operators::util::gdal::register_gdal_drivers_from_list;
use log::info;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing_actix_web::TracingLogger;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

/// Starts the webserver for the Geo Engine API.
///
/// # Panics
///  * may panic if the `Postgres` backend is chosen without compiling the `postgres` feature
///
///
pub async fn start_server(static_files_dir: Option<PathBuf>) -> Result<()> {
    log_server_info()?;

    let web_config: crate::util::config::Web = get_config_element()?;
    let session_config: crate::util::config::Session = get_config_element()?;

    if let Some(session_token) = session_config.fixed_session_token {
        info!("Fixed Session Token: {session_token}");
    }

    let data_path_config: config::DataProvider = get_config_element()?;

    let chunk_byte_size = config::get_config_element::<config::QueryContext>()?
        .chunk_byte_size
        .into();

    let tiling_spec = config::get_config_element::<config::TilingSpecification>()?.into();

    register_gdal_drivers_from_list(config::get_config_element::<config::Gdal>()?.allowed_drivers);

    let db_config = config::get_config_element::<config::Postgres>()?;
    let mut pg_config = bb8_postgres::tokio_postgres::Config::new();
    pg_config
        .user(&db_config.user)
        .password(&db_config.password)
        .host(&db_config.host)
        .dbname(&db_config.database)
        // fix schema by providing `search_path` option
        .options(&format!("-c search_path={}", db_config.schema));

    let ctx = PostgresContext::new_with_data(
        pg_config,
        tokio_postgres::NoTls,
        data_path_config.dataset_defs_path,
        data_path_config.provider_defs_path,
        data_path_config.layer_defs_path,
        data_path_config.layer_collection_defs_path,
        tiling_spec,
        chunk_byte_size,
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

async fn start<C>(
    static_files_dir: Option<PathBuf>,
    bind_address: SocketAddr,
    api_prefix: String,
    version_api: bool,
    app_ctx: C,
) -> Result<(), Error>
where
    C: SimpleApplicationContext,
{
    let wrapped_ctx = web::Data::new(app_ctx);

    let openapi = ApiDoc::openapi();

    HttpServer::new(move || {
        let mut api = web::scope(&api_prefix)
            .configure(configure_extractors)
            .configure(handlers::datasets::init_dataset_routes::<C>)
            .configure(handlers::layers::init_layer_routes::<C>)
            .configure(handlers::plots::init_plot_routes::<C>)
            .configure(handlers::projects::init_project_routes::<C>)
            .configure(handlers::session::init_session_routes::<C>)
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
            "Geo Engine",
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
