use crate::apidoc::ApiDoc;
use crate::contexts::{InMemoryContext, SimpleContext};
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

    info!("Using in memory backend");

    let data_path_config: config::DataProvider = get_config_element()?;

    let chunk_byte_size = config::get_config_element::<config::QueryContext>()?
        .chunk_byte_size
        .into();

    let tiling_spec = config::get_config_element::<config::TilingSpecification>()?.into();

    register_gdal_drivers_from_list(config::get_config_element::<config::Gdal>()?.allowed_drivers);

    let ctx = InMemoryContext::new_with_data(
        data_path_config.dataset_defs_path,
        data_path_config.provider_defs_path,
        data_path_config.layer_defs_path,
        data_path_config.layer_collection_defs_path,
        tiling_spec,
        chunk_byte_size,
    )
    .await;

    start(
        static_files_dir,
        web_config.bind_address,
        web_config.version_api,
        ctx,
    )
    .await
}

async fn start<C>(
    static_files_dir: Option<PathBuf>,
    bind_address: SocketAddr,
    version_api: bool,
    ctx: C,
) -> Result<(), Error>
where
    C: SimpleContext,
{
    let wrapped_ctx = web::Data::new(ctx);

    let openapi = ApiDoc::openapi();

    HttpServer::new(move || {
        #[allow(unused_mut)]
        let mut app = App::new()
            .app_data(wrapped_ctx.clone())
            .wrap(
                middleware::ErrorHandlers::default()
                    .handler(http::StatusCode::NOT_FOUND, render_404)
                    .handler(http::StatusCode::METHOD_NOT_ALLOWED, render_405),
            )
            .wrap(TracingLogger::<CustomRootSpanBuilder>::new())
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
            .configure(handlers::workflows::init_workflow_routes::<C>);

        let mut api_urls = vec![];

        app = serve_openapi_json(
            app,
            &mut api_urls,
            "Geo Engine",
            "../api-docs/openapi.json",
            "/api-docs/openapi.json",
            openapi.clone(),
        );

        #[cfg(feature = "ebv")]
        {
            app = app.service(web::scope("/ebv").configure(handlers::ebv::init_ebv_routes::<C>()));

            app = serve_openapi_json(
                app,
                &mut api_urls,
                "EBV",
                "../api-docs/ebv/openapi.json",
                "/api-docs/ebv/openapi.json",
                crate::handlers::ebv::ApiDoc::openapi(),
            );
        }

        #[cfg(feature = "nfdi")]
        {
            app = app.configure(handlers::gfbio::init_gfbio_routes::<C>);
        }

        app = app.service(SwaggerUi::new("/swagger-ui/{_:.*}").urls(api_urls));

        if version_api {
            app = app.route(
                "/info",
                web::get().to(crate::util::server::server_info_handler),
            );
        }
        if let Some(static_files_dir) = static_files_dir.clone() {
            app.service(Files::new("/static", static_files_dir))
        } else {
            app
        }
    })
    .worker_max_blocking_threads(calculate_max_blocking_threads_per_worker())
    .on_connect(connection_init)
    .bind(bind_address)?
    .run()
    .await
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::*;
    use crate::contexts::{Session, SimpleSession};
    use crate::handlers::ErrorResponse;

    /// Test the webserver startup to ensure that `tokio` and `actix` are working properly
    #[actix_rt::test]
    async fn webserver_start() {
        tokio::select! {
            server = start_server(None) => {
                server.expect("server run");
            }
            _ = queries() => {}
        }
    }

    async fn queries() {
        let web_config: config::Web = get_config_element().unwrap();
        let base_url = Url::parse(&format!("http://{}", web_config.bind_address)).unwrap();

        assert!(wait_for_server(&base_url).await);
        issue_queries(&base_url).await;
    }

    async fn issue_queries(base_url: &Url) {
        let client = reqwest::Client::new();

        let body = client
            .post(base_url.join("anonymous").unwrap())
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        let session: SimpleSession = serde_json::from_str(&body).unwrap();

        let body = client
            .post(base_url.join("project").unwrap())
            .header("Authorization", format!("Bearer {}", session.id()))
            .header("Content-Type", "application/json")
            .body("no json")
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        assert_eq!(
            serde_json::from_str::<ErrorResponse>(&body).unwrap(),
            ErrorResponse {
                error: "BodyDeserializeError".to_string(),
                message: "expected ident at line 1 column 2".to_string()
            }
        );
    }

    const WAIT_SERVER_RETRIES: i32 = 5;
    const WAIT_SERVER_RETRY_INTERVAL: u64 = 1;

    async fn wait_for_server(base_url: &Url) -> bool {
        for _ in 0..WAIT_SERVER_RETRIES {
            if reqwest::get(base_url.clone()).await.is_ok() {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_secs(WAIT_SERVER_RETRY_INTERVAL));
        }
        false
    }
}
