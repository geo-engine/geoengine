use crate::contexts::{InMemoryContext, SimpleContext};
use crate::error;
use crate::error::{Error, Result};
use crate::handlers;
use crate::handlers::{validate_token, ErrorResponse};
use crate::util::config;
use crate::util::config::get_config_element;

use actix_files::Files;
use actix_web::body::ResponseBody;
use actix_web::dev::ServiceResponse;
use actix_web::error::{InternalError, JsonPayloadError, QueryPayloadError};
use actix_web::{http, middleware, web, App, HttpResponse, HttpServer, Responder};
use actix_web_httpauth::middleware::HttpAuthentication;
use log::info;
use snafu::ResultExt;
use std::net::SocketAddr;
use std::path::PathBuf;

/// Starts the webserver for the Geo Engine API.
///
/// # Panics
///  * may panic if the `Postgres` backend is chosen without compiling the `postgres` feature
///
///
pub async fn start_server(static_files_dir: Option<PathBuf>) -> Result<()> {
    let web_config: config::Web = get_config_element()?;
    let bind_address = web_config
        .bind_address
        .parse::<SocketAddr>()
        .context(error::AddrParse)?;

    info!(
        "Starting server… {}",
        format!(
            "http://{}/",
            web_config
                .external_address
                .unwrap_or(web_config.bind_address)
        )
    );

    info!("Using in memory backend");

    start(
        static_files_dir,
        bind_address,
        InMemoryContext::new_with_data().await,
    )
    .await
}

async fn start<C>(
    static_files_dir: Option<PathBuf>,
    bind_address: SocketAddr,
    ctx: C,
) -> Result<(), Error>
where
    C: SimpleContext,
{
    //handlers::workflows::dataset_from_workflow_handler(ctx.clone()),
    let wrapped_ctx = web::Data::new(ctx);

    HttpServer::new(move || {
        let app = App::new()
            .app_data(wrapped_ctx.clone())
            .wrap(middleware::Logger::default())
            .wrap(middleware::NormalizePath::default())
            .configure(configure_extractors)
            .configure(init_routes::<C>);

        if let Some(static_files_dir) = static_files_dir.clone() {
            app.service(Files::new("/static", static_files_dir))
        } else {
            app
        }
    })
    .bind(bind_address)?
    .run()
    .await
    .map_err(Into::into)
}

pub(crate) fn configure_extractors(cfg: &mut web::ServiceConfig) {
    cfg.app_data(web::JsonConfig::default().error_handler(|err, _req| {
        match err {
            JsonPayloadError::Overflow => todo!(),
            JsonPayloadError::ContentType => InternalError::from_response(
                err,
                HttpResponse::UnsupportedMediaType().json(ErrorResponse {
                    error: "UnsupportedMediaType".to_string(),
                    message: "Unsupported content type header.".to_string(),
                }),
            )
            .into(),
            JsonPayloadError::Deserialize(err) => ErrorResponse {
                error: "BodyDeserializeError".to_string(),
                message: err.to_string(),
            }
            .into(),
            JsonPayloadError::Payload(_err) => todo!(),
        }
    }));
    cfg.app_data(web::QueryConfig::default().error_handler(|err, _req| {
        actix_web::error::InternalError::from_response(
            "",
            HttpResponse::BadRequest().json(match err {
                QueryPayloadError::Deserialize(err) => ErrorResponse {
                    error: "UnableToParseQueryString".to_string(),
                    message: format!("Unable to parse query string: {}", err),
                },
            }),
        )
        .into()
    }));
}

pub(crate) fn render_401<B>(
    mut res: ServiceResponse<B>,
) -> actix_web::Result<middleware::ErrorHandlerResponse<B>> {
    res.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    res = res.map_body(|_, _| {
        ResponseBody::Other(
            serde_json::to_string(&ErrorResponse {
                error: "MissingAuthorizationHeader".to_string(),
                message: "Header with authorization token not provided.".to_string(),
            })
            .unwrap()
            .into(),
        )
    });
    Ok(middleware::ErrorHandlerResponse::Response(res))
}

pub(crate) fn init_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: SimpleContext,
{
    cfg.route("/version", web::get().to(show_version_handler)) // TODO: allow disabling this function via config or feature flag
        .route("/wms", web::get().to(handlers::wms::wms_handler::<C>))
        .route("/wfs", web::get().to(handlers::wfs::wfs_handler::<C>))
        .route(
            "/wcs/{workflow}",
            web::get().to(handlers::wcs::wcs_handler::<C>),
        )
        .route(
            "/anonymous",
            web::post().to(handlers::session::anonymous_handler::<C>),
        )
        .service(
            web::scope("")
                .wrap(HttpAuthentication::bearer(validate_token::<C>))
                .wrap(
                    middleware::ErrorHandlers::new()
                        .handler(http::StatusCode::UNAUTHORIZED, render_401),
                )
                .route(
                    "/workflow",
                    web::post().to(handlers::workflows::register_workflow_handler::<C>),
                )
                .route(
                    "/workflow/{id}",
                    web::get().to(handlers::workflows::load_workflow_handler::<C>),
                )
                .route(
                    "/workflow/{id}/metadata",
                    web::get().to(handlers::workflows::get_workflow_metadata_handler::<C>),
                )
                .route(
                    "/workflow/{id}/provenance",
                    web::get().to(handlers::workflows::get_workflow_provenance_handler::<C>),
                )
                .route(
                    "datasetFromWorkflow/{workflow_id}",
                    web::post().to(handlers::workflows::dataset_from_workflow_handler::<C>),
                )
                .route(
                    "/session",
                    web::get().to(handlers::session::session_handler::<C>),
                )
                .route(
                    "/session/project/{project}",
                    web::post().to(handlers::session::session_project_handler::<C>),
                )
                .route(
                    "/session/view",
                    web::post().to(handlers::session::session_view_handler::<C>),
                )
                .route(
                    "/project",
                    web::post().to(handlers::projects::create_project_handler::<C>),
                )
                .route(
                    "/projects",
                    web::get().to(handlers::projects::list_projects_handler::<C>),
                )
                .route(
                    "/project/{project}",
                    web::patch().to(handlers::projects::update_project_handler::<C>),
                )
                .route(
                    "/project/{project}",
                    web::delete().to(handlers::projects::delete_project_handler::<C>),
                )
                .route(
                    "/project/{project}",
                    web::get().to(handlers::projects::load_project_handler::<C>),
                )
                .route(
                    "/dataset/internal/{dataset}",
                    web::get().to(handlers::datasets::get_dataset_handler::<C>),
                )
                .route(
                    "/dataset/auto",
                    web::post().to(handlers::datasets::auto_create_dataset_handler::<C>),
                )
                .route(
                    "/dataset",
                    web::post().to(handlers::datasets::create_dataset_handler::<C>),
                )
                .route(
                    "/dataset/suggest",
                    web::get().to(handlers::datasets::suggest_meta_data_handler::<C>),
                )
                .route(
                    "/providers",
                    web::get().to(handlers::datasets::list_providers_handler::<C>),
                )
                .route(
                    "/datasets/external/{provider}",
                    web::get().to(handlers::datasets::list_external_datasets_handler::<C>),
                )
                .route(
                    "/datasets",
                    web::get().to(handlers::datasets::list_datasets_handler::<C>),
                )
                .route(
                    "/plot/{id}",
                    web::get().to(handlers::plots::get_plot_handler::<C>),
                )
                .route(
                    "/upload",
                    web::post().to(handlers::upload::upload_handler::<C>),
                )
                .route(
                    "/spatialReferenceSpecification/{srs_string}",
                    web::get().to(
                        handlers::spatial_references::get_spatial_reference_specification_handler::<
                            C,
                        >,
                    ),
                ),
        );
}

/// Shows information about the server software version.
///
/// # Example
///
/// ```text
/// GET /version
/// ```
/// Response:
/// ```text
/// {
///   "buildDate": "2021-05-17",
///   "commitHash": "16cd0881a79b6f03bb5f1f6ef2b2711e570b9865"
/// }
/// ```
#[allow(clippy::unused_async)] // the function signature of request handlers requires it
pub(crate) async fn show_version_handler() -> impl Responder {
    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct VersionInfo<'a> {
        build_date: Option<&'a str>,
        commit_hash: Option<&'a str>,
    }

    web::Json(&VersionInfo {
        build_date: option_env!("VERGEN_BUILD_DATE"),
        commit_hash: option_env!("VERGEN_GIT_SHA"),
    })
}

/*#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{Session, SimpleSession};
    use crate::handlers::ErrorResponse;
    use tokio::sync::oneshot;

    /// Test the webserver startup to ensure that `tokio` and `warp` are working properly
    #[tokio::test]
    async fn webserver_start() {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let (server, _) =
            tokio::join!(start_server(Some(shutdown_rx), None), queries(shutdown_tx),);
        server.expect("server run");
    }

    async fn queries(shutdown_tx: Sender<()>) {
        let web_config: config::Web = get_config_element().unwrap();
        let base_url = format!("http://{}", web_config.bind_address);

        assert!(wait_for_server(&base_url).await);
        issue_queries(&base_url).await;

        shutdown_tx.send(()).unwrap();
    }

    async fn issue_queries(base_url: &str) {
        let client = reqwest::Client::new();

        let body = client
            .post(&format!("{}/{}", base_url, "anonymous"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        let session: SimpleSession = serde_json::from_str(&body).unwrap();

        let body = client
            .post(&format!("{}/{}", base_url, "project"))
            .header("Authorization", format!("Bearer {}", session.id()))
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

    async fn wait_for_server(base_url: &str) -> bool {
        for _ in 0..WAIT_SERVER_RETRIES {
            if reqwest::get(base_url).await.is_ok() {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_secs(WAIT_SERVER_RETRY_INTERVAL));
        }
        false
    }
}*/
