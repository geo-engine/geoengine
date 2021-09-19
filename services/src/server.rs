use crate::contexts::{InMemoryContext, SimpleContext};
use crate::error::{Error, Result};
use crate::handlers;
use crate::handlers::ErrorResponse;
use crate::util::config;
use crate::util::config::get_config_element;

use actix_files::Files;
use actix_web::dev::{Body, ServiceResponse};
use actix_web::error::{InternalError, JsonPayloadError, QueryPayloadError};
use actix_web::{http, middleware, web, App, HttpResponse, HttpServer, Responder};
use log::{debug, info};
use std::net::SocketAddr;
use std::path::PathBuf;
use url::Url;

/// Starts the webserver for the Geo Engine API.
///
/// # Panics
///  * may panic if the `Postgres` backend is chosen without compiling the `postgres` feature
///
///
pub async fn start_server(static_files_dir: Option<PathBuf>) -> Result<()> {
    let web_config: config::Web = get_config_element()?;

    info!(
        "Starting server… {}",
        web_config
            .external_address
            .unwrap_or(Url::parse(&format!("http://{}/", web_config.bind_address))?)
    );

    info!("Using in memory backend");

    let data_path_config: config::DataProvider = get_config_element()?;

    start(
        static_files_dir,
        web_config.bind_address,
        InMemoryContext::new_with_data(
            data_path_config.dataset_defs_path,
            data_path_config.provider_defs_path,
        )
        .await,
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
    let wrapped_ctx = web::Data::new(ctx);

    HttpServer::new(move || {
        let app = App::new()
            .app_data(wrapped_ctx.clone())
            .wrap(
                middleware::ErrorHandlers::default()
                    .handler(http::StatusCode::NOT_FOUND, render_404)
                    .handler(http::StatusCode::METHOD_NOT_ALLOWED, render_405),
            )
            .wrap(middleware::Logger::default())
            .wrap(middleware::NormalizePath::default())
            .configure(configure_extractors)
            .configure(handlers::datasets::init_dataset_routes::<C>)
            .configure(handlers::plots::init_plot_routes::<C>)
            .configure(handlers::projects::init_project_routes::<C>)
            .configure(handlers::session::init_session_routes::<C>)
            .configure(handlers::spatial_references::init_spatial_reference_routes::<C>)
            .configure(handlers::upload::init_upload_routes::<C>)
            .configure(handlers::wcs::init_wcs_routes::<C>)
            .configure(handlers::wfs::init_wfs_routes::<C>)
            .configure(handlers::wms::init_wms_routes::<C>)
            .configure(handlers::workflows::init_workflow_routes::<C>)
            .route("/version", web::get().to(show_version_handler)); // TODO: allow disabling this function via config or feature flag

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
            JsonPayloadError::ContentType => InternalError::from_response(
                err,
                HttpResponse::UnsupportedMediaType().json(ErrorResponse {
                    error: "UnsupportedMediaType".to_string(),
                    message: "Unsupported content type header.".to_string(),
                }),
            )
            .into(),
            JsonPayloadError::Overflow { limit } => InternalError::from_response(
                err,
                HttpResponse::PayloadTooLarge().json(ErrorResponse {
                    error: "Overflow".to_string(),
                    message: format!("JSON payload has exceeded limit ({} bytes).", limit),
                }),
            )
            .into(),
            JsonPayloadError::OverflowKnownLength { length, limit } => {
                InternalError::from_response(
                    err,
                    HttpResponse::PayloadTooLarge().json(ErrorResponse {
                        error: "Overflow".to_string(),
                        message: format!(
                            "JSON payload ({} bytes) is larger than allowed (limit: {} bytes).",
                            length, limit
                        ),
                    }),
                )
                .into()
            }
            JsonPayloadError::Payload(err) => ErrorResponse {
                error: "Payload".to_string(),
                message: err.to_string(),
            }
            .into(),
            JsonPayloadError::Deserialize(err) => ErrorResponse {
                error: "BodyDeserializeError".to_string(),
                message: err.to_string(),
            }
            .into(),
            JsonPayloadError::Serialize(err) => ErrorResponse {
                error: "BodySerializeError".to_string(),
                message: err.to_string(),
            }
            .into(),
            _ => {
                debug!("Unknown JsonPayloadError variant");
                ErrorResponse {
                    error: "UnknownError".to_string(),
                    message: "Unknown Error".to_string(),
                }
                .into()
            }
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
                _ => {
                    debug!("Unknown QueryPayloadError variant");
                    ErrorResponse {
                        error: "UnknownError".to_string(),
                        message: "Unknown Error".to_string(),
                    }
                }
            }),
        )
        .into()
    }));
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

pub(crate) fn render_404(
    mut res: ServiceResponse,
) -> actix_web::Result<middleware::ErrorHandlerResponse<Body>> {
    res.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    let res = res.map_body(|_, _| {
        Body::from(
            serde_json::to_string(&ErrorResponse {
                error: "NotFound".to_string(),
                message: "Not Found".to_string(),
            })
            .unwrap(),
        )
    });
    Ok(middleware::ErrorHandlerResponse::Response(res))
}

pub(crate) fn render_405(
    mut res: ServiceResponse,
) -> actix_web::Result<middleware::ErrorHandlerResponse<Body>> {
    res.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    let res = res.map_body(|_, _| {
        Body::from(
            serde_json::to_string(&ErrorResponse {
                error: "MethodNotAllowed".to_string(),
                message: "HTTP method not allowed.".to_string(),
            })
            .unwrap(),
        )
    });
    Ok(middleware::ErrorHandlerResponse::Response(res))
}

#[cfg(test)]
mod tests {
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
