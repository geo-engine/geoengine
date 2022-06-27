use crate::contexts::{InMemoryContext, SimpleContext};
use crate::error::{Error, Result};
use crate::handlers;
use crate::handlers::ErrorResponse;
use crate::util::config;
use crate::util::config::get_config_element;

use actix_files::Files;
use actix_http::body::{BoxBody, EitherBody, MessageBody};
use actix_http::uri::PathAndQuery;
use actix_http::HttpMessage;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::error::{InternalError, JsonPayloadError, QueryPayloadError};
use actix_web::{http, middleware, web, App, HttpResponse, HttpServer};
use log::{debug, info};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use tracing::Span;
use tracing_actix_web::{RequestId, RootSpanBuilder, TracingLogger};
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
        "Starting server… local address: {}, external address: {}",
        Url::parse(&format!("http://{}/", web_config.bind_address))?,
        web_config
            .external_address
            .unwrap_or(Url::parse(&format!("http://{}/", web_config.bind_address))?)
    );

    let session_config: crate::util::config::Session = get_config_element()?;

    if session_config.anonymous_access {
        info!("Anonymous access is enabled");
    } else {
        info!("Anonymous access is disabled");
    }

    if let Some(session_token) = session_config.fixed_session_token {
        info!("Fixed session token is set, it is {session_token}");
    }

    info!("Using in memory backend");

    let data_path_config: config::DataProvider = get_config_element()?;

    let chunk_byte_size = config::get_config_element::<config::QueryContext>()?
        .chunk_byte_size
        .into();

    let tiling_spec = config::get_config_element::<config::TilingSpecification>()?.into();

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
            .wrap(middleware::NormalizePath::trim())
            .configure(configure_extractors)
            .configure(handlers::datasets::init_dataset_routes::<C>)
            .configure(handlers::layers::init_layer_routes::<C>)
            .configure(handlers::plots::init_plot_routes::<C>)
            .configure(handlers::projects::init_project_routes::<C>)
            .configure(handlers::session::init_session_routes::<C>)
            .configure(handlers::spatial_references::init_spatial_reference_routes::<C>)
            .configure(handlers::upload::init_upload_routes::<C>)
            .configure(handlers::wcs::init_wcs_routes::<C>)
            .configure(handlers::wfs::init_wfs_routes::<C>)
            .configure(handlers::wms::init_wms_routes::<C>)
            .configure(handlers::workflows::init_workflow_routes::<C>);

        #[cfg(feature = "ebv")]
        {
            app = app
                .service(web::scope("/ebv").configure(handlers::ebv::init_ebv_routes::<C>(None)));
        }

        #[cfg(feature = "nfdi")]
        {
            app = app.configure(handlers::gfbio::init_gfbio_routes::<C>);
        }
        if version_api {
            app = app.route("/version", web::get().to(show_version_handler));
        }
        if let Some(static_files_dir) = static_files_dir.clone() {
            app.service(Files::new("/static", static_files_dir))
        } else {
            app
        }
    })
    .worker_max_blocking_threads(calculate_max_blocking_threads_per_worker())
    .bind(bind_address)?
    .run()
    .await
    .map_err(Into::into)
}

/// Custom root span for web requests that paste a request id to all logs.
pub struct CustomRootSpanBuilder;

impl RootSpanBuilder for CustomRootSpanBuilder {
    fn on_request_start(request: &ServiceRequest) -> Span {
        let request_id = request.extensions().get::<RequestId>().copied().unwrap();

        let span = tracing::info_span!("Request", request_id = %request_id);

        // Emit HTTP request at the beginng of the span.
        {
            let _entered = span.enter();

            let head = request.head();
            let http_method = head.method.as_str();

            let http_route: std::borrow::Cow<'static, str> = request
                .match_pattern()
                .map_or_else(|| "default".into(), Into::into);

            let http_target = request
                .uri()
                .path_and_query()
                .map_or("", PathAndQuery::as_str);

            tracing::info!(
                target: "HTTP request",
                method = %http_method,
                route = %http_route,
                target = %http_target,
            );
        }

        span
    }

    fn on_request_end<B>(_span: Span, _outcome: &Result<ServiceResponse<B>, actix_web::Error>) {}
}

/// Calculate maximum number of blocking threads **per worker**.
///
/// By default set to 512 / workers.
///
/// TODO: use blocking threads globally instead of per worker.
///
pub(crate) fn calculate_max_blocking_threads_per_worker() -> usize {
    const MIN_BLOCKING_THREADS_PER_WORKER: usize = 32;

    // Taken from `actix_server::ServerBuilder`.
    // By default, server uses number of available logical CPU as thread count.
    let number_of_workers = std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(1);

    // Taken from `actix_server::ServerWorkerConfig`.
    let max_blocking_threads = std::cmp::max(512 / number_of_workers, 1);

    std::cmp::max(max_blocking_threads, MIN_BLOCKING_THREADS_PER_WORKER)
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
        match err {
            QueryPayloadError::Deserialize(err) => ErrorResponse {
                error: "UnableToParseQueryString".to_string(),
                message: format!("Unable to parse query string: {}", err),
            }
            .into(),
            _ => {
                debug!("Unknown QueryPayloadError variant");
                ErrorResponse {
                    error: "UnknownError".to_string(),
                    message: "Unknown Error".to_string(),
                }
                .into()
            }
        }
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
pub(crate) async fn show_version_handler() -> impl actix_web::Responder {
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

#[allow(clippy::unnecessary_wraps)]
pub(crate) fn render_404(
    mut response: ServiceResponse,
) -> actix_web::Result<middleware::ErrorHandlerResponse<BoxBody>> {
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::header::HeaderValue::from_static("application/json"),
    );

    let response_json_string = serde_json::to_string(&ErrorResponse {
        error: "NotFound".to_string(),
        message: "Not Found".to_string(),
    })
    .expect("Serialization of fixed ErrorResponse must not fail");

    let response = response.map_body(|_, _| EitherBody::new(response_json_string.boxed()));

    Ok(middleware::ErrorHandlerResponse::Response(response))
}

#[allow(clippy::unnecessary_wraps)]
pub(crate) fn render_405(
    mut response: ServiceResponse,
) -> actix_web::Result<middleware::ErrorHandlerResponse<BoxBody>> {
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::header::HeaderValue::from_static("application/json"),
    );

    let response_json_string = serde_json::to_string(&ErrorResponse {
        error: "MethodNotAllowed".to_string(),
        message: "HTTP method not allowed.".to_string(),
    })
    .expect("Serialization of fixed ErrorResponse must not fail");

    let response = response.map_body(|_, _| EitherBody::new(response_json_string.boxed()));

    Ok(middleware::ErrorHandlerResponse::Response(response))
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
