use crate::error::Result;
use crate::handlers::ErrorResponse;

use actix_http::body::{BoxBody, EitherBody, MessageBody};
use actix_http::uri::PathAndQuery;
use actix_http::HttpMessage;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::error::{InternalError, JsonPayloadError, QueryPayloadError};
use actix_web::{http, middleware, web, HttpResponse};
use log::debug;
use std::num::NonZeroUsize;
use tracing::Span;
use tracing_actix_web::{RequestId, RootSpanBuilder};
use utoipa::ToSchema;

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

#[derive(serde::Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct VersionInfo {
    build_date: Option<String>,
    commit_hash: Option<String>,
}

/// Shows information about the server software version.
#[utoipa::path(
    tag = "General",
    get,
    path = "/version",
    responses(
        (status = 200, description = "Server software information", body = VersionInfo,
            example = json!({"buildDate": "2021-05-17", "commitHash": "16cd0881a79b6f03bb5f1f6ef2b2711e570b9865"}))
    )
)]
#[allow(clippy::unused_async)] // the function signature of request handlers requires it
pub(crate) async fn show_version_handler() -> impl actix_web::Responder {
    use std::string::ToString;
    web::Json(VersionInfo {
        build_date: option_env!("VERGEN_BUILD_DATE").map(ToString::to_string),
        commit_hash: option_env!("VERGEN_GIT_SHA").map(ToString::to_string),
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
