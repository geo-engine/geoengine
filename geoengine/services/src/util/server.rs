use crate::{api::model::responses::ErrorResponse, config::get_config_element, error::Result};
use actix_http::{
    Extensions, HttpMessage, StatusCode,
    body::{BoxBody, EitherBody, MessageBody},
    header::{HeaderName, HeaderValue},
    uri::PathAndQuery,
};
use actix_web::{
    HttpRequest, HttpResponse,
    dev::{ServiceFactory, ServiceRequest, ServiceResponse},
    error::{InternalError, JsonPayloadError, QueryPayloadError},
    http, middleware, web,
};
use futures::future::BoxFuture;
use geoengine_datatypes::primitives::CacheHint;
use std::{any::Any, fmt::Write, num::NonZeroUsize, time::Duration};
use tracing::{Span, debug, info};
use tracing_actix_web::{RequestId, RootSpanBuilder};
use url::Url;
use utoipa::{ToSchema, openapi::OpenApi};

/// Custom root span for web requests that paste a request id to all logs.
pub struct CustomRootSpanBuilder;

impl RootSpanBuilder for CustomRootSpanBuilder {
    fn on_request_start(request: &ServiceRequest) -> Span {
        // TODO: rethink this error handling
        let request_id = request
            .extensions()
            .get::<RequestId>()
            .copied()
            .expect("it should not run without the `RequestId` extension");

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
    let number_of_workers = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);

    // Taken from `actix_server::ServerWorkerConfig`.
    let max_blocking_threads = std::cmp::max(512 / number_of_workers, 1);

    std::cmp::max(max_blocking_threads, MIN_BLOCKING_THREADS_PER_WORKER)
}

pub(crate) fn configure_extractors(cfg: &mut web::ServiceConfig) {
    cfg.app_data(
        web::JsonConfig::default().error_handler(|err, _req| handle_json_payload_error(err)),
    );
    cfg.app_data(web::QueryConfig::default().error_handler(|err, _req| {
        match err {
            QueryPayloadError::Deserialize(err) => ErrorResponse {
                error: "UnableToParseQueryString".to_string(),
                message: format!("Unable to parse query string: {err}"),
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

pub fn handle_json_payload_error(err: actix_web::error::JsonPayloadError) -> actix_web::Error {
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
                message: format!("JSON payload has exceeded limit ({limit} bytes)."),
            }),
        )
        .into(),
        JsonPayloadError::OverflowKnownLength { length, limit } => InternalError::from_response(
            err,
            HttpResponse::PayloadTooLarge().json(ErrorResponse {
                error: "Overflow".to_string(),
                message: format!(
                    "JSON payload ({length} bytes) is larger than allowed (limit: {limit} bytes)."
                ),
            }),
        )
        .into(),
        JsonPayloadError::Payload(err) => ErrorResponse {
            error: "Payload".to_string(),
            message: err.to_string(),
        }
        .into(),
        JsonPayloadError::Deserialize(err) => ErrorResponse {
            error: "BodyDeserializeError".to_string(),
            message: format!("Error in user input: {err}"),
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
}

#[derive(serde::Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ServerInfo {
    pub(crate) build_date: &'static str,
    pub(crate) commit_hash: &'static str,
    pub(crate) version: &'static str,
    pub(crate) features: &'static str,
}

/// Shows information about the server software version.
#[utoipa::path(
    tag = "General",
    get,
    path = "/info",
    responses(
        (status = 200, description = "Server software information", body = ServerInfo,
            example = json!({
                "buildDate": "2022-09-29",
                "commitHash": "555dc6d84d3682c37490a145d53c5097d0b81b27",
                "version": "0.7.0",
                "features": "default"
              }))
    )
)]
#[allow(clippy::unused_async)] // the function signature of request handlers requires it
pub(crate) async fn server_info_handler() -> impl actix_web::Responder {
    web::Json(server_info())
}

pub(crate) fn server_info() -> ServerInfo {
    ServerInfo {
        build_date: env!("VERGEN_BUILD_DATE"),
        commit_hash: option_env!("VERGEN_GIT_SHA").unwrap_or("unknown"), // 'unknown' if not builded in git repository
        version: env!("CARGO_PKG_VERSION"),
        features: env!("VERGEN_CARGO_FEATURES"),
    }
}

/// Server availablity check.
#[utoipa::path(
    tag = "General",
    get,
    path = "/available",
    responses(
        (status = 204, description = "Server availablity check")
    )
)]
#[allow(clippy::unused_async)] // the function signature of request handlers requires it
pub(crate) async fn available_handler() -> impl actix_web::Responder {
    HttpResponse::Ok().status(StatusCode::NO_CONTENT).finish()
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

// this is a workaround to be able to serve swagger UI and the openapi.json behind a proxy (/api)
// TODO: remove this when utoipa allows configuring the paths to serve the openapi.json and to include it in the swagger UI separately
pub fn serve_openapi_json<
    T: ServiceFactory<ServiceRequest, Config = (), Error = actix_web::Error, InitError = ()>,
>(
    app: actix_web::Scope<T>,
    api_urls: &mut Vec<(utoipa_swagger_ui::Url, OpenApi)>,
    name: &'static str,
    ui_url: &'static str,
    serve_url: &str,
    openapi: OpenApi,
) -> actix_web::Scope<T> {
    api_urls.push((utoipa_swagger_ui::Url::new(name, ui_url), openapi.clone()));
    app.route(
        serve_url,
        web::get().to(move || {
            let openapi = openapi.clone();
            async move { web::Json(openapi) }
        }),
    )
}

pub(crate) fn log_server_info() -> Result<()> {
    fn enabled_disabled(enabled: bool) -> &'static str {
        if enabled { "enabled" } else { "disabled" }
    }

    let cache_config: crate::config::Cache = get_config_element()?;
    let oidc_config: crate::config::Oidc = get_config_element()?;
    let open_telemetry: crate::config::OpenTelemetry = get_config_element()?;
    let postgres_config: crate::config::Postgres = get_config_element()?;
    let quota_config: crate::config::Quota = get_config_element()?;
    let session_config: crate::config::Session = get_config_element()?;
    let user_config: crate::config::User = get_config_element()?;
    let web_config: crate::config::Web = get_config_element()?;

    let external_address = web_config.api_url()?;
    let local_address = Url::parse(&format!(
        "http://{}{}",
        web_config.bind_address, web_config.api_prefix
    ))?;
    let swagger_url = external_address.join("swagger-ui/")?;
    let version = server_info();

    let mut cache_enabled = enabled_disabled(cache_config.enabled).to_string();
    if cache_config.enabled {
        let _ = write!(cache_enabled, " ({} MB)", cache_config.size_in_mb);
    }

    info!(
        "Version" = %version.version,
        "Commit" = %version.commit_hash,
        "Build Date" = %version.build_date,
        "Build Features" = Some(version.features).filter(|s| !s.trim().is_empty()),
        "Local Address" = %local_address,
        "External Address" = %external_address,
        "Swagger URL" = %swagger_url,
        "Clear DB on Start" = postgres_config.clear_database_on_start,
        "Anonymous Access" = enabled_disabled(session_config.anonymous_access),
        "User Registration" = enabled_disabled(user_config.registration),
        "OIDC" = enabled_disabled(oidc_config.enabled),
        "OpenTelemetry Tracing" = enabled_disabled(open_telemetry.enabled),
        "OpenTelemetry Tracing Endpoint" = open_telemetry.enabled.then_some(open_telemetry.endpoint.as_str()),
        "Cache" = enabled_disabled(cache_config.enabled),
        "Cache Size" = cache_config.enabled.then(|| format!("{} MB", cache_config.size_in_mb)),
        "Quota Tracking" = ?quota_config.mode,
        "Server starting with configuration",
    );

    Ok(())
}

#[allow(clippy::unused_async)]
// async is required for the request handler signature
pub async fn not_implemented_handler() -> HttpResponse {
    HttpResponse::NotImplemented().finish()
}

#[cfg(target_os = "linux")]
pub struct SocketFd {
    pub fd: std::os::unix::prelude::RawFd,
    /// Set to `false` when the fd number is re-used by a new connection; see [`connection_closed`].
    pub still_open: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// Global registry of socket fds that are currently held by connections, mapped to a flag
/// that is set to `false` as soon as the fd number is claimed by a new connection. See
/// [`connection_closed`] for the rationale.
///
/// All monitors of the same connection share one flag (stored in its connection data), since
/// HTTP/1.1 keep-alive multiplexes many requests over a single connection. Entries are only
/// overwritten when the fd number is re-used, so the map stays bounded by the fd limit.
#[cfg(target_os = "linux")]
static MONITORED_FDS: std::sync::LazyLock<
    std::sync::Mutex<
        std::collections::HashMap<
            std::os::unix::prelude::RawFd,
            std::sync::Arc<std::sync::atomic::AtomicBool>,
        >,
    >,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// attach the connection's socket file descriptor and its "still open" flag to the connection data
#[cfg(target_os = "linux")]
pub fn connection_init(connection: &dyn Any, data: &mut Extensions) {
    use actix_rt::net::TcpStream;
    use std::num::NonZeroI32;
    use std::os::unix::prelude::{AsRawFd, RawFd};

    if let Some(sock) = connection.downcast_ref::<TcpStream>() {
        let fd = sock.as_raw_fd();
        if let Ok(fd) = NonZeroI32::try_from(fd) {
            let fd = RawFd::from(fd);

            // Register the connection under its fd. If a previous, now-closed connection is
            // still registered under this fd number, the kernel re-used it for the new
            // connection. Invalidate the old flag so its monitors report the close
            // immediately instead of polling the fd of an unrelated, live connection forever
            // (which would delay cancellation).
            let still_open = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
            {
                let mut monitored = MONITORED_FDS
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(previous) = monitored.insert(fd, still_open.clone()) {
                    // the fd is re-used: the old connection is definitely gone
                    previous.store(false, std::sync::atomic::Ordering::SeqCst);
                }
            }

            data.insert(SocketFd { fd, still_open });
        }
    }
}

/// start a new task that monitors the request's socket file descriptor and tries to detect when the connection is closed.
/// The returned join handle can be awaited to get notified when the connection is closed (or, if given, when the timeout is reached).
///
/// The socket fd number may be re-used by a subsequent connection before we notice that the
/// old connection was closed. To detect this, [`connection_init`] registers every live
/// connection's fd together with a per-connection `still_open` flag in [`MONITORED_FDS`] and
/// stores that flag in the connection data. When the fd is re-used, `connection_init` flips the
/// old flag, so the monitor observes the close even though the kernel would happily report the
/// recycled fd as "still valid". Since the flag is registered when the connection is accepted,
/// this also covers the race where the fd is already re-used before this function is called:
/// the monitor then sees the flag already flipped.
#[cfg(target_os = "linux")]
pub fn connection_closed(req: &HttpRequest, timeout: Option<Duration>) -> BoxFuture<'_, ()> {
    use futures::TryFutureExt;
    use nix::errno::Errno;
    use nix::sys::socket::MsgFlags;
    use std::time::Instant;

    const CONNECTION_MONITOR_INTERVAL_SECONDS: u64 = 1;

    if let Some(socket_fd) = req.conn_data::<SocketFd>() {
        let fd = socket_fd.fd;
        let still_open = socket_fd.still_open.clone();

        // 1-byte peek buffer: `recv` fills one byte if the connection is alive, returns
        // `Ok(0)` only on a closed connection, and `EAGAIN` on an idle-but-open socket.
        let mut data = [0u8; 1];

        let handle = crate::util::spawn(async move {
            let start = Instant::now();

            // NOTE: loop while the timeout has NOT elapsed yet; exits once the timeout
            //       is reached so the caller learns about it (see function doc).
            while timeout.is_none_or(|t| start.elapsed() < t) {
                // the fd was handed back to the kernel and re-used by a new connection
                if !still_open.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }

                let r = nix::sys::socket::recv(fd, &mut data, MsgFlags::MSG_PEEK);

                match r {
                    Ok(0)
                    | Err(
                        Errno::EBADF
                        | Errno::ENOTCONN
                        | Errno::ECONNRESET
                        | Errno::ECONNABORTED
                        | Errno::ENOTSOCK
                        | Errno::EPIPE,
                    ) => {
                        // the connection seems to be closed
                        return;
                    }
                    _ => (), // the connection seems to be still valid
                }

                tokio::time::sleep(std::time::Duration::from_secs(
                    CONNECTION_MONITOR_INTERVAL_SECONDS,
                ))
                .await;
            }
        });

        // TODO: return `pending` on JoinError?
        let handle = handle.unwrap_or_else(|_| ());

        Box::pin(handle)
    } else if let Some(timeout) = timeout {
        Box::pin(tokio::time::sleep(timeout))
    } else {
        Box::pin(futures::future::pending())
    }
}

// on non-linux systems we do not monitor the connections because they would require a different implementation

#[cfg(not(target_os = "linux"))]
pub fn connection_init(_connection: &dyn Any, _data: &mut Extensions) {}

#[cfg(not(target_os = "linux"))]
pub fn connection_closed(_req: &HttpRequest, timeout: Option<Duration>) -> BoxFuture<()> {
    if let Some(timeout) = timeout {
        Box::pin(tokio::time::sleep(timeout))
    } else {
        Box::pin(futures::future::pending())
    }
}

pub trait CacheControlHeader {
    fn cache_control_header(&self) -> (HeaderName, HeaderValue);
}

impl CacheControlHeader for CacheHint {
    fn cache_control_header(&self) -> (HeaderName, HeaderValue) {
        let value = match self.expires().seconds_to_expiration() {
            // RFC 2616:
            // "To mark a response as "never expires," an origin server sends an Expires date approximately one year
            // from the time the response is sent. HTTP/1.1 servers SHOULD NOT send Expires dates more than one year in the future."
            s if s > 31_536_000 => HeaderValue::from_str("private, max-age=31536000")
                .expect("should be a valid header value according to the HTTP standard"),
            0 => HeaderValue::from_str("no-cache")
                .expect("should be a valid header value according to the HTTP standard"),
            s => HeaderValue::from_str(&format!("private, max-age={s}"))
                .expect("should be a valid header value according to the HTTP standard"),
        };

        (actix_http::header::CACHE_CONTROL, value)
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_invalidates_old_fd_flag_on_fd_reuse() {
        use std::os::unix::io::AsRawFd;

        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("should bind to a free port");
        let address = listener
            .local_addr()
            .expect("should return the listener's address");

        let client_a =
            std::net::TcpStream::connect(address).expect("should connect to the listener");
        let accepted_a = listener.accept().expect("should accept connection a").0;
        accepted_a
            .set_nonblocking(true)
            .expect("should make connection a non-blocking");
        let connection_a = tokio::net::TcpStream::from_std(accepted_a)
            .expect("should register connection a with the io driver");
        let fd_a = connection_a.as_raw_fd();

        let mut data_a = Extensions::default();
        connection_init(&connection_a, &mut data_a);
        let socket_fd_a = data_a
            .remove::<SocketFd>()
            .expect("connection_init should insert a SocketFd");
        assert!(
            socket_fd_a
                .still_open
                .load(std::sync::atomic::Ordering::SeqCst)
        );

        drop(connection_a);
        drop(client_a);

        let client_b =
            std::net::TcpStream::connect(address).expect("should connect to the listener");
        let accepted_b = listener.accept().expect("should accept connection b").0;
        accepted_b
            .set_nonblocking(true)
            .expect("should make connection b non-blocking");
        let connection_b = tokio::net::TcpStream::from_std(accepted_b)
            .expect("should register connection b with the io driver");

        // Linux allocates the lowest free fd number, so closing connection_a hands it back and the
        // new connection reuses it. The test relies on this for determinism.
        assert_eq!(
            connection_b.as_raw_fd(),
            fd_a,
            "the kernel should have reused the fd of the closed connection"
        );

        let mut data_b = Extensions::default();
        connection_init(&connection_b, &mut data_b);

        // the flag of the old connection must be invalidated so its monitor reports the close
        assert!(
            !socket_fd_a
                .still_open
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        let socket_fd_b = data_b
            .get::<SocketFd>()
            .expect("connection_init should insert a SocketFd");
        assert!(
            socket_fd_b
                .still_open
                .load(std::sync::atomic::Ordering::SeqCst)
        );

        drop(client_b);
    }
}
