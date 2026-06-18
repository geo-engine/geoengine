use std::env;
use std::{fmt::Display, str::FromStr};

use gdal::raster::GdalType;
use geoengine_datatypes::raster::Pixel;
use geoengine_operators::source::{
    GdalDatasetCache, IpcChannelMessage, IpcChannelMessagePayload, IpcProcessError,
    IpcProcessRasterResult,
    gdal_source::{GdalRasterLoader, process::GdalIpcBytePayload},
    setup_client,
};
use ipc_channel::ipc::IpcSender;
use num::FromPrimitive;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;

use tracing::Level;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::prelude::*;

fn exit_with_error(msg: impl Display) -> ! {
    tracing::error!("Error: {msg}");
    std::process::exit(1);
}

type Token = String;

/// Parses command-line arguments.
///
/// Usage: `gdalsource-process <token> [<debug_level>] [<otlp_endpoint>]`
///
/// - `token`: IPC one-shot server token required for parent-child handshake.
/// - `debug_level`: Optional. Logging level (trace, debug, info, warn, error). Defaults to "info".
/// - `otlp_endpoint`: Optional. OpenTelemetry OTLP/gRPC endpoint, e.g. `http://localhost:4317`.
///   When provided, the subprocess initializes OpenTelemetry tracing connected to this endpoint
///   and attaches its spans as children of the parent trace context received via IPC messages.
fn setup() -> (Token, Level, Option<String>) {
    let args: Vec<String> = std::env::args().collect();
    match args.as_slice() {
        [_bin, token] => (token.clone(), Level::INFO, None),
        [_bin, token, debug_level] => (
            token.clone(),
            Level::from_str(debug_level).unwrap_or(Level::DEBUG),
            None,
        ),
        [_bin, token, debug_level, otlp_endpoint] => (
            token.clone(),
            Level::from_str(debug_level).unwrap_or(Level::DEBUG),
            Some(otlp_endpoint.clone()),
        ),
        _ => {
            panic!("Usage: gdalsource-process <token> [<debug_level>] [<otlp_endpoint>]")
        }
    }
}

/// Holds the OpenTelemetry tracer provider and an optional tokio runtime
/// needed by the OTLP gRPC exporter (tonic requires an active tokio runtime).
struct TelemetryGuard {
    provider: SdkTracerProvider,
    _rt: Option<tokio::runtime::Runtime>,
}

/// Builds a target-based filter that shows high-level GDAL worker logs
/// while suppressing internal noise from OpenTelemetry / gRPC libraries.
fn make_fmt_filter(level: Level) -> Targets {
    Targets::new()
        // Our custom spans — show at the configured level (typically DEBUG)
        .with_target("gdalsource-process", level)
        // GDAL C-library error handler — show INFO+
        .with_target("GDAL", Level::INFO)
        // Library code running inside the subprocess (read timing etc.)
        .with_target("geoengine_operators::source::gdal_source", Level::DEBUG)
        .with_target("geoengine_datatypes", Level::DEBUG)
        // Squash internal OTel/h2/tonic debug noise
        .with_target("opentelemetry_sdk", Level::WARN)
        .with_target("opentelemetry_otlp", Level::WARN)
        .with_target("h2", Level::WARN)
        .with_target("hyper_util", Level::WARN)
        .with_target("tonic", Level::WARN)
        .with_target("rustls", Level::WARN)
        .with_target("want", Level::WARN)
        .with_target("tokio_util", Level::WARN)
        .with_target("tokio_cron_scheduler", Level::WARN)
        // Everything else at WARN by default
        .with_default(Level::WARN)
}

/// Filter for the OpenTelemetry layer: only export our high-level spans.
fn make_otel_filter(level: Level) -> Targets {
    Targets::new()
        // Our custom GDAL worker spans
        .with_target("gdalsource-process", level)
        // GDAL C-library errors / warnings
        .with_target("GDAL", Level::INFO)
        // Library timing events (e.g. "read raster band in x s") as span logs
        .with_target("geoengine_operators::source::gdal_source", Level::DEBUG)
    // No `with_default` → default is OFF, nothing else goes to Jaeger
}

/// Initialises logging and OpenTelemetry tracing.
///
/// Falls back to the `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable when
/// `otlp_endpoint` is `None`, so children of a parent process that exports
/// traces via the standard OTLP env-var inherit the endpoint automatically.
///
/// The subprocess is a synchronous (std-thread) process, but `opentelemetry-otlp`
/// with `grpc-tonic` requires a Tokio runtime. We create one internally and keep
/// it alive for the lifetime of the `TelemetryGuard`.
///
/// Returns `None` when no endpoint is configured **or** when the OTLP exporter
/// fails to build (the process continues without tracing).
fn init_telemetry(level: Level, otlp_endpoint: Option<&str>) -> Option<TelemetryGuard> {
    // Check CLI arg first, then env var
    let endpoint = otlp_endpoint
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
        .filter(|s| !s.is_empty())?; // No endpoint → no tracing

    // Build a multi-threaded tokio runtime for the OTLP gRPC exporter.
    // The subprocess is fully synchronous, but tonic needs a runtime to
    // establish & maintain the gRPC connection to Jaeger. A multi-thread
    // runtime keeps background gRPC tasks alive without manual driving.
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
    {
        Ok(rt) => Some(rt),
        Err(e) => {
            tracing::warn!("Failed to create tokio runtime for OTLP exporter: {e}");
            // Initialise the subscriber with just the fmt layer
            let fmt_filter = make_fmt_filter(level);
            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_target(true)
                .with_ansi(false)
                .with_filter(fmt_filter);
            tracing_subscriber::registry().with(fmt_layer).init();
            return None;
        }
    };

    // Keep the runtime entered during initialisation so tonic can spawn
    // internal tasks for the gRPC connection.
    let _rt_guard = rt.as_ref().map(|r| r.enter());

    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
    {
        Ok(exporter) => exporter,
        Err(e) => {
            tracing::warn!("Failed to build OTLP span exporter: {e}. Tracing disabled.");
            let fmt_filter = make_fmt_filter(level);
            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_target(true)
                .with_ansi(false)
                .with_filter(fmt_filter);
            tracing_subscriber::registry().with(fmt_layer).init();
            return None;
        }
    };

    // Register the W3C Trace Context propagator globally so that
    // `global::get_text_map_propagator` returns a working propagator
    // that can inject/extract the `traceparent` header for IPC context propagation.
    global::set_text_map_propagator(TraceContextPropagator::new());

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder_empty()
                .with_attribute(opentelemetry::KeyValue::new(
                    "service.name",
                    "Geo Engine GDAL Worker",
                ))
                .build(),
        )
        .build();

    let tracer = provider.tracer("Geo Engine GDAL Worker");
    let otel_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(make_otel_filter(level));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(true)
        .with_ansi(false)
        .with_filter(make_fmt_filter(level));

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    Some(TelemetryGuard { provider, _rt: rt })
}

/// We install a GDAL error handler that logs all messages with our log macros.
fn reroute_gdal_logging() {
    gdal::config::set_error_handler(|error_type, error_num, error_msg| {
        const LOG_TARGET: &str = "GDAL";
        match error_type {
            gdal::errors::CplErrType::None => {
                // should never log anything
                tracing::info!(target: LOG_TARGET, "GDAL None {error_num}: {error_msg}");
            }
            gdal::errors::CplErrType::Debug => {
                tracing::info!(target: LOG_TARGET, "GDAL Debug {error_num}: {error_msg}");
            }
            gdal::errors::CplErrType::Warning => {
                tracing::warn!(target: LOG_TARGET, "GDAL Warning {error_num}: {error_msg}");
            }
            gdal::errors::CplErrType::Failure => {
                tracing::error!(target: LOG_TARGET, "GDAL Failure {error_num}: {error_msg}");
            }
            gdal::errors::CplErrType::Fatal => {
                tracing::error!(target: LOG_TARGET, "GDAL Fatal {error_num}: {error_msg}");
            }
        }
    });
}

fn main() {
    let (token, debug_lvl, otlp_endpoint) = setup();
    let _telemetry = init_telemetry(debug_lvl, otlp_endpoint.as_deref());

    // Keep the tokio runtime entered for the entire lifetime of the process.
    // The `SimpleSpanProcessor` calls `block_on` synchronously when a span
    // ends (via tracing-opentelemetry). Without an active runtime context
    // the tonic gRPC call inside the OTLP exporter would hang.
    let _rt_guard = _telemetry
        .as_ref()
        .and_then(|t| t._rt.as_ref().map(tokio::runtime::Runtime::enter));

    reroute_gdal_logging();
    run(token);

    // Drop the enter-guard FIRST, then flush & shut down the provider.
    drop(_rt_guard);

    if let Some(guard) = _telemetry {
        if let Err(err) = guard.provider.shutdown() {
            tracing::warn!("Error shutting down OpenTelemetry tracer provider: {err}");
        }
    }
}

fn raster_type_dispatch(
    payload: IpcChannelMessagePayload,
    dataset_cache: &mut GdalDatasetCache,
    sender: &IpcSender<IpcProcessRasterResult>,
) -> Result<(), IpcProcessError> {
    match payload.data_type {
        geoengine_datatypes::raster::RasterDataType::U8 => {
            read_and_send::<u8>(payload, dataset_cache, sender)?;
        }
        geoengine_datatypes::raster::RasterDataType::U16 => {
            read_and_send::<u16>(payload, dataset_cache, sender)?;
        }
        geoengine_datatypes::raster::RasterDataType::U32 => {
            read_and_send::<u32>(payload, dataset_cache, sender)?;
        }
        geoengine_datatypes::raster::RasterDataType::U64 => {
            read_and_send::<u64>(payload, dataset_cache, sender)?;
        }
        geoengine_datatypes::raster::RasterDataType::I8 => {
            read_and_send::<i8>(payload, dataset_cache, sender)?;
        }
        geoengine_datatypes::raster::RasterDataType::I16 => {
            read_and_send::<i16>(payload, dataset_cache, sender)?;
        }
        geoengine_datatypes::raster::RasterDataType::I32 => {
            read_and_send::<i32>(payload, dataset_cache, sender)?;
        }
        geoengine_datatypes::raster::RasterDataType::I64 => {
            read_and_send::<i64>(payload, dataset_cache, sender)?;
        }
        geoengine_datatypes::raster::RasterDataType::F32 => {
            read_and_send::<f32>(payload, dataset_cache, sender)?;
        }
        geoengine_datatypes::raster::RasterDataType::F64 => {
            read_and_send::<f64>(payload, dataset_cache, sender)?;
        }
    }

    Ok(())
}

#[allow(clippy::print_stderr)]
fn run(token: Token) {
    let (sender, receiver) = match setup_client::<IpcChannelMessage, IpcProcessRasterResult>(token)
    {
        Ok(pair) => pair,
        Err(err) => exit_with_error(err),
    };

    let mut dataset_cache = GdalDatasetCache::new();

    // Loop runs indefinitely, reusing the process and its GDAL dataset cache
    while let Ok(message) = receiver.recv() {
        let IpcChannelMessage(payload) = message;

        // --- Extract and attach parent tracing context ---
        // The parent process (or an upstream span) injected its OpenTelemetry
        // context into `span_context` using the W3C Trace Context propagator.
        // We restore it here so any tracing spans we create become proper
        // children of that parent in Jaeger.
        let parent_cx = if !payload.span_context.is_empty() {
            global::get_text_map_propagator(|propagator| {
                propagator
                    .extract_with_context(&opentelemetry::Context::new(), &payload.span_context)
            })
        } else {
            opentelemetry::Context::new()
        };

        // Create a root tracing span that inherits the parent trace context.
        // The `tracing-opentelemetry` bridge will translate this into an
        // OpenTelemetry span whose parent is correctly set.
        let root_span = tracing::info_span!(
            target: "gdalsource-process",
            "gdal.request",
            file_path = %payload.dataset_params.file_path.display(),
            band = payload.dataset_params.rasterband_channel,
        );
        let _ = root_span.set_parent(parent_cx);
        let _guard = root_span.enter();

        // If helper returns an error, it means the underlying IPC channel is completely broken
        if let Err(err) = raster_type_dispatch(payload, &mut dataset_cache, &sender) {
            tracing::error!("Fatal IPC channel error: {err:?}. Exiting worker thread.");
            break;
        }
    }
}

fn read_and_send<T: GdalType + Pixel + FromPrimitive>(
    IpcChannelMessagePayload {
        dataset_params,
        read_advise,
        data_type: _,
        span_context: _,
    }: IpcChannelMessagePayload,
    dataset_cache: &mut GdalDatasetCache,
    sender: &IpcSender<IpcProcessRasterResult>,
) -> Result<(), IpcProcessError> {
    // Span for the dataset open + read operation.
    // The parent is the `gdal.request` span created in `run()`.
    let read_span = tracing::debug_span!(
        target: "gdalsource-process",
        "gdal.read",
        file_path = %dataset_params.file_path.display(),
        band = dataset_params.rasterband_channel,
    );
    let _read_guard = read_span.enter();

    let gp = GdalRasterLoader::load_tile_data_with_dataset_retry::<T>(
        dataset_cache,
        &dataset_params,
        read_advise,
    )
    .map_err(IpcProcessError::from);

    // Drop the read span guard before sending the result
    drop(_read_guard);

    let byte_payload = gp.and_then(|p| GdalIpcBytePayload::try_from(p).map_err(Into::into));
    // Propagate channel send errors directly up out of the handler
    match byte_payload {
        Ok(td) => sender.send(Ok(td)),
        Err(err) => sender.send(Err(err)),
    }?;

    Ok(())
}
