use std::{fmt::Display, str::FromStr};

use gdal::raster::GdalType;
use geoengine_datatypes::raster::Pixel;
use geoengine_operators::source::gdal_worker_process::{
    GdalDatasetParameters,
    process_common::{
        GdalIpcBytePayload, IpcChannelMessage, IpcChannelMessagePayload, IpcProcessError,
        IpcProcessRasterResult,
    },
    process_impl::{GdalDatasetHolder, GdalHandling, setup_client},
};
use ipc_channel::ipc::IpcSender;
use num::FromPrimitive;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

fn exit_with_error(msg: impl Display) -> ! {
    tracing::error!("Error: {msg}");
    std::process::exit(1);
}

/// placeholder for logging implementation
#[allow(clippy::upper_case_acronyms)]
enum Level {
    INFO,
    DEBUG,
}

impl FromStr for Level {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Info" | "info" => Ok(Level::INFO),
            "Debug" | "debug" => Ok(Level::DEBUG),
            _ => Err("unknown level".to_string()),
        }
    }
}

type Token = String;

fn setup() -> (Token, Level) {
    let args: Vec<String> = std::env::args().collect();
    match args.as_slice() {
        [_bin, token] => (token.clone(), Level::INFO),
        [_bin, token, debug_level] => (
            token.clone(),
            Level::from_str(debug_level).unwrap_or(Level::DEBUG),
        ),
        _ => {
            panic!("Usage: gdalprocess-ipc-channel-server <token> <debug>")
        }
    }
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
                tracing::debug!(target: LOG_TARGET, "GDAL Debug {error_num}: {error_msg}");
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

/// Configure GDAL process-global options once before any dataset is opened.
/// Options like VSICURL cache sizes are only read by GDAL at first use, so they must be set here
/// rather than per-request.
fn set_gdal_process_global_options() {
    for (key, value) in GdalDatasetParameters::gdal_worker_process_global_config_options() {
        if let Err(err) = gdal::config::set_config_option(key, value) {
            eprintln!("Failed to set GDAL config option {key}={value}: {err}");
        }
    }
}

/// Initializes a tracing subscriber. When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, an OpenTelemetry
/// OTLP layer is added and the corresponding tracer provider is returned so it can be flushed on
/// shutdown. Otherwise only a stderr fmt layer is installed.
fn init_subscriber() -> Option<SdkTracerProvider> {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(EnvFilter::from_default_env());

    if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        if !endpoint.is_empty() {
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
                .unwrap_or_else(|err| {
                    eprintln!("Failed to build OTLP exporter: {err}. Continuing without OTLP.");
                    std::process::exit(1);
                });

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

            let opentelemetry =
                tracing_opentelemetry::layer().with_tracer(provider.tracer("gdal-worker"));

            tracing_subscriber::registry()
                .with(fmt_layer)
                .with(opentelemetry)
                .init();

            return Some(provider);
        }
    }

    tracing_subscriber::registry().with(fmt_layer).init();
    None
}

fn main() {
    let (token, _debug_lvl) = setup();
    set_gdal_process_global_options();

    // The OTLP exporter needs a tokio runtime. We keep the worker's main loop synchronous and
    // dedicate a single background thread to the runtime that drives the exporter.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime for telemetry");

    let _runtime_guard = runtime.enter();
    let provider = init_subscriber();

    reroute_gdal_logging();
    run(token);

    drop(_runtime_guard);
    if let Some(provider) = provider {
        if let Err(err) = provider.shutdown() {
            eprintln!("Failed to flush OpenTelemetry provider: {err}");
        }
    }
}

fn raster_type_dispatch(
    payload: IpcChannelMessagePayload,
    dataset_cache: &mut GdalDatasetHolder,
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

    let mut dataset_cache = GdalDatasetHolder::new();

    // Loop runs indefinitely, reusing the process and its GDAL dataset cache
    while let Ok(message) = receiver.recv() {
        let payload = message.0;

        // If helper returns an error, it means the underlying IPC channel is completely broken
        if let Err(err) = raster_type_dispatch(payload, &mut dataset_cache, &sender) {
            eprintln!("Fatal IPC channel error: {err:?}. Exiting worker thread.");
            break;
        }
    }
}

fn read_and_send<T: GdalType + Pixel + FromPrimitive>(
    IpcChannelMessagePayload {
        dataset_params,
        read_advise,
        data_type: _,
    }: IpcChannelMessagePayload,
    dataset_cache: &mut GdalDatasetHolder,
    sender: &IpcSender<IpcProcessRasterResult>,
) -> Result<(), IpcProcessError> {
    let gp = GdalHandling::load_tile_data_with_dataset_retry::<T>(
        dataset_cache,
        &dataset_params,
        read_advise,
    );

    let byte_payload = gp.and_then(|p| GdalIpcBytePayload::try_from(p).map_err(Into::into));
    // Propagate channel send errors directly up out of the handler
    match byte_payload {
        Ok(td) => sender.send(Ok(td)),
        Err(err) => sender.send(Err(err)),
    }?;

    Ok(())
}
