use std::{collections::HashMap, fmt::Display, net::SocketAddr, path::PathBuf};

use config::{Config, Environment, File};
use gdal::raster::GdalType;
use geoengine_datatypes::raster::Pixel;
use geoengine_operators::source::gdal_worker_process::{
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
use serde::Deserialize;
use tracing::Subscriber;
use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::{
    EnvFilter,
    fmt::{
        FormatFields,
        format::{DefaultFields, Writer},
    },
    layer::{Layer, SubscriberExt},
    util::SubscriberInitExt,
};

fn exit_with_error(msg: impl Display) -> ! {
    tracing::error!("Error: {msg}");
    std::process::exit(1);
}

type Token = String;

fn setup() -> Token {
    let args: Vec<String> = std::env::args().collect();
    match args.as_slice() {
        [_bin, token] => token.clone(),
        _ => {
            panic!("Usage: gdalsource-process <token>")
        }
    }
}

#[derive(Debug, Deserialize)]
struct WorkerLoggingConfig {
    pub log_spec: String,
    #[serde(default)]
    pub stderr_log_spec: String,
    #[serde(default)]
    pub log_to_file: bool,
    #[serde(default = "default_worker_log_prefix")]
    pub filename_prefix: String,
    pub log_directory: Option<String>,
}

fn default_worker_log_prefix() -> String {
    "geo_engine_worker".to_string()
}

impl Default for WorkerLoggingConfig {
    fn default() -> Self {
        Self {
            log_spec: "info".to_string(),
            stderr_log_spec: "info".to_string(),
            log_to_file: false,
            filename_prefix: default_worker_log_prefix(),
            log_directory: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct OpenTelemetryConfig {
    enabled: bool,
    endpoint: SocketAddr,
}

#[derive(Debug, Deserialize, Default)]
struct GdalProcessPoolWorkerConfig {
    #[serde(default)]
    gdal_config_options: HashMap<String, String>,
    #[serde(default)]
    logging: WorkerLoggingConfig,
}

/// Locate the directory that contains `Settings-default.toml`, walking up from the current
/// working directory. This matches the behaviour of the main Geo Engine process and is needed
/// because the worker may be spawned from a subdirectory (e.g. during `cargo test`).
fn retrieve_settings_dir() -> PathBuf {
    const MAX_PARENT_DIRS: usize = 5;

    let mut settings_dir =
        std::env::current_dir().expect("working directory must exist for GDAL worker");

    for _ in 0..=MAX_PARENT_DIRS {
        if settings_dir.join("Settings-default.toml").exists() {
            return settings_dir;
        }

        if !settings_dir.pop() {
            break;
        }
    }

    panic!(
        "Settings-default.toml not found in current directory or any parent up to {MAX_PARENT_DIRS} levels"
    )
}

fn load_settings() -> Config {
    let dir = retrieve_settings_dir();

    let files = ["Settings-default.toml", "Settings.toml"];
    let file_sources: Vec<File<_, _>> = files
        .iter()
        .map(|f| dir.join(f))
        .filter(|p| p.exists())
        .map(File::from)
        .collect();

    Config::builder()
        .add_source(file_sources)
        .add_source(Environment::with_prefix("geoengine").separator("__"))
        .build()
        .unwrap_or_else(|err| panic!("Failed to load GDAL worker settings: {err}"))
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
///
/// Some VSICURL-related options (e.g. `VSI_CACHE_SIZE`, `CPL_VSIL_CURL_CHUNK_SIZE`) are frozen by
/// GDAL after the first `/vsicurl/` access: the cache pool is allocated once and there is no public
/// API to resize it at runtime. Changing those options therefore requires restarting the worker
/// process so the new values are read from the settings file on startup.
///
/// Per-open options such as `GDAL_DISABLE_READDIR_ON_OPEN` or
/// `CPL_VSIL_CURL_ALLOWED_EXTENSIONS` can additionally be overridden per request via
/// `GdalDatasetParameters.gdal_config_options`.
fn set_gdal_process_global_options(options: &HashMap<String, String>) {
    for (key, value) in options {
        if let Err(err) = gdal::config::set_config_option(key, value) {
            eprintln!("Failed to set GDAL config option {key}={value}: {err}");
        }
    }
}

/// Initializes a tracing subscriber. Writes to stderr (filtered by `stderr_log_spec`) and,
/// if enabled, to a per-worker rolling log file (filtered by `log_spec`). The filename includes
/// the worker's IPC token so multiple workers do not corrupt a shared file.
///
/// When `[open_telemetry]` is enabled, an OpenTelemetry OTLP layer is added and the corresponding
/// tracer provider is returned so it can be flushed on shutdown.
///
/// # Panics
/// Panics if `log_spec` or `stderr_log_spec` cannot be parsed as an `EnvFilter`.
fn init_subscriber(
    logging_config: &WorkerLoggingConfig,
    open_telemetry_config: &OpenTelemetryConfig,
    token: &str,
) -> (Option<SdkTracerProvider>, Option<WorkerGuard>) {
    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(EnvFilter::new(&logging_config.stderr_log_spec));

    let (file_layer, file_guard) = if logging_config.log_to_file {
        let (layer, guard) = file_layer_with_filter(logging_config, token);
        (Some(layer), Some(guard))
    } else {
        (None, None)
    };

    if open_telemetry_config.enabled {
        let endpoint = open_telemetry_config.endpoint.to_string();
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
                    .with_attribute(opentelemetry::KeyValue::new(
                        "service.instance.id",
                        token.to_string(),
                    ))
                    .build(),
            )
            .build();

        let opentelemetry =
            tracing_opentelemetry::layer().with_tracer(provider.tracer("gdal-worker"));

        tracing_subscriber::registry()
            .with(stderr_layer)
            .with(file_layer)
            .with(opentelemetry)
            .init();

        return (Some(provider), file_guard);
    }

    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(file_layer)
        .init();
    (None, file_guard)
}

// We use a custom formatter because there are still format flags within spans even when
// `with_ansi` is false due to bug: https://github.com/tokio-rs/tracing/issues/1817
struct FileFormatterWorkaround(DefaultFields);

impl<'writer> FormatFields<'writer> for FileFormatterWorkaround {
    fn format_fields<R: tracing_subscriber::field::RecordFields>(
        &self,
        writer: Writer<'writer>,
        fields: R,
    ) -> core::fmt::Result {
        self.0.format_fields(writer, fields)
    }
}

fn file_layer_with_filter<S>(
    logging_config: &WorkerLoggingConfig,
    token: &str,
) -> (impl Layer<S> + use<S>, WorkerGuard)
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    // ponytail: short retention because number_of_processes workers each produce rotated files.
    let file_appender = RollingFileAppender::builder()
        .max_log_files(3)
        .filename_prefix(format!("{}-{}", logging_config.filename_prefix, token))
        .filename_suffix("log")
        .rotation(Rotation::DAILY)
        .build(logging_config.log_directory.as_deref().unwrap_or("./"))
        .expect("failed to initialize rolling file appender");

    let (non_blocking_writer, guard) = tracing_appender::non_blocking(file_appender);

    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .fmt_fields(FileFormatterWorkaround(DefaultFields::default()))
        .with_writer(non_blocking_writer)
        .with_filter(EnvFilter::new(&logging_config.log_spec));

    (layer, guard)
}

fn main() {
    let token = setup();

    let settings = load_settings();
    let logging_config: WorkerLoggingConfig = settings
        .get::<GdalProcessPoolWorkerConfig>("gdal_process_pool_worker")
        .unwrap_or_default()
        .logging;
    let open_telemetry_config: OpenTelemetryConfig = settings
        .get("open_telemetry")
        .expect("open_telemetry config must be present");
    let gdal_worker_config: GdalProcessPoolWorkerConfig =
        settings.get("gdal_process_pool_worker").unwrap_or_default();

    // The OTLP exporter needs a tokio runtime. We keep the worker's main loop synchronous and
    // dedicate a single background thread to the runtime that drives the exporter.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime for telemetry");

    let _runtime_guard = runtime.enter();
    let (provider, _file_guard) = init_subscriber(&logging_config, &open_telemetry_config, &token);

    set_gdal_process_global_options(&gdal_worker_config.gdal_config_options);
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
