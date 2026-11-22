use std::fmt::Display;
use std::num::NonZeroUsize;

use gdal::raster::GdalType;
use geoengine_datatypes::raster::{GridBounds, Pixel};
use geoengine_operators::source::gdal_worker_process::{
    GdalDatasetParameters, OpenTelemetryConfig, WorkerConfig, WorkerLoggingConfig, byte_cache,
    process_common::{
        GdalIpcBytePayload, GdalReadAdvise, GdalRequest, IpcChannelMessagePayload,
        IpcChannelResult, IpcProcessError, IpcProcessRasterResult, WorkerRequest, WorkerResponse,
    },
    process_impl::{GdalDatasetHolder, GdalHandling, setup_client},
};
use ipc_channel::ipc::IpcSender;
use num::FromPrimitive;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
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

fn setup() -> (Token, WorkerConfig) {
    let args: Vec<String> = std::env::args().collect();
    match args.as_slice() {
        [_bin, token, config_json] => {
            let config: WorkerConfig = serde_json::from_str(config_json)
                .unwrap_or_else(|e| panic!("Failed to parse worker config JSON: {e}"));
            (token.clone(), config)
        }
        _ => {
            panic!("Usage: gdalsource-process <token> <config-json>")
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
///
/// Some VSICURL-related options (e.g. `VSI_CACHE_SIZE`, `CPL_VSIL_CURL_CHUNK_SIZE`) are frozen by
/// GDAL after the first `/vsicurl/` access: the cache pool is allocated once and there is no public
/// API to resize it at runtime. Changing those options therefore requires restarting the worker
/// process so the new values are passed via the config at spawn time.
///
/// Per-open options such as `GDAL_DISABLE_READDIR_ON_OPEN` or
/// `CPL_VSIL_CURL_ALLOWED_EXTENSIONS` can additionally be overridden per request via
/// `GdalDatasetParameters.gdal_config_options`.
fn set_gdal_process_global_options(options: &[(String, String)]) {
    for (key, value) in options {
        if let Err(err) = gdal::config::set_config_option(key, value) {
            tracing::warn!("Failed to set GDAL config option {key}={value}: {err}");
        }
    }
}

/// Initializes a tracing subscriber. Writes to stderr (filtered by `log_spec`) and,
/// if enabled, to a per-worker rolling log file (filtered by `log_spec`). The filename includes
/// the worker's IPC token so multiple workers do not corrupt a shared file.
///
/// When `open_telemetry` is enabled, an OpenTelemetry OTLP layer is added and the corresponding
/// tracer provider is returned so it can be flushed on shutdown.
///
/// # Panics
/// Panics if `log_spec` cannot be parsed as an `EnvFilter`.
#[allow(clippy::print_stderr)] // subscriber not yet initialized
fn init_subscriber(
    logging_config: &WorkerLoggingConfig,
    open_telemetry_config: &OpenTelemetryConfig,
    token: &str,
) -> (Option<SdkTracerProvider>, WorkerGuard, Option<WorkerGuard>) {
    // we use the non-blocking wrapper around stderr so that we don't block the worker on logging. The guard is dropped at the end of main to flush any remaining logs.
    let (non_blocking_writer, stderr_guard) = tracing_appender::non_blocking(std::io::stderr());
    let stderr_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_file(false)
        .with_target(true)
        .with_ansi(true)
        .with_writer(non_blocking_writer)
        .with_filter(EnvFilter::new(&logging_config.log_spec));

    let (file_layer, file_guard) = if logging_config.log_to_file {
        let (layer, guard) = file_layer_with_filter(logging_config);
        (Some(layer), Some(guard))
    } else {
        (None, None)
    };

    if open_telemetry_config.enabled {
        let endpoint = open_telemetry_config.endpoint.clone();
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .unwrap_or_else(|err| {
                eprintln!("Failed to build OTLP exporter: {err}. Exiting worker.");
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

        let opentelemetry_filter = EnvFilter::new(&logging_config.log_spec);
        let opentelemetry = tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("gdal-worker"))
            .with_filter(opentelemetry_filter);

        tracing_subscriber::registry()
            .with(stderr_layer)
            .with(file_layer)
            .with(opentelemetry)
            .init();

        return (Some(provider), stderr_guard, file_guard);
    }

    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(file_layer)
        .init();
    (None, stderr_guard, file_guard)
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
) -> (impl Layer<S> + use<S>, WorkerGuard)
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    // short retention because number_of_processes workers each produce rotated files.
    let file_appender = RollingFileAppender::builder()
        .max_log_files(3)
        .filename_prefix(format!(
            "{}-{}",
            logging_config.filename_prefix, logging_config.worker_id,
        ))
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
    let (token, worker_config) = setup();

    // The OTLP exporter needs a tokio runtime. We keep the worker's main loop synchronous and
    // dedicate a single background thread to the runtime that drives the exporter.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime for telemetry");

    let runtime_guard = runtime.enter();
    let (provider, stderr_guard, file_guard) = init_subscriber(
        &worker_config.logging,
        &worker_config.open_telemetry,
        &token,
    );

    let _worker_span =
        tracing::info_span!("gdal_worker", worker_id = worker_config.logging.worker_id,).entered();

    if let Some(ref opts) = worker_config.gdal_config_options {
        set_gdal_process_global_options(opts);
    }
    reroute_gdal_logging();
    let byte_cache = worker_config.byte_cache.as_ref().and_then(|config| {
        byte_cache::install(&config.directory)
            .inspect_err(|err| {
                tracing::error!("GDAL byte cache not installed, reading without it: {err}");
            })
            .ok()
    });
    run(token, byte_cache);

    drop(runtime_guard);
    if let Some(provider) = provider
        && let Err(err) = provider.shutdown()
    {
        tracing::error!("Failed to flush OpenTelemetry provider: {err}");
    }
    drop(stderr_guard);
    drop(file_guard);
}

fn handle_request(
    IpcChannelMessagePayload {
        dataset_params,
        read_id: _,
        request,
    }: IpcChannelMessagePayload,
    dataset_cache: &mut GdalDatasetHolder,
    sender: &IpcSender<WorkerResponse>,
) -> Result<(), IpcProcessError> {
    match request {
        GdalRequest::Read {
            read_advise,
            data_type,
            level_factor,
        } => raster_type_dispatch(
            &dataset_params,
            read_advise,
            data_type,
            level_factor,
            dataset_cache,
            sender,
        ),
        GdalRequest::Header => send_result(
            GdalHandling::load_raster_info_with_dataset_retry(dataset_cache, &dataset_params)
                .map(IpcChannelResult::RasterInfo),
            sender,
        ),
    }
}

fn raster_type_dispatch(
    dataset_params: &GdalDatasetParameters,
    read_advise: GdalReadAdvise,
    data_type: geoengine_datatypes::raster::RasterDataType,
    level_factor: Option<NonZeroUsize>,
    dataset_cache: &mut GdalDatasetHolder,
    sender: &IpcSender<WorkerResponse>,
) -> Result<(), IpcProcessError> {
    use geoengine_datatypes::raster::RasterDataType as T;
    macro_rules! dispatch {
        ($t:ty) => {
            read_and_send::<$t>(
                dataset_params,
                read_advise,
                level_factor,
                dataset_cache,
                sender,
            )
        };
    }
    match data_type {
        T::U8 => dispatch!(u8),
        T::U16 => dispatch!(u16),
        T::U32 => dispatch!(u32),
        T::U64 => dispatch!(u64),
        T::I8 => dispatch!(i8),
        T::I16 => dispatch!(i16),
        T::I32 => dispatch!(i32),
        T::I64 => dispatch!(i64),
        T::F32 => dispatch!(f32),
        T::F64 => dispatch!(f64),
    }
}

fn run(token: Token, byte_cache: Option<&'static byte_cache::ByteCache>) {
    let (sender, receiver) = match setup_client::<WorkerRequest, WorkerResponse>(token) {
        Ok(pair) => pair,
        Err(err) => exit_with_error(err),
    };

    let mut dataset_cache = if byte_cache.is_some() {
        GdalDatasetHolder::with_byte_cache()
    } else {
        GdalDatasetHolder::new()
    };

    // Loop runs indefinitely, reusing the process and its GDAL dataset cache
    while let Ok(message) = receiver.recv() {
        let payload = match message {
            WorkerRequest::Read(message) => message.0,
            WorkerRequest::ClearCaches => {
                dataset_cache.clear_caches();
                if sender.send(WorkerResponse::CachesCleared).is_err() {
                    break;
                }
                continue;
            }
        };

        let _span = tracing::debug_span!(
            "gdal_worker_read_tile",
            read_id = payload.read_id.as_deref().unwrap_or(""),
            dataset = %payload.dataset_params.file_path.display(),
            band = payload.dataset_params.rasterband_channel,
        )
        .entered();

        let stats = tracing::enabled!(target: "gdal_worker_read", tracing::Level::DEBUG)
            .then(|| ReadStats::start(&payload, &dataset_cache, byte_cache));

        // If helper returns an error, it means the underlying IPC channel is completely broken
        if let Err(err) = handle_request(payload, &mut dataset_cache, &sender) {
            tracing::error!("Fatal IPC channel error: {err:?}. Exiting worker thread.");
            break;
        }
        if let Some(stats) = stats {
            stats.log(byte_cache);
        }
    }
}

/// What a read cost, logged as one event per read for the benches (target `gdal_worker_read`).
/// Requests and bytes come from GDAL's network statistics, which count only with
/// `CPL_VSIL_NETWORK_STATS_ENABLED=YES` and are reset before each read, so that they neither
/// mix reads nor grow with every file a long-lived worker has read.
struct ReadStats {
    read_id: String,
    file: String,
    band: usize,
    /// 0 for a resampled read of the full resolution
    level_factor: usize,
    /// in full-resolution pixels, as the broker ranks workers by it
    footprint: String,
    opened: bool,
    start: std::time::Instant,
}

impl ReadStats {
    fn start(
        payload: &IpcChannelMessagePayload,
        dataset_cache: &GdalDatasetHolder,
        byte_cache: Option<&byte_cache::ByteCache>,
    ) -> Self {
        // SAFETY: this worker runs one request at a time, so no other read is counted
        unsafe { gdal_sys::VSINetworkStatsReset() };
        if let Some(cache) = byte_cache {
            cache.take_counts();
        }
        let level_factor = match &payload.request {
            GdalRequest::Read { level_factor, .. } => level_factor.map_or(0, NonZeroUsize::get),
            GdalRequest::Header => 0,
        };
        Self {
            read_id: payload.read_id.clone().unwrap_or_default(),
            file: payload.dataset_params.file_path.display().to_string(),
            band: payload.dataset_params.rasterband_channel,
            level_factor,
            footprint: payload
                .full_resolution_footprint()
                .map(|b| {
                    let (min, max) = (b.min_index(), b.max_index());
                    format!("{},{},{},{}", min.x(), min.y(), max.x(), max.y())
                })
                .unwrap_or_default(),
            opened: !dataset_cache.contains(&payload.dataset_params),
            start: std::time::Instant::now(),
        }
    }

    /// With the byte cache, requests and bytes are those it downloaded; `chunk_hits` and
    /// `chunk_misses` count its chunks.
    fn log(self, byte_cache: Option<&byte_cache::ByteCache>) {
        let ms = self.start.elapsed().as_secs_f64() * 1000.;
        let (requests, bytes) = network_stats();
        let (chunk_hits, chunk_misses) =
            byte_cache.map_or((0, 0), byte_cache::ByteCache::take_counts);
        tracing::debug!(
            target: "gdal_worker_read",
            read_id = self.read_id,
            file = self.file,
            band = self.band,
            level_factor = self.level_factor,
            footprint = self.footprint,
            opened = self.opened,
            requests,
            bytes,
            chunk_hits,
            chunk_misses,
            ms,
            "GDAL read"
        );
    }
}

/// Requests and downloaded bytes since the last reset of GDAL's network statistics.
fn network_stats() -> (u64, u64) {
    // SAFETY: GDAL returns a string allocated with `VSIMalloc`, or null, and we free it below
    let json = unsafe {
        let ptr = gdal_sys::VSINetworkStatsGetAsSerializedJSON(std::ptr::null_mut());
        if ptr.is_null() {
            return (0, 0);
        }
        let json = std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned();
        gdal_sys::VSIFree(ptr.cast());
        json
    };
    let methods = serde_json::from_str::<serde_json::Value>(&json)
        .ok()
        .and_then(|stats| stats.get("methods").and_then(|m| m.as_object()).cloned())
        .unwrap_or_default();
    methods.values().fold((0, 0), |(requests, bytes), method| {
        let field = |name| {
            method
                .get(name)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        };
        (requests + field("count"), bytes + field("downloaded_bytes"))
    })
}

fn read_and_send<T: GdalType + Pixel + FromPrimitive>(
    dataset_params: &GdalDatasetParameters,
    read_advise: GdalReadAdvise,
    level_factor: Option<NonZeroUsize>,
    dataset_cache: &mut GdalDatasetHolder,
    sender: &IpcSender<WorkerResponse>,
) -> Result<(), IpcProcessError> {
    let result = GdalHandling::load_tile_data_with_dataset_retry::<T>(
        dataset_cache,
        dataset_params,
        read_advise,
        level_factor,
    )
    .and_then(|p| GdalIpcBytePayload::try_from(p).map_err(Into::into))
    .map(IpcChannelResult::Raster);

    send_result(result, sender)
}

/// Sends a worker result back to the server.
fn send_result(
    result: IpcProcessRasterResult,
    sender: &IpcSender<WorkerResponse>,
) -> Result<(), IpcProcessError> {
    // Propagate channel send errors directly up out of the handler
    sender.send(WorkerResponse::ReadResult(result))?;

    Ok(())
}
