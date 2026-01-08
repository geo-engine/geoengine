pub use geoengine_operators::processing::initialize_expression_dependencies;
use geoengine_services::{
    config::{self, get_config_element},
    error::Result,
};
use tracing::Subscriber;
use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{RollingFileAppender, Rotation},
};
use tracing_flame::FlameLayer;
use tracing_subscriber::{
    EnvFilter, Layer,
    field::RecordFields,
    fmt::{
        FormatFields,
        format::{DefaultFields, Writer},
    },
    layer::Filter,
    prelude::*,
    registry::LookupSpan,
};

/// Starts the server.
///
///  # Panics
///  - if the logging configuration is not valid
///
pub async fn start_server() -> Result<()> {
    reroute_gdal_logging();
    let logging_config: config::Logging = get_config_element()?;
    configure_error_report_formatting(&logging_config);

    // get a new tracing subscriber registry to add all log and tracing layers to
    let registry = tracing_subscriber::Registry::default();

    // create a filter for the log message level in console output
    let console_filter =
        EnvFilter::try_new(&logging_config.log_spec).expect("to have a valid log spec");

    // create a log layer for output to the console and add it to the registry
    let registry = registry.with(console_layer_with_filter(console_filter));

    // create a filter for the log message level in file output. Since the console_filter is not copy or clone, we have to create a new one. TODO: allow a different log level for file output.
    let file_filter =
        EnvFilter::try_new(&logging_config.log_spec).expect("to have a valid log spec");

    // create a log layer for output to a file and add it to the registry
    let (file_layer, _writer_drop_guard) = if logging_config.log_to_file {
        let (file_layer, writer_drop_guard) = file_layer_with_filter(
            &logging_config.filename_prefix,
            logging_config.log_directory.as_deref(),
            file_filter,
        );
        (Some(file_layer), Some(writer_drop_guard))
    } else {
        (None, None)
    };

    let (flame_layer, _flame_drop_guard) = if logging_config.log_to_flame {
        let (flame_layer, flame_drop_quard) =
            FlameLayer::with_file(format!("{}.folded", &logging_config.filename_prefix))
                .expect("to create flame layer");
        (Some(flame_layer), Some(flame_drop_quard))
    } else {
        (None, None)
    };

    let registry = registry.with(file_layer).with(flame_layer);

    // create a telemetry layer for output to opentelemetry and add it to the registry
    let open_telemetry_config: geoengine_services::config::OpenTelemetry = get_config_element()?;
    let opentelemetry_layer = if open_telemetry_config.enabled {
        Some(open_telemetry_layer(&open_telemetry_config)?)
    } else {
        None
    };
    let registry = registry.with(opentelemetry_layer);

    // initialize the registry as the global tracing subscriber
    registry.init();

    geoengine_services::server::start_server(None).await
}

fn open_telemetry_layer<S>(
    open_telemetry_config: &geoengine_services::config::OpenTelemetry,
) -> Result<
    tracing_opentelemetry::OpenTelemetryLayer<
        S,
        impl opentelemetry::trace::Tracer<Span: Send + Sync> + use<S>,
    >,
>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::trace::Sampler;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(open_telemetry_config.endpoint.to_string())
        .build()?;
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(Sampler::AlwaysOn)
        .with_resource(
            opentelemetry_sdk::Resource::builder_empty()
                .with_attribute(opentelemetry::KeyValue::new("service.name", "Geo Engine"))
                .build(),
        )
        .build();

    let tracer = provider.tracer("Geo Engine");

    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    Ok(opentelemetry)
}

fn console_layer_with_filter<S, F: Filter<S> + 'static>(filter: F) -> impl Layer<S>
where
    S: Subscriber,
    for<'a> S: LookupSpan<'a>,
{
    tracing_subscriber::fmt::layer()
        .pretty()
        .with_file(false)
        .with_target(true)
        .with_ansi(true)
        .with_writer(std::io::stderr)
        .with_filter(filter)
}

// we use a custom formatter because there are still format flags within spans even when `with_ansi` is false due to bug: https://github.com/tokio-rs/tracing/issues/1817
struct FileFormatterWorkaround(DefaultFields);

impl<'writer> FormatFields<'writer> for FileFormatterWorkaround {
    fn format_fields<R: RecordFields>(
        &self,
        writer: Writer<'writer>,
        fields: R,
    ) -> core::fmt::Result {
        self.0.format_fields(writer, fields)
    }
}

fn file_layer_with_filter<S, F: Filter<S> + 'static>(
    filename_prefix: &str,
    log_directory: Option<&str>,
    filter: F,
) -> (impl Layer<S> + use<S, F>, WorkerGuard)
where
    S: Subscriber,
    for<'a> S: LookupSpan<'a>,
{
    let file_appender = RollingFileAppender::builder()
        .max_log_files(7)
        .filename_prefix(filename_prefix)
        .filename_suffix("log")
        .rotation(Rotation::DAILY)
        .build(log_directory.unwrap_or("./"))
        .expect("failed to initialize rolling file appender");

    let (non_blocking_writer, guard) = tracing_appender::non_blocking(file_appender);

    let layer = tracing_subscriber::fmt::layer()
        .with_file(false)
        .with_target(true)
        // we use a custom formatter because there are still format flags within spans even when `with_ansi` is false due to bug: https://github.com/tokio-rs/tracing/issues/1817
        .fmt_fields(FileFormatterWorkaround(DefaultFields::default()))
        .with_ansi(false)
        .with_writer(non_blocking_writer)
        .with_filter(filter);
    (layer, guard)
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

fn configure_error_report_formatting(logging_config: &config::Logging) {
    if logging_config.raw_error_messages {
        // there is no way to configure snafu::Report other than through env variables
        unsafe { std::env::set_var("SNAFU_RAW_ERROR_MESSAGES", "1") };
    }
}

#[tokio::main]
async fn main() {
    initialize_expression_dependencies()
        .await
        .expect("successful compilation process is necessary for expression operators to work");

    start_server().await.expect("the server has to start");
}
