use flexi_logger::writers::FileLogWriter;
use flexi_logger::{Age, Cleanup, Criterion, FileSpec, Naming, WriteMode};
use geoengine_services::error::Result;
use geoengine_services::util::config;
use geoengine_services::util::config::get_config_element;
use std::str::FromStr;
use time::format_description;
use tracing::metadata::LevelFilter;
use tracing::Subscriber;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

#[tokio::main]
async fn main() {
    start_server().await.unwrap();
}

#[cfg(not(feature = "pro"))]
pub async fn start_server() -> Result<()> {
    reroute_gdal_logging();
    let logging_config: config::Logging = get_config_element()?;
    // create a filter for the log message level
    let level_filter = LevelFilter::from_str(&logging_config.log_spec).unwrap();

    // get a new tracing subscriber registry to add all log and tracing layers to
    let registry = tracing_subscriber::Registry::default();

    // create a log layer for output to the console and add it to the registry
    let registry = registry.with(console_layer().with_filter(level_filter));

    // create a log layer for output to a file and add it to the registry
    let file_layer = if logging_config.log_to_file {
        Some(
            file_layer(
                &logging_config.filename_prefix,
                logging_config.log_directory.as_deref(),
            )
            .with_filter(level_filter),
        )
    } else {
        None
    };
    let registry = registry.with(file_layer);

    registry.init();

    geoengine_services::server::start_server(None).await
}

#[cfg(feature = "pro")]
pub async fn start_server() -> Result<()> {
    reroute_gdal_logging();
    let logging_config: config::Logging = get_config_element()?;

    // get a new tracing subscriber registry to add all log and tracing layers to
    let registry = tracing_subscriber::Registry::default();

    // create a filter for the log message level
    let level_filter = LevelFilter::from_str(&logging_config.log_spec).unwrap();

    // create a log layer for output to the console and add it to the registry
    let registry = registry.with(console_layer().with_filter(level_filter));

    // create a log layer for output to a file and add it to the registry
    let file_layer = if logging_config.log_to_file {
        Some(
            file_layer(
                &logging_config.filename_prefix,
                logging_config.log_directory.as_deref(),
            )
            .with_filter(level_filter),
        )
    } else {
        None
    };
    let registry = registry.with(file_layer);

    // create a telemetry layer for output to opentelemetry and add it to the registry
    let open_telemetry_config: geoengine_services::pro::util::config::OpenTelemetry =
        get_config_element()?;
    let opentelemetry_layer = if open_telemetry_config.enabled {
        Some(open_telemetry_layer(&open_telemetry_config)?)
    } else {
        None
    };
    let registry = registry.with(opentelemetry_layer);

    // initialize the registry as the global tracing subscriber
    registry.init();

    geoengine_services::pro::server::start_pro_server(None).await
}

#[cfg(feature = "pro")]
fn open_telemetry_layer<S>(
    open_telemetry_config: &geoengine_services::pro::util::config::OpenTelemetry,
) -> Result<tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry::sdk::trace::Tracer>>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    use opentelemetry_jaeger::config::agent::AgentPipeline;
    use tracing_opentelemetry::OpenTelemetryLayer;
    let tracer = AgentPipeline::default()
        .with_endpoint(open_telemetry_config.endpoint)
        .with_service_name("Geo Engine")
        .install_simple()?;
    let opentelemetry: OpenTelemetryLayer<S, _> =
        tracing_opentelemetry::layer().with_tracer(tracer);
    Ok(opentelemetry)
}

fn console_layer<S>() -> impl Layer<S>
where
    S: Subscriber,
    for<'a> S: LookupSpan<'a>,
{
    tracing_subscriber::fmt::layer()
        .with_file(false)
        .with_target(true)
        .with_ansi(true)
        .with_writer(std::io::stderr)
}

fn file_layer<S>(filename_prefix: &str, log_directory: Option<&str>) -> impl Layer<S>
where
    S: Subscriber,
    for<'a> S: LookupSpan<'a>,
{
    let mut file_spec = FileSpec::default().basename(filename_prefix);

    if let Some(dir) = log_directory {
        file_spec = file_spec.directory(dir);
    }

    // TODO: use local time instead of UTC
    // On Unix, using local time implies using an unsound feature: https://docs.rs/flexi_logger/latest/flexi_logger/error_info/index.html#time
    // Thus, we use UTC time instead.
    flexi_logger::DeferredNow::force_utc();

    let (file_writer, _fw_handle) = FileLogWriter::builder(file_spec)
        // TODO: the mode should be Async but somehow that never writes anything to the file
        .write_mode(WriteMode::BufferAndFlush)
        .append()
        .rotate(
            Criterion::Age(Age::Day),
            Naming::Timestamps,
            Cleanup::KeepLogFiles(7),
        )
        .try_build_with_handle()
        .unwrap();

    tracing_subscriber::fmt::layer()
        .with_file(false)
        .with_target(true)
        // TODO: there are still format flags within spans due to bug: https://github.com/tokio-rs/tracing/issues/1817
        .with_ansi(false)
        .with_writer(move || file_writer.clone())
}

/// We install a GDAL error handler that logs all messages with our log macros.
fn reroute_gdal_logging() {
    gdal::config::set_error_handler(|error_type, error_num, error_msg| {
        let target = "GDAL";
        match error_type {
            gdal::errors::CplErrType::None => {
                // should never log anything
                log::info!(target: target, "GDAL None {}: {}", error_num, error_msg)
            }
            gdal::errors::CplErrType::Debug => {
                log::debug!(target: target, "GDAL Debug {}: {}", error_num, error_msg)
            }
            gdal::errors::CplErrType::Warning => {
                log::warn!(target: target, "GDAL Warning {}: {}", error_num, error_msg)
            }
            gdal::errors::CplErrType::Failure => {
                log::error!(target: target, "GDAL Failure {}: {}", error_num, error_msg)
            }
            gdal::errors::CplErrType::Fatal => {
                log::error!(target: target, "GDAL Fatal {}: {}", error_num, error_msg)
            }
        };
    });
}

const TIME_FORMAT_STR: &str = "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:6] \
                [offset_hour sign:mandatory]:[offset_minute]";

lazy_static::lazy_static! {
    static ref TIME_FORMAT: Vec<format_description::FormatItem<'static>>
        = format_description::parse(TIME_FORMAT_STR).expect("time format must be parsable");
}
