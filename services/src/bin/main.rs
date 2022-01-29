use flexi_logger::writers::{FileLogWriter, FileLogWriterHandle};
use flexi_logger::{Age, Cleanup, Criterion, FileSpec, Naming, WriteMode};
use geoengine_services::error::Result;
use geoengine_services::util::config;
use geoengine_services::util::config::get_config_element;
use std::str::FromStr;
use time::format_description;
use tracing::metadata::LevelFilter;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() {
    let _log_writer_handle = initialize_logging();

    start_server().await.unwrap();
}

#[cfg(not(feature = "pro"))]
pub async fn start_server() -> Result<()> {
    geoengine_services::server::start_server(None).await
}

#[cfg(feature = "pro")]
pub async fn start_server() -> Result<()> {
    geoengine_services::pro::server::start_pro_server(None).await
}

/// Tries to initialize `tracing-subscriber` logging, `flexi-logging` and `GDAL` logging.
///
/// # Panics
/// If anything cannot be initialized, this method panics as it is supposed to run at startup.
///
fn initialize_logging() -> Option<FileLogWriterHandle> {
    reroute_gdal_logging();

    let logging_config: config::Logging = get_config_element().unwrap();

    let level_filter = LevelFilter::from_str(&logging_config.log_spec).unwrap();

    let console_layer = tracing_subscriber::fmt::layer()
        .with_file(false)
        .with_target(true)
        .with_ansi(true)
        .with_writer(std::io::stderr)
        .with_filter(level_filter);

    let registry = tracing_subscriber::registry().with(console_layer);

    if logging_config.log_to_file {
        let mut file_spec = FileSpec::default().basename(logging_config.filename_prefix);

        if let Some(dir) = logging_config.log_directory {
            file_spec = file_spec.directory(dir);
        }

        // TODO: use local time instead of UTC
        // On Unix, using local time implies using an unsound feature: https://docs.rs/flexi_logger/latest/flexi_logger/error_info/index.html#time
        // Thus, we use UTC time instead.
        flexi_logger::DeferredNow::force_utc();

        let (file_writer, fw_handle) = FileLogWriter::builder(file_spec)
            .write_mode(WriteMode::Async)
            .append()
            .rotate(
                Criterion::Age(Age::Day),
                Naming::Timestamps,
                Cleanup::KeepLogFiles(7),
            )
            .try_build_with_handle()
            .unwrap();

        let file_layer = tracing_subscriber::fmt::layer()
            .with_file(false)
            .with_target(true)
            // TODO: there are still format flags within spans due to bug: https://github.com/tokio-rs/tracing/issues/1817
            .with_ansi(false)
            .with_writer(move || file_writer.clone())
            .with_filter(level_filter);

        let registry = registry.with(file_layer);

        registry.init();

        return Some(fw_handle);
    }

    registry.init();

    None
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
