use flexi_logger::{
    style, AdaptiveFormat, Age, Cleanup, Criterion, DeferredNow, Duplicate, FileSpec, Logger,
    LoggerHandle, Naming, WriteMode,
};
use geoengine_services::error::{Error, Result};
use geoengine_services::util::config;
use geoengine_services::util::config::get_config_element;
use log::Record;
use time::format_description;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let logger = initialize_logging()?;

    let res = start_server().await;

    logger.shutdown();
    res
}

#[cfg(not(feature = "pro"))]
pub async fn start_server() -> Result<()> {
    geoengine_services::server::start_server(None).await
}

#[cfg(feature = "pro")]
pub async fn start_server() -> Result<()> {
    geoengine_services::pro::server::start_pro_server(None).await
}

fn initialize_logging() -> Result<LoggerHandle> {
    let logging_config: config::Logging = get_config_element()?;

    let mut logger = Logger::try_with_str(logging_config.log_spec)?
        .format(custom_log_format)
        .adaptive_format_for_stderr(AdaptiveFormat::Custom(
            custom_log_format,
            colored_custom_log_format,
        ));

    if logging_config.log_to_file {
        let mut file_spec = FileSpec::default().basename(logging_config.filename_prefix);

        if let Some(dir) = logging_config.log_directory {
            file_spec = file_spec.directory(dir);
        }

        logger = logger
            .log_to_file(file_spec)
            .write_mode(if logging_config.enable_buffering {
                WriteMode::BufferAndFlush
            } else {
                WriteMode::Direct
            })
            .append()
            .rotate(
                Criterion::Age(Age::Day),
                Naming::Timestamps,
                Cleanup::KeepLogFiles(7),
            )
            .duplicate_to_stderr(Duplicate::All);
    }

    reroute_gdal_logging();

    Ok(logger.start()?)
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

/// A logline-formatter that produces log lines like
/// <br>
/// ```[2021-05-18 10:16:53 +02:00] INFO [my_prog::some_submodule] Task successfully read from conf.json```
/// <br>
fn custom_log_format(
    w: &mut dyn std::io::Write,
    now: &mut DeferredNow,
    record: &Record,
) -> Result<(), std::io::Error> {
    write!(
        w,
        "[{}] {} [{}] {}",
        now.now()
            .format(&TIME_FORMAT)
            .unwrap_or_else(|_| "Timestamping failed".to_string()),
        record.level(),
        record.module_path().unwrap_or("<unnamed>"),
        &record.args()
    )
}

/// A colored version of the logline-formatter `custom_log_format`.
fn colored_custom_log_format(
    w: &mut dyn std::io::Write,
    now: &mut DeferredNow,
    record: &Record,
) -> Result<(), std::io::Error> {
    let level = record.level();
    let style = style(level);

    write!(
        w,
        "[{}] {} [{}] {}",
        style.paint(
            now.now()
                .format(&TIME_FORMAT)
                .unwrap_or_else(|_| "Timestamping failed".to_string()),
        ),
        style.paint(level.to_string()),
        record.module_path().unwrap_or("<unnamed>"),
        style.paint(&record.args().to_string())
    )
}
