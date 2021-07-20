use flexi_logger::{
    style, AdaptiveFormat, Age, Cleanup, Criterion, DeferredNow, Duplicate, Logger, LoggerHandle,
    Naming,
};
use geoengine_services::error::{Error, Result};
use geoengine_services::util::config;
use geoengine_services::util::config::get_config_element;
use log::Record;

#[actix_web::main]
async fn main() -> Result<(), Error> {
    let logger = initialize_logging();

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

fn initialize_logging() -> LoggerHandle {
    let logging_config: config::Logging = get_config_element().unwrap();

    let mut logger = Logger::with_str(logging_config.log_spec)
        .format(custom_log_format)
        .adaptive_format_for_stderr(AdaptiveFormat::Custom(
            custom_log_format,
            colored_custom_log_format,
        ));

    if logging_config.log_to_file {
        logger = logger
            .log_to_file()
            .basename(logging_config.filename_prefix)
            .use_buffering(logging_config.enable_buffering)
            .append()
            .rotate(
                Criterion::Age(Age::Day),
                Naming::Timestamps,
                Cleanup::KeepLogFiles(7),
            )
            .duplicate_to_stderr(Duplicate::All);

        if let Some(dir) = logging_config.log_directory {
            logger = logger.directory(dir);
        }
    }
    logger.start().expect("initialized logger")
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
        now.now().format("%Y-%m-%d %H:%M:%S %:z"),
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
    write!(
        w,
        "[{}] {} [{}] {}",
        style(level, now.now().format("%Y-%m-%d %H:%M:%S %:z")),
        style(level, level),
        record.module_path().unwrap_or("<unnamed>"),
        style(level, &record.args())
    )
}
