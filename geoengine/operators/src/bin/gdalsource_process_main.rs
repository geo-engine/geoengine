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

use tracing::Level;
use tracing_subscriber::fmt;

fn exit_with_error(msg: impl Display) -> ! {
    tracing::error!("Error: {msg}");
    std::process::exit(1);
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

fn setup_logging(level: Level) {
    fmt()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        .with_target(true)
        .with_ansi(false)
        .init();
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
    let (token, debug_lvl) = setup();
    setup_logging(debug_lvl);
    reroute_gdal_logging();

    run(token);
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
        let payload = message.0;

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
    }: IpcChannelMessagePayload,
    dataset_cache: &mut GdalDatasetCache,
    sender: &IpcSender<IpcProcessRasterResult>,
) -> Result<(), IpcProcessError> {
    let gp = GdalRasterLoader::load_tile_data_with_dataset_retry::<T>(
        dataset_cache,
        &dataset_params,
        read_advise,
    )
    .map_err(IpcProcessError::from);

    let byte_payload = gp.and_then(|p| GdalIpcBytePayload::try_from(p).map_err(Into::into));
    // Propagate channel send errors directly up out of the handler
    match byte_payload {
        Ok(td) => sender.send(Ok(td)),
        Err(err) => sender.send(Err(err)),
    }?;

    Ok(())
}
