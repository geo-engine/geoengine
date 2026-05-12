use std::{fmt::Display, str::FromStr};

use gdal::raster::GdalType;
use geoengine_datatypes::raster::Pixel;
use geoengine_operators::source::{
    GdalDatasetCache, IpcChannelMessage, IpcChannelMessagePayload, IpcProcessError,
    IpcProcessRasterResult, TileData, gdal_source::GdalRasterLoader, setup_client,
};
use ipc_channel::ipc::IpcSender;
use num::FromPrimitive;

use tracing::Level;

type Sender = IpcSender<IpcProcessRasterResult>;

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

fn main() {
    let (token, debug_lvl) = setup();
    reroute_gdal_logging();

    //tracing_subscriber::fmt().with_max_level(debug_lvl).init();

    if let Err(err) = run(token) {
        exit_with_error(err)
    }
}

fn run(token: Token) -> Result<(), IpcProcessError> {
    let (sender, receiver) = match setup_client::<IpcChannelMessage, IpcProcessRasterResult>(token)
    {
        Ok(pair) => pair,
        Err(err) => exit_with_error(err),
    };
    let mut dataset_cache = GdalDatasetCache::new();

    while let Ok(message) = receiver.recv() {
        let payload = message.0;
        let result = match payload.data_type {
            geoengine_datatypes::raster::RasterDataType::U8 => {
                handle::<u8>(payload, &mut dataset_cache, &sender)
            }
            geoengine_datatypes::raster::RasterDataType::U16 => {
                handle::<u16>(payload, &mut dataset_cache, &sender)
            }
            geoengine_datatypes::raster::RasterDataType::U32 => {
                handle::<u32>(payload, &mut dataset_cache, &sender)
            }
            geoengine_datatypes::raster::RasterDataType::U64 => {
                handle::<u64>(payload, &mut dataset_cache, &sender)
            }
            geoengine_datatypes::raster::RasterDataType::I8 => {
                handle::<i8>(payload, &mut dataset_cache, &sender)
            }
            geoengine_datatypes::raster::RasterDataType::I16 => {
                handle::<i16>(payload, &mut dataset_cache, &sender)
            }
            geoengine_datatypes::raster::RasterDataType::I32 => {
                handle::<i32>(payload, &mut dataset_cache, &sender)
            }
            geoengine_datatypes::raster::RasterDataType::I64 => {
                handle::<i64>(payload, &mut dataset_cache, &sender)
            }
            geoengine_datatypes::raster::RasterDataType::F32 => {
                handle::<f32>(payload, &mut dataset_cache, &sender)
            }
            geoengine_datatypes::raster::RasterDataType::F64 => {
                handle::<f64>(payload, &mut dataset_cache, &sender)
            }
        };

        if let Err(err) = result {
            // Attempt to notify the parent, but if the sender is also dead, just exit
            if let Err(_) = sender.send(Err(IpcProcessError::IpcOther {
                ipc_error: err.to_string(),
            })) {
                eprintln!("IPC Receiver error: {:?}. Exiting worker.", err);
                break;
            }
        }
    }

    Ok(())
}

fn handle<P: FromPrimitive + Pixel + GdalType>(
    IpcChannelMessagePayload {
        dataset_params,
        read_advise,
        data_type: _,
    }: IpcChannelMessagePayload,
    dataset_cache: &mut GdalDatasetCache,
    sender: &IpcSender<IpcProcessRasterResult>,
) -> Result<(), IpcProcessError> {
    let gp = GdalRasterLoader::load_tile_data_with_dataset_retry::<P>(
        dataset_cache,
        &dataset_params,
        read_advise,
    )
    .map_err(|e| IpcProcessError::GdalError {
        gdal_error: e.to_string(),
    });

    let tile_data = gp.and_then(|g| TileData::serialize_grid_with_props(g));

    if let Err(some_err) = send_serialized_tile_or_error(tile_data, sender) {
        sender.send(Err(some_err)).map_err(IpcProcessError::from)?;
    }
    Ok(())
}

fn send_serialized_tile_or_error(
    tile_res: Result<TileData, IpcProcessError>,
    sender: &Sender,
) -> std::result::Result<(), IpcProcessError> {
    match tile_res {
        Ok(td) => sender.send(Ok(td)),
        Err(err) => sender.send(Err(err)),
    }?;
    Ok(())
}
