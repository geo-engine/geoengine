use std::fmt::Display;

use gdal::raster::GdalType;
use geoengine_datatypes::raster::{Pixel, RasterTile2D, TypedRasterTile2D};
use geoengine_operators::source::{
    self, GdalDatasetCache, IpcChannelMessage, IpcChannelMessagePayload, IpcProcessError,
    IpcProcessRasterResult, TileData, setup_client,
};
use ipc_channel::ipc::IpcSender;
use num::FromPrimitive;

type Sender = IpcSender<IpcProcessRasterResult>;

fn exit_with_error(msg: impl Display) -> ! {
    tracing::error!("Error: {msg}");
    std::process::exit(1);
}

#[tokio::main]
async fn main() -> Result<()> {
    if let Err(err) = run().await {
        exit_with_error(err)
    }

    Ok(())
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn run() -> Result<()> {
    let token = setup();
    let (sender, receiver) = match setup_client::<IpcChannelMessage, IpcProcessRasterResult>(token)
    {
        Ok(pair) => pair,
        Err(err) => exit_with_error(err),
    };
    let mut dataset_cache = GdalDatasetCache::new();

    loop {
        let message = match receiver.recv() {
            Ok(msg) => msg,
            Err(err) => {
                if let Err(err) = sender.send(Err(IpcProcessError::from(err))) {
                    sender.send(Err(IpcProcessError::Unknown(err.to_string())))?;
                }
                continue;
            }
        };

        let payload = message.0;
        let result = match payload.data_type {
            geoengine_datatypes::raster::RasterDataType::U8 => {
                handle::<u8>(payload, &mut dataset_cache, &sender).await
            }
            geoengine_datatypes::raster::RasterDataType::U16 => {
                handle::<u16>(payload, &mut dataset_cache, &sender).await
            }
            geoengine_datatypes::raster::RasterDataType::U32 => {
                handle::<u32>(payload, &mut dataset_cache, &sender).await
            }
            geoengine_datatypes::raster::RasterDataType::U64 => {
                handle::<u64>(payload, &mut dataset_cache, &sender).await
            }
            geoengine_datatypes::raster::RasterDataType::I8 => {
                handle::<i8>(payload, &mut dataset_cache, &sender).await
            }
            geoengine_datatypes::raster::RasterDataType::I16 => {
                handle::<i16>(payload, &mut dataset_cache, &sender).await
            }
            geoengine_datatypes::raster::RasterDataType::I32 => {
                handle::<i32>(payload, &mut dataset_cache, &sender).await
            }
            geoengine_datatypes::raster::RasterDataType::I64 => {
                handle::<i64>(payload, &mut dataset_cache, &sender).await
            }
            geoengine_datatypes::raster::RasterDataType::F32 => {
                handle::<f32>(payload, &mut dataset_cache, &sender).await
            }
            geoengine_datatypes::raster::RasterDataType::F64 => {
                handle::<f64>(payload, &mut dataset_cache, &sender).await
            }
        };
        if let Err(err) = result
            && let Err(err) = sender.send(Err(IpcProcessError::Unknown(err.to_string())))
        {
            sender.send(Err(IpcProcessError::Unknown(err.to_string())))?;
        }
    }
}

async fn handle<P: FromPrimitive + Pixel + GdalType>(
    IpcChannelMessagePayload {
        cache_hint,
        dataset_params,
        tile_information,
        tile_time,
        read_advise,
        data_type: _,
    }: IpcChannelMessagePayload,
    dataset_cache: &mut GdalDatasetCache,
    sender: &IpcSender<IpcProcessRasterResult>,
) -> Result<()>
where
    RasterTile2D<P>: Into<TypedRasterTile2D>,
{
    // cache now happens in the callchain
    #[allow(deprecated)] // this is the place where it should be used!
    let tile: RasterTile2D<P> = source::__private::load_tile_async_cached(
        dataset_cache,
        dataset_params.clone(),
        read_advise,
        tile_information,
        tile_time,
        cache_hint,
    )
    .await?;

    if let Err(some_err) = send_tile(tile, sender) {
        sender.send(Err(some_err))?;
    }
    Ok(())
}

fn send_tile<T: Pixel + FromPrimitive + GdalType>(
    tile: RasterTile2D<T>,
    sender: &Sender,
) -> std::result::Result<(), IpcProcessError> {
    match TileData::new(tile) {
        Ok(td) => sender.send(Ok(td)),
        Err(err) => sender.send(Err(IpcProcessError::from(err))),
    }
    .map_err(|err| IpcProcessError::SerializationError(err.to_string()))
}

type Token = String;

fn setup() -> Token {
    let args: Vec<String> = std::env::args().collect();
    assert!(
        args.len() >= 2,
        "Usage: gdalprocess-ipc-channel-server <token>"
    );
    args[1].clone()
}
