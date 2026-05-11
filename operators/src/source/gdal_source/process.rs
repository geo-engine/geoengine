use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};

use gdal::Dataset as GdalDataset;
use geoengine_datatypes::primitives::{CacheHint, TimeInterval};
use geoengine_datatypes::raster::{
    Pixel, RasterDataType, RasterTile2D, TileInformation,
    arrow_ipc_file_to_raster_tile_2d_for_ipc_channel,
    raster_tile_2d_to_arrow_ipc_file_for_ipc_channel,
};
use ipc_channel::{
    IpcError,
    ipc::{self, IpcReceiver, IpcSender},
};

use crate::error::Error;

use super::{GdalDatasetParameters, GdalReadAdvise};

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct IpcChannelMessagePayload {
    pub cache_hint: CacheHint,
    pub dataset_params: GdalDatasetParameters,
    pub tile_information: TileInformation,
    pub tile_time: TimeInterval,
    pub read_advise: GdalReadAdvise,
    /// We use this to know what type we serialize using the `arrow_conversion` functions
    pub data_type: RasterDataType,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct TileData {
    tile: Vec<u8>,
}

impl TileData {
    pub fn new<P: Pixel>(tile: RasterTile2D<P>) -> Result<Self, Error> {
        let as_vec = raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(tile)
            .map_err(|err| Error::DataType { source: err })?;
        Ok(Self { tile: as_vec })
    }
}

// [`IpcError`] does not implement the serde traits, and thus cant be send
// via the ipc_channels
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub enum IpcProcessError {
    SerializationError(String),
    Io(String),
    Disconnected,
    DataType(String),
    Unknown(String),
}

pub type IpcProcessRasterResult = std::result::Result<TileData, IpcProcessError>;

impl From<Error> for IpcProcessError {
    fn from(value: Error) -> Self {
        match value {
            Error::DataType { source } => IpcProcessError::DataType(source.to_string()),
            err => IpcProcessError::Unknown(err.to_string()),
        }
    }
}

impl From<IpcError> for IpcProcessError {
    fn from(value: IpcError) -> Self {
        match value {
            IpcError::SerializationError(error_kind) => {
                IpcProcessError::SerializationError(error_kind.to_string())
            }
            IpcError::Io(error) => IpcProcessError::Io(error.to_string()),
            IpcError::Disconnected => IpcProcessError::Disconnected,
        }
    }
}

pub fn into_raster<P: Pixel>(
    result: IpcProcessRasterResult,
) -> std::result::Result<RasterTile2D<P>, crate::error::Error> {
    result
        .map_err(|err| match err {
            IpcProcessError::SerializationError(error_kind) => Error::GdalSource {
                source: crate::source::GdalSourceError::ProcessInternalBincodeError { error_kind },
            },
            IpcProcessError::Io(internal_error) => Error::GdalSource {
                source: crate::source::GdalSourceError::ProcessInternalIoError { internal_error },
            },
            IpcProcessError::Disconnected => Error::GdalSource {
                source: crate::source::GdalSourceError::ProcessIsDisconnected,
            },
            IpcProcessError::DataType(reason) => Error::GdalSource {
                source: crate::source::GdalSourceError::IpcArrowConversionFailed { reason },
            },
            IpcProcessError::Unknown(err) => Error::GdalSource {
                source: crate::source::GdalSourceError::UnknownErrorHappenedWhileReading {
                    error: err,
                },
            },
        })
        .and_then(|td| {
            arrow_ipc_file_to_raster_tile_2d_for_ipc_channel(td.tile)
                .map_err(|err| Error::DataType { source: err })
        })
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct IpcChannelMessage(pub IpcChannelMessagePayload);

impl IpcChannelMessage {
    pub fn new_request_tile_message(data: IpcChannelMessagePayload) -> Self {
        Self(data)
    }
}

pub fn spawn_ipc_server_process<S, R>() -> (Child, IpcSender<S>, IpcReceiver<R>) {
    let (server, token) = ipc::IpcOneShotServer::<(IpcSender<S>, IpcReceiver<R>)>::new()
        .expect("Failed to create IPC Server");

    let path: PathBuf = std::env::var("GDAL_SOURCE_PROCESS_PATH").map_or_else(
        |_| {
            std::env::current_exe()
                .expect("failed to get current executable path")
                .parent()
                .expect("executable has no parent directory")
                .join("gdalsource-process")
        },
        PathBuf::from,
    );

    let child = Command::new(path)
        .arg(token)
        .spawn()
        .expect("failed to spawn ipc server process");

    let (_rx, channels) = server.accept().expect("accept failed to receive message");
    (child, channels.0, channels.1)
}

/// Creates channels and connects to the IPC server with the given token,
/// sending the channels to the server, so that communication can be established.
///
/// Assumes that the server is already running and listening for connections.
pub fn setup_client<S, C>(token: String) -> crate::util::Result<(IpcSender<C>, IpcReceiver<S>)>
where
    S: for<'de> serde::Deserialize<'de> + serde::Serialize,
    C: for<'de> serde::Deserialize<'de> + serde::Serialize,
{
    let (server_sender, client_reciever) =
        ipc::channel::<S>().map_err(|err| Error::GdalSource {
            source: crate::source::GdalSourceError::ProcessSetupFailed {
                reason: err.to_string(),
            },
        })?;
    let (client_sender, server_reciever) =
        ipc::channel::<C>().map_err(|err| Error::GdalSource {
            source: crate::source::GdalSourceError::ProcessSetupFailed {
                reason: err.to_string(),
            },
        })?;

    let sender =
        ipc::IpcSender::<(IpcSender<S>, IpcReceiver<C>)>::connect(token).map_err(|err| {
            Error::GdalSource {
                source: crate::source::GdalSourceError::ProcessSetupFailed {
                    reason: err.to_string(),
                },
            }
        })?;

    sender
        .send((server_sender, server_reciever))
        .map_err(|error| Error::GdalSource {
            source: crate::source::GdalSourceError::ProcessSetupFailed {
                reason: crate::source::GdalSourceError::IpcSendError { error }.to_string(),
            },
        })?;

    Ok((client_sender, client_reciever))
}

pub struct ProcessData {
    child: Child,
    /// stored in one mutex to gether to ensure that one call to `send`,
    /// ends up with the correct result from `recv`
    sender_receiver_pair: Mutex<(
        IpcSender<IpcChannelMessage>,
        IpcReceiver<IpcProcessRasterResult>,
    )>,
}

impl ProcessData {
    fn new(
        child: Child,
        sender: IpcSender<IpcChannelMessage>,
        receiver: IpcReceiver<IpcProcessRasterResult>,
    ) -> Self {
        Self {
            child,
            sender_receiver_pair: Mutex::new((sender, receiver)),
        }
    }

    /// Spawns a new child process
    /// Is kept alive until `Self` is dropped.
    pub fn spawn() -> Arc<Self> {
        // let (child, sender, receiver) = spawn_ipc_server_process_bytes::<IpcChannelMessage>();
        let (child, sender, receiver) =
            spawn_ipc_server_process::<IpcChannelMessage, IpcProcessRasterResult>();
        Arc::new(Self::new(child, sender, receiver))
    }

    /// Sends `message` to the worker and blocks until the response arrives.
    pub fn send_recv_blocking<P: Pixel>(
        &self,
        message: IpcChannelMessage,
    ) -> crate::util::Result<RasterTile2D<P>> {
        let pair = self
            .sender_receiver_pair
            .lock()
            .map_err(|err| Error::GdalSource {
                source: crate::source::GdalSourceError::ProcessLockPoisoned {
                    error: err.to_string(),
                },
            })?;

        let sender = &pair.0;
        let receiver = &pair.1;

        sender.send(message).map_err(|error| Error::GdalSource {
            source: crate::source::GdalSourceError::IpcSendError { error },
        })?;

        let result = receiver.recv().map_err(|error| Error::GdalSource {
            source: crate::source::GdalSourceError::IpcReceiveError { error },
        })?;

        into_raster::<P>(result)
    }
}

impl Drop for ProcessData {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

/// A simple, single-entry cache for an open GDAL dataset tile.
#[derive(Default)]
pub struct GdalDatasetCache {
    path: Option<PathBuf>,
    dataset: Option<GdalDataset>,
}

impl GdalDatasetCache {
    pub fn new() -> Self {
        Self {
            path: None,
            dataset: None,
        }
    }

    pub fn cache(&mut self, path: PathBuf, dataset: GdalDataset) {
        self.path = Some(path);
        self.dataset = Some(dataset);
    }

    pub fn clear(&mut self) {
        self.path = None;
        self.dataset = None;
    }

    /// Moves the `GdalDataset` because it does not implement `clone()`
    pub fn take(&mut self, input_path: &PathBuf) -> Option<GdalDataset> {
        if let Some(path_ref) = self.path.as_ref()
            && *path_ref == *input_path
        {
            return self.dataset.take();
        }
        None
    }

    pub fn contains(&self, input_path: &PathBuf) -> bool {
        self.path
            .as_ref()
            .is_some_and(|path_ref| *path_ref == *input_path)
    }
}
