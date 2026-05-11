use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};

use gdal::errors::GdalError;
use gdal::{Dataset as GdalDataset, DatasetOptions, GdalOpenFlags};

use geoengine_datatypes::raster::arrow_conversion::{
    arrow_record_batch_to_grid_with_properties, bytes_to_record_batch,
    grid_with_properties_to_arrow_record_batch, record_batch_to_bytes,
};
use geoengine_datatypes::raster::{
    GridBoundingBox2D, GridOrEmpty, Pixel, RasterDataType, RasterProperties,
};

use ipc_channel::{
    IpcError,
    ipc::{self, IpcReceiver, IpcSender},
};
use snafu::Snafu;

use super::{GdalDatasetParameters, GdalReadAdvise, GdalSourceError};
use crate::source::gdal_source::reader::GridAndProperties;
use crate::util::TemporaryGdalThreadLocalConfigOptions;
use crate::util::gdal::gdal_open_ex_gdal_error;

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct IpcChannelMessagePayload {
    pub dataset_params: GdalDatasetParameters,
    pub read_advise: GdalReadAdvise,
    /// We use this to know what type we serialize using the `arrow_conversion` functions
    pub data_type: RasterDataType,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct TileData {
    tile: Vec<u8>,
}

impl TileData {
    pub fn serialize_grid_with_props<P: Pixel>(
        gp: GridAndProperties<P, GridBoundingBox2D>,
    ) -> Result<Self, IpcProcessError> {
        let as_vec = grid_with_properties_to_arrow_record_batch(gp.grid, &gp.properties)
            .and_then(|rb| record_batch_to_bytes(&rb))
            .map_err(|e| IpcProcessError::RasterArrowConversion {
                conversion: e.to_string(),
            })?;

        Ok(Self { tile: as_vec })
    }

    pub fn deserialize_grid_with_props<P: Pixel>(
        &self,
    ) -> Result<GridAndProperties<P, GridBoundingBox2D>, GdalSourceError> {
        let rb = bytes_to_record_batch(&self.tile)
            .map_err(|e| GdalSourceError::ArrowConversion { source: e })?;

        let (grid, properties): (GridOrEmpty<GridBoundingBox2D, P>, RasterProperties) =
            arrow_record_batch_to_grid_with_properties(&rb)
                .map_err(|e| GdalSourceError::ArrowConversion { source: e })?;

        Ok(GridAndProperties { grid, properties })
    }
}

// [`IpcError`] does not implement the serde traits, and thus cant be send
// via the ipc_channels
#[derive(Debug, Snafu, serde::Serialize, serde::Deserialize, Clone)]
pub enum IpcProcessError {
    SerializationError { serde: String },
    Io { io: String },
    Disconnected,
    DataType { datatype: String },
    GdalError { gdal_error: String },
    RasterArrowConversion { conversion: String },

    IpcOther { ipc_error: String },
}

pub type IpcProcessRasterResult = std::result::Result<TileData, IpcProcessError>;

impl From<IpcError> for IpcProcessError {
    fn from(value: IpcError) -> Self {
        match value {
            IpcError::SerializationError(e) => IpcProcessError::SerializationError {
                serde: e.to_string(),
            },
            IpcError::Io(e) => IpcProcessError::Io { io: e.to_string() },
            IpcError::Disconnected => IpcProcessError::Disconnected,
        }
    }
}

impl From<std::io::Error> for IpcProcessError {
    fn from(value: std::io::Error) -> Self {
        IpcProcessError::Io {
            io: value.to_string(),
        }
    }
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
        .arg("debug") // FIXME: paste log level here!
        .stderr(std::process::Stdio::inherit()) // This sends child logs to the parent's stderr
        .spawn()
        .expect("failed to spawn ipc server process");

    let (_rx, channels) = server.accept().expect("accept failed to receive message");
    (child, channels.0, channels.1)
}

/// Creates channels and connects to the IPC server with the given token,
/// sending the channels to the server, so that communication can be established.
///
/// Assumes that the server is already running and listening for connections.
pub fn setup_client<S, C>(
    token: String,
) -> crate::util::Result<(IpcSender<C>, IpcReceiver<S>), IpcProcessError>
where
    S: for<'de> serde::Deserialize<'de> + serde::Serialize,
    C: for<'de> serde::Deserialize<'de> + serde::Serialize,
{
    let (server_sender, client_reciever) = ipc::channel::<S>()?;
    let (client_sender, server_reciever) = ipc::channel::<C>()?;

    let sender = ipc::IpcSender::<(IpcSender<S>, IpcReceiver<C>)>::connect(token)?;

    sender.send((server_sender, server_reciever))?;
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
    ) -> crate::util::Result<GridAndProperties<P, GridBoundingBox2D>, GdalSourceError> {
        // TODO: solution for Mutex that does not pose performance complications
        let pair = self.sender_receiver_pair.lock().map_err(|err| {
            crate::source::GdalSourceError::ProcessLockPoisoned {
                error: err.to_string(),
            }
        })?;

        let sender = &pair.0;
        let receiver = &pair.1;

        sender
            .send(message)
            .map_err(|error| crate::source::GdalSourceError::IpcSendError { error })?;

        let result = receiver
            .recv()
            .map_err(|error| crate::source::GdalSourceError::IpcReceiveError { error })?;

        // now we either got an error or a grid with props from the IPC loader. This helps to understand what went wrong in the IPC process.
        let result =
            result.map_err(|e| crate::source::GdalSourceError::IpcProcessError { source: e })?;

        let g = result.deserialize_grid_with_props::<P>()?;
        // TODO: do some logging here
        Ok(g)
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
    params: Option<GdalDatasetParameters>,
    dataset: Option<GdalDataset>,
    thread_local: Option<TemporaryGdalThreadLocalConfigOptions>,
}

impl GdalDatasetCache {
    pub fn new() -> Self {
        Self {
            params: None,
            dataset: None,
            thread_local: None,
        }
    }

    fn open_ex(
        dataset_params: &GdalDatasetParameters,
    ) -> Result<(GdalDataset, Option<TemporaryGdalThreadLocalConfigOptions>), GdalError> {
        let options = dataset_params
            .gdal_open_options
            .as_ref()
            .map(|o| o.iter().map(String::as_str).collect::<Vec<_>>());

        // reverts the thread local configs on drop
        let thread_local_configs: Option<TemporaryGdalThreadLocalConfigOptions> = dataset_params
            .gdal_config_options
            .as_ref()
            .map(|config_options| TemporaryGdalThreadLocalConfigOptions::new(config_options))
            .transpose()
            .expect("Thread local options must not fail");

        let ds = gdal_open_ex_gdal_error(
            &dataset_params.file_path,
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_RASTER,
                open_options: options.as_deref(),
                ..DatasetOptions::default()
            },
        )?;

        Ok((ds, thread_local_configs))
    }

    pub fn get(&mut self, params: &GdalDatasetParameters) -> Option<&mut GdalDataset> {
        if Self::is_hit(self.params.as_ref(), params) {
            self.dataset.as_mut()
        } else {
            None
        }
    }

    pub fn get_or_open(&mut self, params: &GdalDatasetParameters) -> Result<&mut GdalDataset, ()> {
        // FIXME: use error type here!
        let hit = Self::is_hit(self.params.as_ref(), params);

        if hit {
            Ok(self.dataset.as_mut().expect("Dataset must be set"))
        } else {
            self.clear();

            let (ds, tlo) = Self::open_ex(params).map_err(|_| ())?;

            self.params = Some(params.clone());
            self.dataset = Some(ds);
            self.thread_local = tlo;

            Ok(self.dataset.as_mut().expect("Dataset must be set"))
        }
    }

    fn is_hit(params: Option<&GdalDatasetParameters>, other: &GdalDatasetParameters) -> bool {
        matches!((params, other), (Some(cached_params), other_params) if cached_params.file_path == other_params.file_path)
    }

    pub fn clear(&mut self) {
        self.params = None;
        self.dataset = None;
        self.thread_local = None;
    }

    pub fn contains(&self, params: &GdalDatasetParameters) -> bool {
        Self::is_hit(self.params.as_ref(), params)
    }
}
