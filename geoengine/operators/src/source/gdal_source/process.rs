use std::borrow::Cow;
use std::path::PathBuf;
use std::process::Command;

use bytemuck::{AnyBitPattern, NoUninit};
use gdal::errors::GdalError;
use gdal::{Dataset as GdalDataset, DatasetOptions, GdalOpenFlags};

use geoengine_datatypes::raster::{
    Grid, GridBoundingBox2D, GridOrEmpty, GridShape2D, MaskedGrid, NoDataValueGrid, Pixel,
    RasterDataType, RasterProperties,
};
use ipc_channel::{
    IpcError,
    ipc::{self, IpcReceiver, IpcSender},
};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use super::{GdalDatasetParameters, GdalReadAdvise};
use crate::source::gdal_source::GdalRasterLoaderError;
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

pub type IpcProcessRasterResult = Result<GdalIpcBytePayload, IpcProcessError>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GdalIpcBytePayload {
    pub dimensions: GridBoundingBox2D,
    pub properties: RasterProperties,
    pub data_variant: GdalDataByteVariant,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum GdalDataByteVariant {
    /// Represents an Empty Grid (zero allocation for pixels or masks)
    Empty,

    /// GDAL Native: Every single pixel in the buffer is valid.
    /// ZERO mask data is written to or read from the IPC pipe.
    AllValid {
        #[serde(with = "serde_bytes")]
        raw_bytes: Vec<u8>,
    },

    /// GDAL Native: Invalid pixels are defined by a specific scalar value.
    /// The mask vector is completely omitted from the wire and calculated by the caller.
    WithNoData {
        #[serde(with = "serde_bytes")]
        raw_bytes: Vec<u8>,
        no_data_value: f64, // Stored as f64 to easily cover all primitive type limits
    },

    /// Fallback: Used only if an irregular mask band is present
    WithExplicitMask {
        #[serde(with = "serde_bytes")]
        raw_bytes: Vec<u8>,
        #[serde(with = "serde_bytes")]
        validity_mask: Vec<u8>, // 1 byte per pixel: 1 = valid, 0 = invalid
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GdalIpcPayload<T> {
    pub dimensions: GridBoundingBox2D,
    pub properties: RasterProperties,
    pub data_variant: GdalDataVariant<T>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum GdalDataVariant<T> {
    /// Represents an Empty Grid (zero allocation for pixels or masks)
    Empty,
    AllValid {
        data: Vec<T>,
    },

    WithNoData {
        data: Vec<T>,
        no_data_value: f64, // Stored as f64 to easily cover all primitive type limits
    },

    WithExplicitMask {
        data: Vec<T>,
        validity_mask: Vec<u8>, // 1 byte per pixel: 1 = valid, 0 = invalid
    },
}

impl<T> From<GdalIpcBytePayload> for GdalIpcPayload<T>
where
    T: bytemuck::AnyBitPattern,
{
    fn from(value: GdalIpcBytePayload) -> Self {
        Self {
            dimensions: value.dimensions,
            properties: value.properties,
            data_variant: value.data_variant.into(),
        }
    }
}

impl<T> TryFrom<GdalIpcPayload<T>> for GdalIpcBytePayload
where
    T: bytemuck::NoUninit,
{
    type Error = bytemuck::PodCastError;

    fn try_from(value: GdalIpcPayload<T>) -> Result<Self, Self::Error> {
        Ok(Self {
            dimensions: value.dimensions,
            properties: value.properties,
            data_variant: value.data_variant.try_into()?,
        })
    }
}

impl<T> TryFrom<GdalDataVariant<T>> for GdalDataByteVariant
where
    T: NoUninit,
{
    type Error = bytemuck::PodCastError;

    fn try_from(value: GdalDataVariant<T>) -> Result<Self, Self::Error> {
        match value {
            GdalDataVariant::Empty => Ok(GdalDataByteVariant::Empty),
            GdalDataVariant::AllValid { data } => {
                bytemuck::try_cast_slice::<T, u8>(&data).map(|s| GdalDataByteVariant::AllValid {
                    raw_bytes: s.to_vec(),
                })
            }
            GdalDataVariant::WithNoData {
                data,
                no_data_value,
            } => {
                bytemuck::try_cast_slice::<T, u8>(&data).map(|s| GdalDataByteVariant::WithNoData {
                    raw_bytes: s.to_vec(),
                    no_data_value,
                })
            }
            GdalDataVariant::WithExplicitMask {
                data,
                validity_mask,
            } => bytemuck::try_cast_slice::<T, u8>(&data).map(|s| {
                GdalDataByteVariant::WithExplicitMask {
                    raw_bytes: s.to_vec(),
                    validity_mask: validity_mask,
                }
            }),
        }
    }
}

impl<T> From<GdalDataByteVariant> for GdalDataVariant<T>
where
    T: AnyBitPattern,
{
    fn from(value: GdalDataByteVariant) -> Self {
        match value {
            GdalDataByteVariant::Empty => GdalDataVariant::Empty,
            GdalDataByteVariant::AllValid { raw_bytes } => GdalDataVariant::AllValid {
                data: bytemuck::cast_slice::<u8, T>(&raw_bytes).to_vec(),
            },
            GdalDataByteVariant::WithNoData {
                raw_bytes,
                no_data_value,
            } => GdalDataVariant::WithNoData {
                data: bytemuck::cast_slice::<u8, T>(&raw_bytes).to_vec(),
                no_data_value,
            },
            GdalDataByteVariant::WithExplicitMask {
                raw_bytes,
                validity_mask,
            } => GdalDataVariant::WithExplicitMask {
                data: bytemuck::cast_slice::<u8, T>(&raw_bytes).to_vec(),
                validity_mask: validity_mask,
            },
        }
    }
}

impl<T> From<GdalIpcPayload<T>> for GridAndProperties<T, GridBoundingBox2D>
where
    T: Pixel,
{
    fn from(value: GdalIpcPayload<T>) -> Self {
        match value.data_variant {
            GdalDataVariant::Empty => GridAndProperties {
                grid: GridOrEmpty::new_empty_shape(value.dimensions),
                properties: value.properties,
            },
            GdalDataVariant::AllValid { data } => GridAndProperties {
                grid: GridOrEmpty::new_grid(MaskedGrid::new_with_data(
                    Grid::new(value.dimensions, data).expect("shape and data match"),
                )),
                properties: value.properties,
            },
            GdalDataVariant::WithNoData {
                data,
                no_data_value,
            } => {
                let no_data_t = T::from_(no_data_value);
                GridAndProperties {
                    grid: GridOrEmpty::new_grid(
                        NoDataValueGrid::new(
                            Grid::new(value.dimensions, data).expect("shape and data match"),
                            Some(no_data_t),
                        )
                        .into(),
                    ),
                    properties: value.properties,
                }
            }
            GdalDataVariant::WithExplicitMask {
                data,
                validity_mask,
            } => GridAndProperties {
                grid: GridOrEmpty::new_grid(
                    MaskedGrid::new(
                        Grid::new(value.dimensions.clone(), data).expect("shape and data match"),
                        Grid::new(
                            value.dimensions,
                            validity_mask.iter().map(|&m| m > 0).collect(),
                        )
                        .expect("shape and data match"),
                    )
                    .expect("dimensions must match"),
                ),
                properties: value.properties,
            },
        }
    }
}

// [`IpcError`] does not implement the serde traits, and thus cant be send
// via the ipc_channels

// --- 1. STRONGLY TYPED SUB-CATEGORIES (Must be Serialize + Deserialize + Clone) ---

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum GdalErrorKind {
    FileNotFound,
    AccessDenied,
    InvalidDataset,
    ConnectionTimeout,
    Unknown,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum IoErrorKind {
    NotFound,
    PermissionDenied,
    ConnectionRefused,
    ConnectionAborted,
    UnexpectedEof,
    Other,
}

// --- 2. THE IPC TRANSFERRABLE ERROR ENUM ---

#[derive(Debug, Snafu, serde::Serialize, serde::Deserialize, Clone)]
#[serde(tag = "type", content = "details")] // Flattens payload data over the ipc-channel
#[snafu(visibility(pub))]
pub enum IpcProcessError {
    #[snafu(display("Serialization error: {msg}"))]
    SerializationError {
        // Cow allows zero-allocation compilation-string sharing on creation.
        // On the receiving side of ipc-channel, serde automatically hydrates this into Cow::Owned.
        msg: Cow<'static, str>,
    },

    #[snafu(display("IO error ({kind:?}): {context}"))]
    Io { kind: IoErrorKind, context: String },

    #[snafu(display("IPC channel disconnected"))]
    Disconnected,

    #[snafu(display("Unsupported or invalid data type: {datatype}"))]
    DataType { datatype: Cow<'static, str> },

    #[snafu(display("GDAL operational error ({kind:?}): {details}"))]
    GdalError {
        kind: GdalErrorKind,
        details: String,
    },

    #[snafu(display("Raster Arrow conversion failure: {msg}"))]
    RasterArrowConversion { msg: String },

    #[snafu(display("Unhandled worker exception: {msg}"))]
    IpcOther { msg: String },

    #[snafu(display("Gdal buffer len {buffer_len} does not match grid shape {shape:?}"))]
    ShapeBufferMissmatch {
        shape: GridShape2D,
        buffer_len: usize,
    },

    #[snafu(display("Error casting data to bytes for IPC transmission: {error}"))]
    PocCastError { error: String },
}

impl IpcProcessError {
    /// Convenience checker for your parent-side retry loop and fallback handlers
    pub fn is_file_not_found(&self) -> bool {
        match self {
            Self::GdalError {
                kind: GdalErrorKind::FileNotFound,
                ..
            } => true,
            Self::Io {
                kind: IoErrorKind::NotFound,
                ..
            } => true,
            _ => false,
        }
    }
}

impl From<std::io::Error> for IpcProcessError {
    fn from(err: std::io::Error) -> Self {
        let kind = match err.kind() {
            std::io::ErrorKind::NotFound => IoErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied => IoErrorKind::PermissionDenied,
            std::io::ErrorKind::ConnectionRefused => IoErrorKind::ConnectionRefused,
            std::io::ErrorKind::ConnectionAborted => IoErrorKind::ConnectionAborted,
            std::io::ErrorKind::UnexpectedEof => IoErrorKind::UnexpectedEof,
            _ => IoErrorKind::Other,
        };
        IpcProcessError::Io {
            kind,
            context: err.to_string(),
        }
    }
}

impl From<gdal::errors::GdalError> for IpcProcessError {
    /// Maps a raw GDAL library error into a strongly-typed structural variant
    /// before serialization across the IPC channel.
    fn from(err: gdal::errors::GdalError) -> Self {
        let err_string = err.to_string();

        // Parse the error structural context cleanly
        let kind = match &err {
            gdal::errors::GdalError::NullPointer { .. } => GdalErrorKind::InvalidDataset,
            gdal::errors::GdalError::OgrError { .. } => GdalErrorKind::InvalidDataset,
            _ if err_string.contains("No such file or directory") || err_string.contains("404") => {
                GdalErrorKind::FileNotFound
            }
            _ if err_string.contains("Permission denied") || err_string.contains("403") => {
                GdalErrorKind::AccessDenied
            }
            _ if err_string.contains("timeout") || err_string.contains("Connection timed out") => {
                GdalErrorKind::ConnectionTimeout
            }
            _ => GdalErrorKind::Unknown,
        };

        IpcProcessError::GdalError {
            kind,
            details: err_string,
        }
    }
}

impl From<IpcError> for IpcProcessError {
    fn from(err: IpcError) -> Self {
        match err {
            IpcError::SerializationError(serde_err) => IpcProcessError::SerializationError {
                msg: std::borrow::Cow::Owned(serde_err.to_string()),
            },
            IpcError::Io(io_err) => {
                // Automatically delegates to our optimized From<std::io::Error> implementation
                IpcProcessError::from(io_err)
            }
            IpcError::Disconnected => IpcProcessError::Disconnected,
        }
    }
}

impl From<GdalRasterLoaderError> for IpcProcessError {
    fn from(value: GdalRasterLoaderError) -> Self {
        match value {
            GdalRasterLoaderError::GdalError { source } => IpcProcessError::from(source),
            GdalRasterLoaderError::GdalFileNotFound { source } => IpcProcessError::GdalError {
                kind: GdalErrorKind::FileNotFound,
                details: source.to_string(),
            },
            GdalRasterLoaderError::ShapeBufferMissmatch {
                shape: _,
                buffer_len: _,
            } => todo!(),
            GdalRasterLoaderError::AlphaBandAsMaskNotAllowed => todo!(),
        }
    }
}

impl From<bytemuck::PodCastError> for IpcProcessError {
    fn from(error: bytemuck::PodCastError) -> Self {
        IpcProcessError::PocCastError {
            error: error.to_string(),
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

pub struct ChildProcessGuard {
    child: std::process::Child,
}

impl Drop for ChildProcessGuard {
    fn drop(&mut self) {
        // Forcefully kill the process when the guard is dropped
        let _ = self.child.kill();
        let _ = self.child.wait(); // Prevent zombie processes
    }
}

pub fn spawn_ipc_server_process<S, R>() -> (ChildProcessGuard, IpcSender<S>, IpcReceiver<R>) {
    let (server, token) = ipc::IpcOneShotServer::<(IpcSender<S>, IpcReceiver<R>)>::new()
        .expect("Failed to create IPC Server");

    tracing::debug!("spawn_ipc_server token: {}", token);

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
    (ChildProcessGuard { child }, channels.0, channels.1)
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

/// A simple, single-entry cache for an open GDAL dataset tile.
#[derive(Default)]
pub struct GdalDatasetCache {
    params: Option<GdalDatasetParameters>,
    dataset: Option<GdalDataset>,
    thread_local: Option<TemporaryGdalThreadLocalConfigOptions>,
}

struct PrevDatasetOpt {
    _prev_dataset: Option<GdalDataset>,
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

    pub fn get_or_open(
        &mut self,
        params: &GdalDatasetParameters,
    ) -> Result<&mut GdalDataset, GdalError> {
        // FIXME: use error type here!
        let hit = Self::is_hit(self.params.as_ref(), params);

        if hit {
            Ok(self.dataset.as_mut().expect("Dataset must be set"))
        } else {
            let _prev_dataset = self.clear(); // keep old dataset alive untill new one to keep Gdal internal cache alive

            let (ds, tlo) = Self::open_ex(params)?;

            self.params = Some(params.clone());
            self.dataset = Some(ds);
            self.thread_local = tlo;

            Ok(self.dataset.as_mut().expect("Dataset must be set"))
        }
    }

    fn is_hit(params: Option<&GdalDatasetParameters>, other: &GdalDatasetParameters) -> bool {
        if let Some(cached) = params {
            cached.file_path == other.file_path
                && cached.gdal_open_options == other.gdal_open_options
                && cached.gdal_config_options == other.gdal_config_options
        } else {
            false
        }
    }

    fn clear(&mut self) -> PrevDatasetOpt {
        let prev = PrevDatasetOpt {
            _prev_dataset: self.dataset.take(),
        };
        self.params = None;
        self.thread_local = None;
        prev
    }

    pub fn contains(&self, params: &GdalDatasetParameters) -> bool {
        Self::is_hit(self.params.as_ref(), params)
    }
}
