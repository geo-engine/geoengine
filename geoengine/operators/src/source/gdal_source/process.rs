use std::borrow::Cow;
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use bytemuck::{AnyBitPattern, NoUninit};
use gdal::errors::GdalError;
use gdal::{Dataset as GdalDataset, DatasetOptions, GdalOpenFlags};

use geoengine_datatypes::raster::{
    Grid, GridBoundingBox2D, GridOrEmpty, GridShape2D, MaskedGrid, NoDataValueGrid, Pixel,
    RasterDataType, RasterProperties, RasterPropertiesEntry, RasterPropertiesKey,
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

/// Global storage for the detected path, initialized on first access.
static GDALSOURCE_PROCESS_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Returns a reference to the cached `gdalsource-process` path.
/// The detection logic runs exactly once on the very first call.
fn get_gdalsource_path() -> &'static Path {
    GDALSOURCE_PROCESS_PATH.get_or_init(|| {
        // 1. Try the environment variable path first
        if let Ok(env_path) = env::var("GDALSOURCE_PROCESS_PATH")
            && !env_path.is_empty()
        {
            let path = PathBuf::from(env_path);
            if path.is_file() {
                tracing::debug!(
                    "Using gdalsource-process path from environment variable: {}",
                    path.display()
                );
                return path;
            }
        }

        // 2. Fallback detection logic
        let mut exe_path = env::current_exe()
            .unwrap_or_else(|_| env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));

        if exe_path.is_file() {
            exe_path.pop();
        }

        if exe_path.file_name().is_some_and(|name| name == "deps") {
            exe_path.pop();
        }

        let binary_name = if cfg!(windows) {
            "gdalsource-process.exe"
        } else {
            "gdalsource-process"
        };
        exe_path.push(binary_name);

        tracing::debug!("Detected gdalsource-process path: {}", exe_path.display());

        exe_path
    })
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct IpcChannelMessagePayload {
    pub dataset_params: GdalDatasetParameters,
    pub read_advise: GdalReadAdvise,
    /// We use this to know what type we serialize
    pub data_type: RasterDataType,
}

pub type IpcProcessRasterResult = Result<GdalIpcBytePayload, IpcProcessError>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct GdalIpcBytePayload {
    pub dimensions: GridBoundingBox2D,
    pub properties: GdalIpcRasterProperties,
    pub data_variant: GdalDataByteVariant,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum GdalIpcRasterPropertiesEntry {
    Number(f64),
    String(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GdalIpcRasterProperties {
    pub offset: Option<f64>,
    pub scale: Option<f64>,
    pub description: Option<String>,
    pub properties_map: Vec<(RasterPropertiesKey, GdalIpcRasterPropertiesEntry)>,
}

impl From<RasterProperties> for GdalIpcRasterProperties {
    fn from(
        RasterProperties {
            offset,
            scale,
            description,
            properties_map,
        }: RasterProperties,
    ) -> Self {
        Self {
            offset,
            scale,
            description,
            properties_map: properties_map
                .into_iter()
                .map(|(k, v)| {
                    let v_converted = match v {
                        RasterPropertiesEntry::Number(n) => GdalIpcRasterPropertiesEntry::Number(n),
                        RasterPropertiesEntry::String(s) => GdalIpcRasterPropertiesEntry::String(s),
                    };
                    (k, v_converted)
                })
                .collect(),
        }
    }
}

impl From<GdalIpcRasterProperties> for RasterProperties {
    fn from(
        GdalIpcRasterProperties {
            offset,
            scale,
            description,
            properties_map,
        }: GdalIpcRasterProperties,
    ) -> Self {
        Self {
            offset,
            scale,
            description,
            properties_map: properties_map
                .into_iter()
                .map(|(k, v)| {
                    let v_converted = match v {
                        GdalIpcRasterPropertiesEntry::Number(n) => RasterPropertiesEntry::Number(n),
                        GdalIpcRasterPropertiesEntry::String(s) => RasterPropertiesEntry::String(s),
                    };
                    (k, v_converted)
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GdalIpcRasterPropertiesKey {
    pub domain: Option<String>,
    pub key: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
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
            properties: value.properties.into(),
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
            properties: value.properties.into(),
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
                    validity_mask,
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
                validity_mask,
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
                        Grid::new(value.dimensions, data).expect("shape and data match"),
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

#[derive(Debug, Snafu, Serialize, Deserialize, Clone, PartialEq)]
#[snafu(visibility(pub))]
pub enum IpcProcessError {
    #[snafu(display("Serialization error: {msg}"))]
    SerializationError {
        // Cow allows zero-allocation compilation-string sharing on creation.
        // On the receiving side of ipc-channel, serde automatically hydrates this into Cow::Owned.
        msg: String,
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
        matches!(
            self,
            Self::GdalError {
                kind: GdalErrorKind::FileNotFound,
                ..
            } | Self::Io {
                kind: IoErrorKind::NotFound,
                ..
            }
        )
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
            gdal::errors::GdalError::NullPointer { .. }
            | gdal::errors::GdalError::OgrError { .. } => GdalErrorKind::InvalidDataset,
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
                msg: serde_err.to_string(),
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
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

/// Spawns the IPC server process and establishes the communication channels.
///
/// Returns a guard for the child process (which will automatically clean up on drop),
/// as well as the sender and receiver for communication with the server.
/// Assumes that the server executable is located at the path specified by the `GDAL_SOURCE_PROCESS_PATH` environment variable,
/// or defaults to a sibling executable named `gdalsource-process` in the same directory as the current executable if the environment variable is not set.
///
/// # Errors
/// Returns an `IpcProcessError` if the server process fails to start, or if the IPC channels fail to establish communication.
///
/// # Panics
/// Panics if the current executable path cannot be determined, or if the executable has no parent directory, which should not happen under normal circumstances.
pub fn spawn_ipc_server_process<S, R>()
-> Result<(ChildProcessGuard, IpcSender<S>, IpcReceiver<R>), IpcProcessError> {
    let (server, token) = ipc::IpcOneShotServer::<(IpcSender<S>, IpcReceiver<R>)>::new()
        .map_err(IpcProcessError::from)?;

    tracing::debug!("spawn_ipc_server token: {}", token);

    let exe_path = get_gdalsource_path();

    let child = Command::new(exe_path)
        .arg(token)
        .arg("debug") // FIXME: paste log level here!
        .env_remove("LLVM_PROFILE_FILE")
        .env_remove("LLVM_PROFILE_FILE_NAME")
        .stderr(std::process::Stdio::inherit()) // This sends child logs to the parent's stderr
        .spawn()
        .map_err(IpcProcessError::from)?;

    let (_rx, channels) = server.accept().map_err(IpcProcessError::from)?;
    Ok((ChildProcessGuard { child }, channels.0, channels.1))
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

    /// Retrieves the cached dataset if the parameters match, otherwise opens a new dataset with the given parameters, updates the cache, and returns it.
    /// The previous dataset is kept alive until the new one is successfully opened to maintain GDAL's internal caching benefits.
    /// This method ensures that only one dataset is open at a time, and that the cache is updated atomically to prevent stale data.
    /// The caller is responsible for ensuring that the returned dataset is not used concurrently across threads, as GDAL datasets are generally not thread-safe.
    /// # Errors
    /// Returns a `GdalError` if opening the new dataset fails. In this case, the cache remains unchanged and the previous dataset (if any) is still valid.
    ///
    /// # Panics
    /// Panics if the dataset is unexpectedly missing after a cache hit, which should never happen if the cache logic is correct.
    /// Panics if the thread local configuration options fail to apply, which should also not happen under normal circumstances.
    /// Panics if the caller attempts to use the returned dataset concurrently across threads, which is not allowed due to GDAL's thread safety constraints.
    pub fn get_or_open(
        &mut self,
        params: &GdalDatasetParameters,
    ) -> Result<&mut GdalDataset, GdalError> {
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
        // TODO: we could optimize this by hashing the parameters and comparing the hash for a quick check before doing the full equality check, if it turns out to be a bottleneck.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipc_process_error_ipc_channel_roundtrip() {
        let error = IpcProcessError::GdalError {
            kind: GdalErrorKind::FileNotFound,
            details: "not found".into(),
        };

        let (sender, receiver) = ipc::channel::<IpcProcessRasterResult>().unwrap();
        sender.send(Err(error.clone())).unwrap();
        let decoded = receiver.recv().unwrap();

        assert_eq!(decoded, Err(error));
    }
}
