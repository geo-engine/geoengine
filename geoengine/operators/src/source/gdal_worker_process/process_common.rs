use bytemuck::{AnyBitPattern, NoUninit};
use geoengine_datatypes::raster::{
    Grid, GridBoundingBox2D, GridIdx2D, GridOrEmpty, GridShape2D, GridSize, MaskedGrid,
    NoDataValueGrid, Pixel, RasterDataType, RasterProperties, RasterPropertiesEntry,
    RasterPropertiesKey,
};
use ipc_channel::IpcError;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::{
    borrow::Cow,
    hash::{Hash, Hasher},
};

use super::{GdalDatasetParameters, GridAndProperties};

/// This struct is used to advise the GDAL reader how to read the data from the dataset.
/// The Workflow is as follows:
/// 1. The `gdal_read_window` is the window in the pixel space of the dataset that should be read.
/// 2. The `read_window_bounds` is the area in the target pixel space where the data should be placed.
///    2.1 The data read in step one is read to the width and height of the `read_window_bounds`.
///    2.2 if `flip_y` is true the data is flipped in the y direction. And should be unflipped after reading.
/// 3. The `bounds_of_target` is the area in the target pixel space where the data should be placed.
///    3.1 The `read_window_bounds` might be offset from the `bounds_of_target` or might have a different size.
///    Then, the data needs to be placed in the target pixel space accordingly. Other parts of the target pixel space should be filled with nodata.
#[allow(dead_code)]
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize, Hash)]
pub struct GdalReadAdvise {
    pub gdal_read_widow: GdalReadWindow,
    pub read_window_bounds: GridBoundingBox2D,
    pub bounds_of_target: GridBoundingBox2D,
    pub flip_y: bool,
}

impl GdalReadAdvise {
    pub fn direct_read(&self) -> bool {
        self.read_window_bounds == self.bounds_of_target
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct GdalReadWindow {
    pub(crate) start_x: isize, // pixelspace origin
    pub(crate) start_y: isize,
    pub(crate) size_x: usize, // pixelspace size
    pub(crate) size_y: usize,
}

/// The role of one source-array dimension in a multidimensional raster read.
///
/// A dimension that is neither spatial nor time is pinned with [`Self::Fixed`], so a source
/// whose band or level dimension is not the time axis is read as one variable per fixed index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum MdAxisMapping {
    X,
    Y,
    /// The z axis as time steps: every slice is one time step of the array.
    Time,
    Fixed {
        index: i64,
    },
}

impl MdAxisMapping {
    /// Whether this dimension is the z axis a read slices, i.e. `Time`.
    #[must_use]
    pub fn is_z(self) -> bool {
        matches!(self, Self::Time)
    }
}

/// Validates the roles of an axis mapping: exactly one X, exactly one Y, at most one Time.
///
/// Needs no axis sizes, so it also runs where the array is not opened (e.g. external MD rows).
pub fn validate_md_axis_roles(mapping: &[MdAxisMapping]) -> Result<(), String> {
    let (mut x_count, mut y_count, mut z_count) = (0, 0, 0);
    for role in mapping {
        match role {
            MdAxisMapping::X => x_count += 1,
            MdAxisMapping::Y => y_count += 1,
            MdAxisMapping::Time => z_count += 1,
            MdAxisMapping::Fixed { .. } => {}
        }
    }

    if (x_count, y_count) != (1, 1) || z_count > 1 {
        return Err(format!(
            "axis mapping must contain exactly one X and one Y, and at most one Time axis; found {x_count} X, {y_count} Y and {z_count} Time"
        ));
    }

    Ok(())
}

/// Validates one mapping entry per source dimension and checks fixed indices against sizes.
pub fn validate_md_axis_mapping(
    mapping: &[MdAxisMapping],
    axis_sizes: &[usize],
) -> Result<(), String> {
    if mapping.len() != axis_sizes.len() {
        return Err(format!(
            "axis mapping has {} entries for an array with {} dimensions",
            mapping.len(),
            axis_sizes.len()
        ));
    }

    for (axis, (&role, &size)) in mapping.iter().zip(axis_sizes).enumerate() {
        if let MdAxisMapping::Fixed { index } = role {
            let index = usize::try_from(index).map_err(|_| {
                format!("fixed index {index} for dimension {axis} must be non-negative")
            })?;
            if index >= size {
                return Err(format!(
                    "fixed index {index} for dimension {axis} is outside its size {size}"
                ));
            }
        }
    }

    validate_md_axis_roles(mapping)
}

impl GdalReadWindow {
    pub fn new(start: GridIdx2D, size: GridShape2D) -> Self {
        Self {
            start_x: start.x(),
            start_y: start.y(),
            size_x: size.axis_size_x(),
            size_y: size.axis_size_y(),
        }
    }

    pub fn gdal_window_start(&self) -> (isize, isize) {
        (self.start_x, self.start_y)
    }

    pub fn gdal_window_size(&self) -> (usize, usize) {
        (self.size_x, self.size_y)
    }
}

/// What a [`IpcChannelMessagePayload`] asks the worker for: pixels, or only a header.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Hash)]
pub enum GdalRequest {
    /// Read pixels of the window `read_advise` as `data_type`.
    Read {
        read_advise: GdalReadAdvise,
        /// We use this to know what type we serialize
        data_type: RasterDataType,
        kind: GdalReadKind,
    },
    /// Read a header only, without any pixel data.
    Header(GdalHeaderKind),
}

/// The kind of pixel read: a classic 2D rasterband read or a multidimensional (MD) array read.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Hash)]
pub enum GdalReadKind {
    /// Classic 2D rasterband read (`rasterband_channel` selects the band).
    Raster,
    /// Multidim array read: read the array `array_name` (in the MD group `group`,
    /// `None` = root group) at the z-slice `z_index` (file-local).
    MdArray {
        /// The MD group to read the array from, or `None` for the root group.
        group: Option<String>,
        // The name of the array to read.
        array_name: String,
        // The z-slice index to read, in file-local coordinates.
        z_index: usize,
        /// Role of each source-array dimension, in source order.
        axis_mapping: Vec<MdAxisMapping>,
    },
}

/// The kind of header read.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Hash)]
pub enum GdalHeaderKind {
    /// The data type of band `rasterband_channel` and the dataset's CRS.
    Raster,
    /// The shape and declared CRS of the MD array `array_name` (in `group`).
    MdArray {
        group: Option<String>,
        array_name: String,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct IpcChannelMessagePayload {
    pub dataset_params: GdalDatasetParameters,
    /// Correlation ID for tracing: `{hash}-{nanos}`. Used to link the main process
    /// caller span with the worker process span for the same logical read.
    #[serde(default)]
    pub read_id: Option<String>,
    pub request: GdalRequest,
}

impl IpcChannelMessagePayload {
    /// The pixel window a read targets; `None` for header reads.
    pub fn read_window_bounds(&self) -> Option<GridBoundingBox2D> {
        match &self.request {
            GdalRequest::Read { read_advise, .. } => Some(read_advise.read_window_bounds),
            GdalRequest::Header(_) => None,
        }
    }
}

/// Result payload of a worker read: a classic 2D raster payload, or a multidim slice payload.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum IpcChannelResult {
    /// Classic 2D rasterband read result.
    Raster(GdalIpcBytePayload),
    /// Multidim read result: the single z-slice requested by [`GdalReadKind::MdArray`].
    /// Uses the same byte encoding as `Raster`.
    MdSlice(GdalIpcMdBytePayload),
    /// Header result of [`GdalHeaderKind::MdArray`].
    MdArrayInfo(MdArrayInfo),
    /// Header result of [`GdalHeaderKind::Raster`].
    RasterInfo(RasterInfo),
}

/// The data type of one band and the CRS of a classic raster, as read from its header.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct RasterInfo {
    pub data_type: RasterDataType,
    pub spatial_reference: geoengine_datatypes::spatial_reference::SpatialReference,
}

/// The shape and declared CRS of a multidim array, as read from its header.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct MdArrayInfo {
    /// the size of each array dimension, in source order
    pub axis_sizes: Vec<usize>,
    /// the array's CRS as GDAL resolves it, else its `crs`, `spatial_ref` or `grid_mapping`
    /// attribute, if any
    pub crs: Option<geoengine_datatypes::spatial_reference::SpatialReference>,
    /// one entry per dimension, in source order: its coordinate variable, if it has a 1-D one
    /// in degrees or metres
    pub coordinates: Vec<Option<MdAxisCoordinates>>,
}

/// The cell-centre coordinates along one dimension of an MD array.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub struct MdAxisCoordinates {
    pub first: f64,
    pub last: f64,
    /// whether the spacing is constant, i.e. an affine transform can describe the axis
    pub uniform: bool,
}

pub type IpcProcessRasterResult = Result<IpcChannelResult, IpcProcessError>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct GdalIpcBytePayload {
    pub dimensions: GridBoundingBox2D,
    pub properties: GdalIpcRasterProperties,
    pub data_variant: GdalDataGridByteVariant,
}

/// Raw row-major multidimensional slice returned by the GDAL worker.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct GdalIpcMdBytePayload {
    pub axis_sizes: Vec<usize>,
    pub properties: GdalIpcRasterProperties,
    pub data_variant: GdalDataGridByteVariant,
}

#[derive(Debug, Clone)]
pub struct GdalIpcMdPayload<T> {
    pub axis_sizes: Vec<usize>,
    pub properties: RasterProperties,
    pub data_variant: GdalDataGridVariant<T>,
}

impl<T> TryFrom<GdalIpcMdPayload<T>> for GdalIpcMdBytePayload
where
    T: NoUninit,
{
    type Error = bytemuck::PodCastError;

    fn try_from(value: GdalIpcMdPayload<T>) -> Result<Self, Self::Error> {
        Ok(Self {
            axis_sizes: value.axis_sizes,
            properties: value.properties.into(),
            data_variant: value.data_variant.try_into()?,
        })
    }
}

impl<T> TryFrom<&GdalIpcMdBytePayload> for GdalIpcMdPayload<T>
where
    T: AnyBitPattern,
{
    type Error = bytemuck::PodCastError;

    fn try_from(value: &GdalIpcMdBytePayload) -> Result<Self, Self::Error> {
        Ok(Self {
            axis_sizes: value.axis_sizes.clone(),
            properties: value.properties.clone().into(),
            data_variant: (&value.data_variant).try_into()?,
        })
    }
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
pub enum GdalDataGridByteVariant {
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
    pub data_variant: GdalDataGridVariant<T>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum GdalDataGridVariant<T> {
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

impl<T> TryFrom<GdalDataGridVariant<T>> for GdalDataGridByteVariant
where
    T: NoUninit,
{
    type Error = bytemuck::PodCastError;

    fn try_from(value: GdalDataGridVariant<T>) -> Result<Self, Self::Error> {
        match value {
            GdalDataGridVariant::Empty => Ok(GdalDataGridByteVariant::Empty),
            GdalDataGridVariant::AllValid { data } => {
                bytemuck::try_cast_slice::<T, u8>(&data).map(|s| {
                    GdalDataGridByteVariant::AllValid {
                        raw_bytes: s.to_vec(),
                    }
                })
            }
            GdalDataGridVariant::WithNoData {
                data,
                no_data_value,
            } => bytemuck::try_cast_slice::<T, u8>(&data).map(|s| {
                GdalDataGridByteVariant::WithNoData {
                    raw_bytes: s.to_vec(),
                    no_data_value,
                }
            }),
            GdalDataGridVariant::WithExplicitMask {
                data,
                validity_mask,
            } => bytemuck::try_cast_slice::<T, u8>(&data).map(|s| {
                GdalDataGridByteVariant::WithExplicitMask {
                    raw_bytes: s.to_vec(),
                    validity_mask,
                }
            }),
        }
    }
}

impl<T> TryFrom<&GdalDataGridByteVariant> for GdalDataGridVariant<T>
where
    T: AnyBitPattern,
{
    type Error = bytemuck::PodCastError;

    fn try_from(value: &GdalDataGridByteVariant) -> Result<Self, Self::Error> {
        match value {
            GdalDataGridByteVariant::Empty => Ok(GdalDataGridVariant::Empty),
            GdalDataGridByteVariant::AllValid { raw_bytes } => {
                // We borrow the well-aligned bytes from the Arc, cast them, and allocate Vec<T>
                let data = bytemuck::try_cast_slice::<u8, T>(raw_bytes)?.to_vec();
                Ok(GdalDataGridVariant::AllValid { data })
            }
            GdalDataGridByteVariant::WithNoData {
                raw_bytes,
                no_data_value,
            } => {
                let data = bytemuck::try_cast_slice::<u8, T>(raw_bytes)?.to_vec();
                Ok(GdalDataGridVariant::WithNoData {
                    data,
                    no_data_value: *no_data_value,
                })
            }
            GdalDataGridByteVariant::WithExplicitMask {
                raw_bytes,
                validity_mask,
            } => {
                let data = bytemuck::try_cast_slice::<u8, T>(raw_bytes)?.to_vec();
                Ok(GdalDataGridVariant::WithExplicitMask {
                    data,
                    validity_mask: validity_mask.clone(), // Mask is u8, so clone is cheap and safe
                })
            }
        }
    }
}

impl<T> TryFrom<&GdalIpcBytePayload> for GdalIpcPayload<T>
where
    T: bytemuck::AnyBitPattern,
{
    type Error = bytemuck::PodCastError;

    fn try_from(value: &GdalIpcBytePayload) -> Result<Self, Self::Error> {
        Ok(Self {
            dimensions: value.dimensions,
            properties: value.properties.clone().into(),
            data_variant: (&value.data_variant).try_into()?,
        })
    }
}

impl<T> From<GdalIpcPayload<T>> for GridAndProperties<T, GridBoundingBox2D>
where
    T: Pixel,
{
    fn from(value: GdalIpcPayload<T>) -> Self {
        match value.data_variant {
            GdalDataGridVariant::Empty => GridAndProperties {
                grid: GridOrEmpty::new_empty_shape(value.dimensions),
                properties: value.properties,
            },
            GdalDataGridVariant::AllValid { data } => GridAndProperties {
                grid: GridOrEmpty::new_grid(MaskedGrid::new_with_data(
                    Grid::new(value.dimensions, data).expect("shape and data match"),
                )),
                properties: value.properties,
            },
            GdalDataGridVariant::WithNoData {
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
            GdalDataGridVariant::WithExplicitMask {
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

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum IpcProcessGdalErrorKind {
    FileNotFound,
    AccessDenied,
    InvalidDataset,
    ConnectionTimeout,
    Unknown,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum IpcProcessIoErrorKind {
    NotFound,
    PermissionDenied,
    ConnectionRefused,
    ConnectionAborted,
    UnexpectedEof,
    Other,
}

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
    Io {
        kind: IpcProcessIoErrorKind,
        context: String,
    },

    #[snafu(display("IPC channel disconnected"))]
    Disconnected,

    #[snafu(display("Unsupported or invalid data type: {datatype}"))]
    DataType { datatype: Cow<'static, str> },

    #[snafu(display("GDAL operational error ({kind:?}): {details}"))]
    GdalError {
        kind: IpcProcessGdalErrorKind,
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

    #[snafu(display("The dataset uses alpha mask but its not allowed in dataset params"))]
    AlphaBandAsMaskNotAllowed,
}

impl IpcProcessError {
    /// Convenience checker for your parent-side retry loop and fallback handlers
    pub fn is_file_not_found(&self) -> bool {
        matches!(
            self,
            Self::GdalError {
                kind: IpcProcessGdalErrorKind::FileNotFound,
                ..
            } | Self::Io {
                kind: IpcProcessIoErrorKind::NotFound,
                ..
            }
        )
    }
}

impl From<std::io::Error> for IpcProcessError {
    fn from(err: std::io::Error) -> Self {
        let kind = match err.kind() {
            std::io::ErrorKind::NotFound => IpcProcessIoErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied => IpcProcessIoErrorKind::PermissionDenied,
            std::io::ErrorKind::ConnectionRefused => IpcProcessIoErrorKind::ConnectionRefused,
            std::io::ErrorKind::ConnectionAborted => IpcProcessIoErrorKind::ConnectionAborted,
            std::io::ErrorKind::UnexpectedEof => IpcProcessIoErrorKind::UnexpectedEof,
            _ => IpcProcessIoErrorKind::Other,
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
            gdal::errors::GdalError::NullPointer { method_name, msg }
                if *method_name == "GDALOpenEx"
                    && (*msg == "HTTP response code: 404"
                        || msg.ends_with("No such file or directory")) =>
            {
                IpcProcessGdalErrorKind::FileNotFound
            }
            gdal::errors::GdalError::NullPointer { .. }
            | gdal::errors::GdalError::OgrError { .. } => IpcProcessGdalErrorKind::InvalidDataset,
            _ if err_string.contains("No such file or directory") || err_string.contains("404") => {
                IpcProcessGdalErrorKind::FileNotFound
            }
            _ if err_string.contains("Permission denied") || err_string.contains("403") => {
                IpcProcessGdalErrorKind::AccessDenied
            }
            _ if err_string.contains("timeout") || err_string.contains("Connection timed out") => {
                IpcProcessGdalErrorKind::ConnectionTimeout
            }
            _ => IpcProcessGdalErrorKind::Unknown,
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

    pub fn full_hash<H: Hasher>(&self, state: &mut H) {
        self.0.dataset_params.full_hash(state);
        self.0.request.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md_axis_mapping_survives_ipc_encoding() {
        let mapping = vec![
            MdAxisMapping::Time,
            MdAxisMapping::Fixed { index: 3 },
            MdAxisMapping::Y,
            MdAxisMapping::X,
        ];
        let (sender, receiver) = ipc_channel::ipc::channel::<Vec<MdAxisMapping>>().unwrap();
        sender.send(mapping.clone()).unwrap();

        assert_eq!(receiver.recv().unwrap(), mapping);
    }

    #[test]
    fn validate_md_axis_mapping_cases() {
        use MdAxisMapping::{Fixed, Time, X, Y};

        assert!(validate_md_axis_mapping(&[Y, X], &[10, 20]).is_ok());
        assert!(validate_md_axis_mapping(&[Time, Y, X], &[3, 10, 20]).is_ok());
        assert!(
            validate_md_axis_mapping(&[Fixed { index: 1 }, Time, Y, X], &[2, 3, 10, 20]).is_ok()
        );

        assert!(validate_md_axis_mapping(&[Time, Time, Y, X], &[3, 2, 10, 20]).is_err());
        assert!(validate_md_axis_mapping(&[Time, Y], &[3, 10]).is_err());
        assert!(validate_md_axis_mapping(&[Y, X, X], &[10, 20, 20]).is_err());
        assert!(validate_md_axis_mapping(&[Fixed { index: 2 }, Y, X], &[2, 10, 20]).is_err());
        assert!(validate_md_axis_mapping(&[Y, X], &[10]).is_err());
    }

    #[test]
    fn validate_md_axis_roles_needs_no_sizes() {
        use MdAxisMapping::{Fixed, Time, X, Y};

        assert!(validate_md_axis_roles(&[Y, X]).is_ok());
        assert!(validate_md_axis_roles(&[Fixed { index: 7 }, Time, Y, X]).is_ok());

        assert!(validate_md_axis_roles(&[Time, Time, Y, X]).is_err());
        assert!(validate_md_axis_roles(&[Y]).is_err());
        assert!(validate_md_axis_roles(&[Y, X, X]).is_err());
    }
}
