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

/// W3C Trace Context propagated from the parent process to the GDAL worker
/// so worker spans are nested under the parent operator's trace.
///
/// `traceparent` is required when a trace context exists; `tracestate` is an
/// optional vendor-specific payload that extends the trace context.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TraceContext {
    /// W3C Trace Context `traceparent` header (e.g. `00-<trace-id>-<span-id>-01`).
    pub traceparent: String,
    /// W3C Trace Context `tracestate` header, an optional vendor-specific payload.
    #[serde(default)]
    pub tracestate: Option<String>,
}

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

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct IpcChannelMessagePayload {
    pub dataset_params: GdalDatasetParameters,
    pub read_advise: GdalReadAdvise,
    /// We use this to know what type we serialize
    pub data_type: RasterDataType,
    /// W3C Trace Context propagated from the parent process. When `Some`, the
    /// worker attaches its spans under this trace so that GDAL operations are
    /// correctly nested under the parent operator's span tree.
    #[serde(default)]
    pub trace_context: Option<TraceContext>,
}

pub type IpcProcessRasterResult = Result<GdalIpcBytePayload, IpcProcessError>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct GdalIpcBytePayload {
    pub dimensions: GridBoundingBox2D,
    pub properties: GdalIpcRasterProperties,
    pub data_variant: GdalDataGridByteVariant,
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

        // Hash read advise
        self.0.read_advise.hash(state);
        self.0.data_type.hash(state);
    }
}

/// A simple carrier that implements `opentelemetry::propagation::Injector` and `Extractor`
/// for propagating W3C Trace Context across IPC boundaries. Used to transmit the parent
/// span's trace context from the main process to GDAL worker subprocesses.
#[derive(Default)]
pub struct TraceContextCarrier {
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
}

impl opentelemetry::propagation::Injector for TraceContextCarrier {
    fn set(&mut self, key: &str, value: String) {
        match key {
            "traceparent" => self.traceparent = Some(value),
            "tracestate" => self.tracestate = Some(value),
            _ => {}
        }
    }
}

impl opentelemetry::propagation::Extractor for TraceContextCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        match key {
            "traceparent" => self.traceparent.as_deref(),
            "tracestate" => self.tracestate.as_deref(),
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        let mut keys = Vec::new();
        if self.traceparent.is_some() {
            keys.push("traceparent");
        }
        if self.tracestate.is_some() {
            keys.push("tracestate");
        }
        keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::gdal_worker_process::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
    };
    use geoengine_datatypes::raster::{GridBoundingBox2D, RasterDataType};
    use opentelemetry::{
        propagation::TextMapPropagator as _,
        trace::{SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId, TraceState},
    };
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use std::path::PathBuf;

    /// Verify that `TraceContextCarrier` can round-trip a valid W3C trace context
    /// through inject → extract without losing information.
    #[test]
    fn test_trace_context_carrier_roundtrip() {
        let propagator = TraceContextPropagator::new();

        // Build a remote span context with known IDs
        let span_context = SpanContext::new(
            TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap(),
            SpanId::from_hex("deadbeefcafe1234").unwrap(),
            TraceFlags::new(1),
            true,
            TraceState::default(),
        );
        let original_cx =
            opentelemetry::Context::new().with_remote_span_context(span_context.clone());

        // Inject
        let mut carrier = TraceContextCarrier::default();
        propagator.inject_context(&original_cx, &mut carrier);
        assert!(
            carrier.traceparent.is_some(),
            "traceparent should be set after injection"
        );

        // Extract
        let extracted_cx = propagator.extract(&carrier);
        let extracted_span = extracted_cx.span();
        let extracted_sc = extracted_span.span_context();

        assert_eq!(
            extracted_sc.trace_id(),
            span_context.trace_id(),
            "trace_id should match after round-trip"
        );
        assert_eq!(
            extracted_sc.span_id(),
            span_context.span_id(),
            "span_id should match after round-trip"
        );
        assert!(
            extracted_sc.is_remote(),
            "extracted span should be marked remote"
        );
    }

    /// Verify that `TraceContextCarrier` handles an empty (no span) context gracefully.
    #[test]
    fn test_trace_context_carrier_empty_context() {
        let propagator = TraceContextPropagator::new();
        let empty_cx = opentelemetry::Context::new();

        let mut carrier = TraceContextCarrier::default();
        propagator.inject_context(&empty_cx, &mut carrier);

        // With no active span, the propagator may or may not inject a traceparent.
        // The important thing is that extract does not panic.
        let _extracted = propagator.extract(&carrier);
    }

    /// Verify that `IpcChannelMessagePayload` with `traceparent`/`tracestate` fields
    /// survives an IPC channel round-trip.
    #[test]
    #[serial_test::serial]
    fn test_ipc_message_with_trace_context_roundtrip() {
        let payload = IpcChannelMessagePayload {
            dataset_params: GdalDatasetParameters {
                file_path: PathBuf::from("/some/test/file.tif"),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: (0.0, 0.0).into(),
                    x_pixel_size: 1.0,
                    y_pixel_size: -1.0,
                },
                width: 100,
                height: 100,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: Some(0.),
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: true,
                retry: None,
            },
            read_advise: GdalReadAdvise {
                gdal_read_widow: GdalReadWindow::new([0, 0].into(), [8, 8].into()),
                read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
                bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
                flip_y: false,
            },
            data_type: RasterDataType::U8,
            trace_context: Some(TraceContext {
                traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
                tracestate: Some("rojo=00f067aa0ba902b7".to_string()),
            }),
        };

        let msg = IpcChannelMessage::new_request_tile_message(payload.clone());

        let (sender, receiver) = ipc_channel::ipc::channel().unwrap();
        sender.send(msg.clone()).unwrap();
        let recv = receiver.recv().unwrap();

        assert_eq!(
            msg, recv,
            "IPC message with trace context should round-trip unchanged"
        );
    }
}
