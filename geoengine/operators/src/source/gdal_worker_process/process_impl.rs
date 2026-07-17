use super::{
    GdalDatasetGeoTransform, GdalDatasetParameters, GdalMetadataMapping,
    process_common::{
        GdalDataGridVariant, GdalIpcPayload, GdalReadAdvise, GdalReadWindow, IpcProcessError,
    },
};
use crate::{
    source::gdal_worker_process::process_common::IpcProcessGdalErrorKind,
    util::{GdalConfigOptions, gdal::gdal_open_ex_gdal_error, retry::retry_sync},
};
use float_cmp::approx_eq;
use gdal::{
    Dataset as GdalDataset, DatasetOptions, GdalOpenFlags, Metadata as GdalMetadata,
    errors::GdalError,
    raster::{GdalType, RasterBand as GdalRasterBand},
};
use gdal_sys::VSICurlPartialClearCache;
use geoengine_datatypes::{
    primitives::Coordinate2D,
    raster::{GridSize, Pixel, RasterProperties, RasterPropertiesEntry, RasterPropertiesEntryType},
};
use ipc_channel::ipc::{self, IpcReceiver, IpcSender};
use num::FromPrimitive;
use serde::{Deserialize, Serialize};
use std::{
    env,
    ffi::CString,
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
    time::Instant,
};
use tracing::debug;

/// Global storage for the detected path, initialized on first access.
static GDALSOURCE_PROCESS_PATH: OnceLock<PathBuf> = OnceLock::new();

static GDAL_RETRY_INITIAL_BACKOFF_MS: u64 = 1000;
static GDAL_RETRY_MAX_BACKOFF_MS: u64 = 60 * 60 * 1000;
static GDAL_RETRY_EXPONENTIAL_BACKOFF_FACTOR: f64 = 2.;

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

        let binary_name = format!("gdalsource-process{}", std::env::consts::EXE_SUFFIX);
        exe_path.push(binary_name);

        tracing::debug!("Detected gdalsource-process path: {}", exe_path.display());

        exe_path
    })
}

pub struct ChildProcessGuard {
    child: std::process::Child,
}

#[cfg(test)]
impl ChildProcessGuard {
    /// Wraps an already-running child in a guard. For unit tests only.
    pub(crate) fn from_child(child: std::process::Child) -> Self {
        Self { child }
    }
}

impl Drop for ChildProcessGuard {
    fn drop(&mut self) {
        // Forcefully kill the process when the guard is dropped
        let _ = self.child.kill();
        let _ = self.child.wait(); // Prevent zombie processes
    }
}

/// Configuration passed from the main process to each GDAL worker at spawn time via a JSON CLI argument.
/// The worker binary deserializes this instead of reading config files from disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    #[serde(default)]
    pub gdal_config_options: Option<Vec<(String, String)>>,
    #[serde(default)]
    pub logging: WorkerLoggingConfig,
    #[serde(default)]
    pub open_telemetry: OpenTelemetryConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerLoggingConfig {
    pub log_spec: String,
    #[serde(default)]
    pub log_to_file: bool,
    #[serde(default = "default_worker_log_prefix")]
    pub filename_prefix: String,
    pub log_directory: Option<String>,
    /// Set by the pool at spawn time. Used in log filenames to separate per-worker output.
    pub worker_id: usize,
}

fn default_worker_log_prefix() -> String {
    "geo_engine_worker".to_string()
}

impl Default for WorkerLoggingConfig {
    fn default() -> Self {
        Self {
            log_spec: "info".to_string(),
            log_to_file: false,
            filename_prefix: default_worker_log_prefix(),
            log_directory: None,
            worker_id: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenTelemetryConfig {
    pub enabled: bool,
    pub endpoint: String,
}

impl Default for OpenTelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://127.0.0.1:4317".to_string(),
        }
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            gdal_config_options: None,
            logging: WorkerLoggingConfig::default(),
            open_telemetry: OpenTelemetryConfig::default(),
        }
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
pub fn spawn_ipc_server_process<S, R>(
    worker_config: &WorkerConfig,
) -> Result<(ChildProcessGuard, IpcSender<S>, IpcReceiver<R>), IpcProcessError> {
    let (server, token) = ipc::IpcOneShotServer::<(IpcSender<S>, IpcReceiver<R>)>::new()
        .map_err(IpcProcessError::from)?;

    tracing::debug!("spawn_ipc_server token: {}", token);

    let exe_path = get_gdalsource_path();

    let config_json =
        serde_json::to_string(worker_config).expect("WorkerConfig should always serialize");

    let mut cmd = Command::new(exe_path);

    cmd.arg(token)
        .arg(&config_json)
        .stderr(std::process::Stdio::inherit());

    if std::cfg!(debug_assertions) {
        // llcov inserts these env params. We need to remove them from the gdal-processor processes. Otherwise the processes overwrite the main process data and the files become corrupt.
        cmd.env_remove("LLVM_PROFILE_FILE")
            .env_remove("LLVM_PROFILE_FILE_NAME");
    }

    let child = cmd.spawn().map_err(IpcProcessError::from)?;

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
pub struct GdalDatasetHolder {
    params: Option<GdalDatasetParameters>,
    dataset: Option<GdalDataset>,
    thread_local: Option<GdalConfigOptions>,
}

struct PrevDatasetOpt {
    _prev_dataset: Option<GdalDataset>,
}

impl GdalDatasetHolder {
    pub fn new() -> Self {
        Self {
            params: None,
            dataset: None,
            thread_local: None,
        }
    }

    fn open_ex(
        dataset_params: &GdalDatasetParameters,
    ) -> Result<(GdalDataset, Option<GdalConfigOptions>), GdalError> {
        let options = dataset_params
            .gdal_open_options
            .as_ref()
            .map(|o| o.iter().map(String::as_str).collect::<Vec<_>>());

        // reverts the process-global configs on drop
        let thread_local_configs: Option<GdalConfigOptions> = dataset_params
            .gdal_config_options_for_request()
            .as_ref()
            .map(|config_options| GdalConfigOptions::new(config_options))
            .transpose()
            .expect("GdalConfigOptions must not fail");

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

    /// Retrieves the open dataset if the parameters match, otherwise opens a new dataset with the given parameters, updates the stored dataset, and returns it.
    /// The current open dataset is kept alive until the new one is successfully opened to maintain GDAL's internal caching benefits.
    /// This method ensures that only one dataset is open at a time, and that the state is updated atomically to prevent stale data.
    /// The caller is responsible for ensuring that the returned dataset is not used concurrently across threads, as GDAL datasets are generally not thread-safe.
    /// # Errors
    /// Returns a `GdalError` if opening the new dataset fails. In this case, the stored dataset remains unchanged and the previous dataset (if any) is still valid.
    ///
    /// # Panics
    /// Panics if the dataset is unexpectedly missing after a it was detected as open, which should never happen.
    /// Panics if the configuration options fail to apply, which should also not happen under normal circumstances.
    /// Panics if the caller attempts to use the returned dataset concurrently across threads, which is not allowed due to GDAL's thread safety constraints.
    pub fn get_or_open(
        &mut self,
        params: &GdalDatasetParameters,
    ) -> Result<&mut GdalDataset, GdalError> {
        let hit = Self::is_hit(self.params.as_ref(), params);

        if hit {
            let _span = tracing::debug_span!("gdal_dataset_cache_hit").entered();
            Ok(self.dataset.as_mut().expect("Dataset must be set"))
        } else {
            let _prev_dataset = self.clear(); // keep old dataset alive untill new one to keep Gdal internal cache alive

            let _span = tracing::debug_span!(
                "gdal_dataset_open",
                path = %params.file_path.display(),
                band = params.rasterband_channel,
            )
            .entered();

            let (ds, tlo) = Self::open_ex(params)?;

            self.params = Some(params.clone());
            self.dataset = Some(ds);
            self.thread_local = tlo;

            Ok(self.dataset.as_mut().expect("Dataset must be set"))
        }
    }

    fn is_hit(params: Option<&GdalDatasetParameters>, other: &GdalDatasetParameters) -> bool {
        // TODO: we could optimize this by hashing the parameters and comparing the hash for a quick check before doing the full equality check, if it turns out to be a bottleneck.
        if let Some(current_ds_params) = params {
            current_ds_params.file_path == other.file_path
                && current_ds_params.gdal_open_options == other.gdal_open_options
                && current_ds_params.gdal_config_options == other.gdal_config_options
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

pub struct GdalHandling;

impl GdalHandling {
    pub fn error_is_gdal_file_not_found(error: &GdalError) -> bool {
        matches!(
            error,
            GdalError::NullPointer {
                    method_name,
                    msg
                }
             if *method_name == "GDALOpenEx" && (*msg == "HTTP response code: 404" || msg.ends_with("No such file or directory"))
        )
    }

    fn clear_gdal_vsi_cache_for_path(file_path: &Path) {
        unsafe {
            if let Some(Some(c_string)) = file_path.to_str().map(|s| CString::new(s).ok()) {
                VSICurlPartialClearCache(c_string.as_ptr());
            }
        }
    }

    ///
    /// A method to load single tiles from a GDAL dataset.
    ///
    pub fn load_tile_data_with_dataset_retry<T: Pixel + GdalType + FromPrimitive>(
        cache: &mut GdalDatasetHolder,
        dataset_params: &GdalDatasetParameters,
        read_advise: GdalReadAdvise,
    ) -> Result<super::process_common::GdalIpcPayload<T>, IpcProcessError> {
        let is_vsi_curl = dataset_params.is_vis_curl();
        let max_retries = dataset_params.max_retries().unwrap_or(0);
        let dp = &dataset_params;

        // Wrap both OPEN and READ actions inside the retry loop

        retry_sync(
            max_retries,
            GDAL_RETRY_INITIAL_BACKOFF_MS,
            GDAL_RETRY_EXPONENTIAL_BACKOFF_FACTOR,
            Some(GDAL_RETRY_MAX_BACKOFF_MS),
            || {
                let ds = match cache.get_or_open(dp) {
                    Ok(dataset) => dataset,
                    Err(gdal_error) => {
                        if is_vsi_curl {
                            Self::clear_gdal_vsi_cache_for_path(dp.file_path.as_path());
                        }
                        return Err(IpcProcessError::from(gdal_error));
                    }
                };

                Self::load_tile_data(ds, dataset_params, read_advise).inspect_err(|_e| {
                    if is_vsi_curl {
                        Self::clear_gdal_vsi_cache_for_path(dp.file_path.as_path());
                    }
                })
            },
            // If the file explicitly does not exist, do not waste time retrying
            |e| matches!(e, IpcProcessError::GdalError { kind, details: _ } if *kind == IpcProcessGdalErrorKind::FileNotFound),
        )
    }

    /// This method reads the data for a single grid with a specified size from the GDAL dataset.
    /// It fails if the tile is not within the dataset.
    #[allow(clippy::float_cmp)]
    pub fn read_grid_from_raster<T, D>(
        rasterband: &GdalRasterBand,
        read_window: &GdalReadWindow,
        out_shape: &D,
        dataset_params: &GdalDatasetParameters,
    ) -> Result<GdalDataGridVariant<T>, IpcProcessError>
    where
        T: Pixel + GdalType + Default + FromPrimitive,
        D: Clone + GridSize + PartialEq,
    {
        let _span = tracing::debug_span!(
            "gdal_rasterband_read",
            window = %format!("({},{})", read_window.size_x, read_window.size_y),
        )
        .entered();
        let gdal_out_shape = (out_shape.axis_size_x(), out_shape.axis_size_y());

        let read_start = Instant::now();
        let buffer = rasterband.read_as::<T>(
            read_window.gdal_window_start(), // pixelspace origin
            read_window.gdal_window_size(),  // pixelspace size
            gdal_out_shape,                  // requested raster size
            None,                            // sampling mode
        )?;
        let read_duration = read_start.elapsed();
        tracing::debug!(
            "GDAL rasterband read took {read_duration:?} for window size ({}, {})",
            read_window.size_x,
            read_window.size_y,
        );
        let (_, buffer_data) = buffer.into_shape_and_vec();

        let dataset_mask_flags = rasterband.mask_flags()?;

        if dataset_mask_flags.is_all_valid() {
            debug!("all pixels are valid --> skip no-data and mask handling.");
            return Ok(GdalDataGridVariant::AllValid { data: buffer_data });
        }

        if dataset_mask_flags.is_nodata() {
            debug!("raster uses a no-data value --> use no-data handling.");
            let no_data_value = dataset_params
                .no_data_value
                .or_else(|| rasterband.no_data_value());

            if let Some(no_data_value) = no_data_value {
                return Ok(GdalDataGridVariant::WithNoData {
                    data: buffer_data,
                    no_data_value,
                });
            }
            return Ok(GdalDataGridVariant::AllValid { data: buffer_data });
        }

        if dataset_mask_flags.is_alpha() {
            debug!("raster uses alpha band to mask pixels.");
            if !dataset_params.allow_alphaband_as_mask {
                return Err(IpcProcessError::AlphaBandAsMaskNotAllowed);
            }
        }

        debug!("use mask based no-data handling.");

        let mask_band = rasterband.open_mask_band()?;
        let mask_buffer = mask_band.read_as::<u8>(
            read_window.gdal_window_start(), // pixelspace origin
            read_window.gdal_window_size(),  // pixelspace size
            gdal_out_shape,                  // requested raster size
            None,                            // sampling mode
        )?;
        let (_, mask_buffer_data) = mask_buffer.into_shape_and_vec();
        Ok(GdalDataGridVariant::WithExplicitMask {
            data: buffer_data,
            validity_mask: mask_buffer_data,
        })
    }

    pub fn properties_from_gdal_metadata<'a, I, M>(
        properties: &mut RasterProperties,
        gdal_dataset: &M,
        properties_mapping: I,
    ) where
        I: IntoIterator<Item = &'a GdalMetadataMapping>,
        M: GdalMetadata,
    {
        let mapping_iter = properties_mapping.into_iter();

        for m in mapping_iter {
            let data = if let Some(domain) = &m.source_key.domain {
                gdal_dataset.metadata_item(&m.source_key.key, domain)
            } else {
                gdal_dataset.metadata_item(&m.source_key.key, "")
            };

            if let Some(d) = data {
                let entry = match m.target_type {
                    RasterPropertiesEntryType::Number => d.parse::<f64>().map_or_else(
                        |_| RasterPropertiesEntry::String(d),
                        RasterPropertiesEntry::Number,
                    ),
                    RasterPropertiesEntryType::String => RasterPropertiesEntry::String(d),
                };

                debug!(
                    "gdal properties key \"{:?}\" => target key \"{:?}\". Value: {:?} ",
                    &m.source_key, &m.target_key, &entry
                );

                properties.insert_property(m.target_key.clone(), entry);
            }
        }
    }

    pub fn properties_from_band(properties: &mut RasterProperties, gdal_dataset: &GdalRasterBand) {
        if let Some(scale) = gdal_dataset.scale() {
            properties.set_scale(scale);
        }
        if let Some(offset) = gdal_dataset.offset() {
            properties.set_offset(offset);
        }

        // ignore if there is no description
        if let Ok(description) = gdal_dataset.description() {
            properties.set_description(description);
        }
    }

    /// This method reads the data for a single tile with a specified size from the GDAL dataset and adds the requested metadata as properties to the tile.
    pub fn read_raster_properties(
        dataset: &GdalDataset,
        dataset_params: &GdalDatasetParameters,
        rasterband: &GdalRasterBand,
    ) -> RasterProperties {
        let mut properties = RasterProperties::default();

        // always read the scale and offset values from the rasterband
        Self::properties_from_band(&mut properties, rasterband);

        // read the properties from the dataset and rasterband metadata
        if let Some(properties_mapping) = dataset_params.properties_mapping.as_ref() {
            Self::properties_from_gdal_metadata(&mut properties, dataset, properties_mapping);
            Self::properties_from_gdal_metadata(&mut properties, rasterband, properties_mapping);
        }

        properties
    }

    ///
    /// A method to load single tiles from a GDAL dataset.
    ///
    fn load_tile_data<T: Pixel + GdalType + FromPrimitive>(
        dataset: &mut GdalDataset,
        dataset_params: &GdalDatasetParameters,
        read_advise: GdalReadAdvise,
    ) -> Result<GdalIpcPayload<T>, IpcProcessError> {
        let _span = tracing::debug_span!(
            "gdal_load_tile_data",
            data_type = ?T::TYPE,
            window = ?read_advise.bounds_of_target,
        )
        .entered();
        let start = Instant::now();

        debug!(
            "GridOrEmpty2D<{:?}> requested for {:?}.",
            T::TYPE,
            &read_advise.bounds_of_target,
        );

        let gdal_dataset_geotransform = GdalDatasetGeoTransform::from(dataset.geo_transform()?);
        // check that the dataset geo transform is the same as the one we get from GDAL
        debug_assert!(
            approx_eq!(
                Coordinate2D,
                gdal_dataset_geotransform.origin_coordinate,
                dataset_params.geo_transform.origin_coordinate
            ),
            "expected dataset geo transform origin coordinate {:?} to be approximately equal to GDAL dataset geo transform origin coordinate {:?}",
            dataset_params.geo_transform.origin_coordinate,
            gdal_dataset_geotransform.origin_coordinate
        );

        debug_assert!(
            approx_eq!(
                f64,
                gdal_dataset_geotransform.x_pixel_size,
                dataset_params.geo_transform.x_pixel_size
            ),
            "expected dataset geo transform x pixel size {:?} to be approximately equal to GDAL dataset geo transform x pixel size {:?}",
            dataset_params.geo_transform.x_pixel_size,
            gdal_dataset_geotransform.x_pixel_size
        );

        debug_assert!(
            approx_eq!(
                f64,
                gdal_dataset_geotransform.y_pixel_size,
                dataset_params.geo_transform.y_pixel_size
            ),
            "expected dataset geo transform y pixel size {:?} to be approximately equal to GDAL dataset geo transform y pixel size {:?}",
            dataset_params.geo_transform.y_pixel_size,
            gdal_dataset_geotransform.y_pixel_size
        );

        let (gdal_dataset_pixels_x, gdal_dataset_pixels_y) = dataset.raster_size();
        // check that the dataset pixel size is the same as the one we get from GDAL
        debug_assert_eq!(gdal_dataset_pixels_x, dataset_params.width);
        debug_assert_eq!(gdal_dataset_pixels_y, dataset_params.height);

        let rasterband = dataset.rasterband(dataset_params.rasterband_channel)?;

        let result_gdal_raster = Self::read_grid_from_raster(
            &rasterband,
            &read_advise.gdal_read_widow,
            &read_advise.read_window_bounds,
            dataset_params,
        )?;

        let properties = Self::read_raster_properties(dataset, dataset_params, &rasterband);

        let elapsed = start.elapsed();
        debug!("data loaded -> returning data grid, took {elapsed:?}");

        Ok(GdalIpcPayload {
            dimensions: read_advise.read_window_bounds,
            properties,
            data_variant: result_gdal_raster,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::super::{
        FileNotFoundHandling, GridAndProperties,
        process_common::{
            IpcChannelMessage, IpcChannelMessagePayload, IpcProcessGdalErrorKind,
            IpcProcessRasterResult,
        },
    };
    use super::*;
    use float_cmp::assert_approx_eq;
    use geoengine_datatypes::{
        primitives::{AxisAlignedRectangle, SpatialPartition2D, TimeInstance, TimeInterval},
        raster::{
            GeoTransform, GridBoundingBox2D, GridShape2D, RasterDataType, RasterPropertiesKey,
            SpatialGridDefinition, TileInformation,
        },
        test_data,
        util::{gdal::hide_gdal_errors, test::TestDefault},
    };
    use httptest::{Expectation, Server, matchers::request, responders};

    fn get_params() -> GdalDatasetParameters {
        GdalDatasetParameters {
            file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(0.),
            properties_mapping: Some(vec![
                GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: None,
                        key: "AREA_OR_POINT".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                    target_key: RasterPropertiesKey {
                        domain: None,
                        key: "AREA_OR_POINT".to_string(),
                    },
                },
                GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: Some("IMAGE_STRUCTURE".to_string()),
                        key: "COMPRESSION".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                    target_key: RasterPropertiesKey {
                        domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                        key: "COMPRESSION".to_string(),
                    },
                },
            ]),
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        }
    }

    fn tile_information_with_partition_and_shape(
        partition: SpatialPartition2D,
        shape: GridShape2D,
    ) -> TileInformation {
        let real_geotransform = GeoTransform::new(
            partition.upper_left(),
            partition.size_x() / shape.axis_size_x() as f64,
            -partition.size_y() / shape.axis_size_y() as f64,
        );

        TileInformation {
            tile_size_in_pixels: shape,
            global_tile_position: [0, 0].into(),
            global_geo_transform: real_geotransform,
        }
    }

    #[test]
    #[serial_test::serial]
    fn ipc_process_error_ipc_channel_roundtrip() {
        let error = IpcProcessError::GdalError {
            kind: IpcProcessGdalErrorKind::FileNotFound,
            details: "not found".into(),
        };

        let (sender, receiver) = ipc::channel::<IpcProcessRasterResult>().unwrap();
        sender.send(Err(error.clone())).unwrap();
        let decoded = receiver.recv().unwrap();

        assert_eq!(decoded, Err(error));
    }

    #[test]
    #[serial_test::serial]
    fn test_sending_gdal_dataset_parameters_via_string() {
        let msg = get_params();

        let (sender, receiver) = ipc_channel::ipc::channel::<String>().unwrap();

        sender.send(serde_json::to_string(&msg).unwrap()).unwrap();
        let recv = receiver.recv().unwrap();
        let recv = serde_json::from_str::<GdalDatasetParameters>(&recv).unwrap();
        assert_eq!(msg.properties_mapping, recv.properties_mapping);
        assert_eq!(msg, recv);
    }

    #[test]
    #[serial_test::serial]
    fn test_sending_gdal_dataset_parameters() {
        let msg = get_params();

        let (sender, receiver) = ipc_channel::ipc::channel().unwrap();

        sender.send(msg.clone()).unwrap();
        let recv = receiver.recv().unwrap();
        assert_eq!(msg.properties_mapping, recv.properties_mapping);

        assert_eq!(msg, recv);
    }

    #[test]
    #[serial_test::serial]
    fn test_sending_tile_information() {
        let output_shape: GridShape2D = [8, 8].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());

        let msg = tile_information_with_partition_and_shape(output_bounds, output_shape);

        let (sender, receiver) = ipc_channel::ipc::channel().unwrap();

        sender.send(msg).unwrap();
        let recv = receiver.recv().unwrap();
        assert_eq!(msg, recv);
    }

    #[test]
    #[serial_test::serial]
    fn test_sending_time() {
        let msg = TimeInstance::from_millis(10).unwrap();

        let (sender, receiver) = ipc_channel::ipc::channel().unwrap();

        sender.send(msg.clone()).unwrap();
        let recv = receiver.recv().unwrap();
        assert_eq!(msg, recv);
    }

    #[test]
    #[serial_test::serial]
    fn test_sending_time_interval() {
        let msg = TimeInterval::default();

        let (sender, receiver) = ipc_channel::ipc::channel().unwrap();

        sender.send(msg.clone()).unwrap();
        let recv = receiver.recv().unwrap();
        assert_eq!(msg, recv);
    }

    #[test]
    #[serial_test::serial]
    fn test_sending_request_tile_data() {
        let output_shape: GridShape2D = [8, 8].into();

        let read_advise = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), output_shape),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let payload = IpcChannelMessagePayload {
            data_type: RasterDataType::U8,
            dataset_params: GdalDatasetParameters {
                file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").into(),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: (-180., 90.).into(),
                    x_pixel_size: 0.1,
                    y_pixel_size: -0.1,
                },
                width: 3600,
                height: 1800,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: Some(0.),
                properties_mapping: Some(vec![
                    GdalMetadataMapping {
                        source_key: RasterPropertiesKey {
                            domain: None,
                            key: "AREA_OR_POINT".to_string(),
                        },
                        target_type: RasterPropertiesEntryType::String,
                        target_key: RasterPropertiesKey {
                            domain: None,
                            key: "AREA_OR_POINT".to_string(),
                        },
                    },
                    GdalMetadataMapping {
                        source_key: RasterPropertiesKey {
                            domain: Some("IMAGE_STRUCTURE".to_string()),
                            key: "COMPRESSION".to_string(),
                        },
                        target_type: RasterPropertiesEntryType::String,
                        target_key: RasterPropertiesKey {
                            domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                            key: "COMPRESSION".to_string(),
                        },
                    },
                ]),
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: true,
                retry: None,
            },
            read_advise,
            read_id: None,
        };

        let msg = IpcChannelMessage::new_request_tile_message(payload);

        let (sender, receiver) = ipc_channel::ipc::channel().unwrap();

        sender.send(msg.clone()).unwrap();
        let recv = receiver.recv().unwrap();

        assert_eq!(msg, recv);
    }

    #[test]
    #[serial_test::serial]
    fn test_ipc_channel_roundtrip_tile() {
        let output_shape: GridShape2D = [8, 8].into();

        let read_advise = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), output_shape),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let payload = IpcChannelMessagePayload {
            data_type: RasterDataType::U8,
            dataset_params: GdalDatasetParameters {
                file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").into(),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: (-180., 90.).into(),
                    x_pixel_size: 0.1,
                    y_pixel_size: -0.1,
                },
                width: 3600,
                height: 1800,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: Some(0.),
                properties_mapping: Some(vec![
                    GdalMetadataMapping {
                        source_key: RasterPropertiesKey {
                            domain: None,
                            key: "AREA_OR_POINT".to_string(),
                        },
                        target_type: RasterPropertiesEntryType::String,
                        target_key: RasterPropertiesKey {
                            domain: None,
                            key: "AREA_OR_POINT".to_string(),
                        },
                    },
                    GdalMetadataMapping {
                        source_key: RasterPropertiesKey {
                            domain: Some("IMAGE_STRUCTURE".to_string()),
                            key: "COMPRESSION".to_string(),
                        },
                        target_type: RasterPropertiesEntryType::String,
                        target_key: RasterPropertiesKey {
                            domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                            key: "COMPRESSION".to_string(),
                        },
                    },
                ]),
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: true,
                retry: None,
            },
            read_advise,
            read_id: None,
        };

        let msg = IpcChannelMessage::new_request_tile_message(payload);

        let (_child_guard, sender, receiver) = spawn_ipc_server_process::<
            IpcChannelMessage,
            IpcProcessRasterResult,
        >(&WorkerConfig::default())
        .unwrap();

        sender.send(msg).unwrap();
        let rx_result = receiver
            .recv()
            .inspect_err(|e| panic!("IPC receive: {:?}", e))
            .unwrap();

        let payload = match rx_result {
            Ok(r) => r,
            Err(e) => panic!("Error receiving from IPC process: {:?}", e),
        };

        let result_2: GdalIpcPayload<u8> = (&payload).try_into().unwrap();

        let grid_and_props: GridAndProperties<u8, GridBoundingBox2D> = result_2.into();

        assert!(!grid_and_props.grid.is_empty());

        let grid = grid_and_props.grid.into_materialized_masked_grid();

        assert_eq!(grid.inner_grid.data.len(), 64);
        assert_eq!(grid.validity_mask.data.len(), 64);
    }

    // This method loads raster data from a cropped MODIS NDVI raster.
    // To inspect the byte values first convert the file to XYZ with GDAL:
    // 'gdal_translate -of xyz MOD13A2_M_NDVI_2014-04-01_30X30.tif MOD13A2_M_NDVI_2014-04-01_30x30.xyz'
    // Then you can convert them to gruped bytes:
    // 'cut -d ' ' -f 1,2 --complement MOD13A2_M_NDVI_2014-04-01_30x30.xyz | xargs -n 30 > MOD13A2_M_NDVI_2014-04-01_30x30_bytes.txt'.
    fn load_ndvi_apr_2014_cropped(
        gdal_read_advice: GdalReadAdvise,
    ) -> Result<GridAndProperties<u8>, IpcProcessError> {
        let dataset_params = GdalDatasetParameters {
            file_path: test_data!("raster/modis_ndvi/cropped/MOD13A2_M_NDVI_2014-04-01_30x30.tif")
                .into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (8.0, 57.4).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 30,
            height: 30,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(255.),
            properties_mapping: Some(vec![GdalMetadataMapping {
                source_key: RasterPropertiesKey {
                    domain: None,
                    key: "AREA_OR_POINT".to_string(),
                },
                target_type: RasterPropertiesEntryType::String,
                target_key: RasterPropertiesKey {
                    domain: None,
                    key: "AREA_OR_POINT".to_string(),
                },
            }]),
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        };

        let mut gdc = GdalDatasetHolder::new();
        let dataset = gdc.get_or_open(&dataset_params).unwrap();

        let reader_payload =
            GdalHandling::load_tile_data::<u8>(dataset, &dataset_params, gdal_read_advice)
                .map_err(IpcProcessError::from)?;

        let grid_and_props: GridAndProperties<u8, GridBoundingBox2D> = reader_payload.into();
        Ok(grid_and_props.into())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_load_tile_data_top_left() {
        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), [8, 8].into()),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let GridAndProperties { grid, properties } =
            load_ndvi_apr_2014_cropped(gdal_read_advice).unwrap();

        assert!(!grid.is_empty());

        let grid = grid.into_materialized_masked_grid();

        assert_eq!(grid.inner_grid.data.len(), 64);
        // pixel value are the top left 8x8 block from MOD13A2_M_NDVI_2014-04-01_27x27_bytes.txt
        assert_eq!(
            grid.inner_grid.data,
            &[
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 127, 107, 255, 255, 255, 255, 255, 164, 185, 182,
                255, 255, 255, 175, 186, 190, 167, 140, 255, 255, 161, 175, 184, 173, 170, 188,
                255, 255, 128, 177, 165, 145, 191, 174, 255, 117, 100, 174, 159, 147, 99, 135
            ]
        );

        assert_eq!(grid.validity_mask.data.len(), 64);
        // pixel mask is pixel > 0 from the top left 8x8 block from MOD13A2_M_NDVI_2014-04-01_27x27_bytes.txt
        assert_eq!(
            grid.validity_mask.data,
            &[
                false, false, false, false, false, false, false, false, false, false, false, false,
                false, false, false, false, false, false, false, false, false, false, true, true,
                false, false, false, false, false, true, true, true, false, false, false, true,
                true, true, true, true, false, false, true, true, true, true, true, true, false,
                false, true, true, true, true, true, true, false, true, true, true, true, true,
                true, true
            ]
        );

        assert_eq!(
            properties.get_property(&RasterPropertiesKey {
                key: "AREA_OR_POINT".to_string(),
                domain: None,
            }),
            Some(&RasterPropertiesEntry::String("Area".to_string()))
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_load_tile_data_overlaps_dataset_bounds_top_left_out1() {
        // shift world bbox one pixel up and to the left
        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), [7, 7].into()), // this is the data we can read
            read_window_bounds: GridBoundingBox2D::new([1, 1], [7, 7]).unwrap(), // this is the area we can fill in target
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(), // this is the area of the target
            flip_y: false,
        };

        let GridAndProperties {
            grid,
            properties: _properties,
        } = load_ndvi_apr_2014_cropped(gdal_read_advice).unwrap();

        assert!(!grid.is_empty());

        let x = grid.into_materialized_masked_grid();

        assert_eq!(x.inner_grid.data.len(), 49);
        assert_eq!(
            x.inner_grid.data,
            &[
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 127, 255, 255, 255, 255, 255, 164, 185, 255, 255, 255, 175,
                186, 190, 167, 255, 255, 161, 175, 184, 173, 170, 255, 255, 128, 177, 165, 145,
                191,
            ]
        );

        assert_eq!(x.validity_mask.data.len(), 49);
        // pixel mask is pixel == 255 from the top left 8x8 block from MOD13A2_M_NDVI_2014-04-01_27x27_bytes.txt
        assert_eq!(
            x.validity_mask.data,
            &[
                false, false, false, false, false, false, false, false, false, false, false, false,
                false, false, false, false, false, false, false, false, true, false, false, false,
                false, false, true, true, false, false, false, true, true, true, true, false,
                false, true, true, true, true, true, false, false, true, true, true, true, true,
            ]
        );
    }

    #[test]
    #[serial_test::serial]
    fn it_creates_no_data_only_for_missing_files() {
        hide_gdal_errors();

        let ds = GdalDatasetParameters {
            file_path: "nonexisting_file.tif".into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        };

        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), [8, 8].into()),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let mut gdc = GdalDatasetHolder::new();

        let result =
            GdalHandling::load_tile_data_with_dataset_retry::<u8>(&mut gdc, &ds, gdal_read_advice);

        // file not found => specific error
        match result {
            Err(IpcProcessError::GdalError { kind, .. })
                if kind == IpcProcessGdalErrorKind::FileNotFound => {}
            _ => panic!("expected FileNotFound error"),
        }

        let ds = GdalDatasetParameters {
            file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").into(),
            rasterband_channel: 100, // invalid channel
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        };

        // invalid channel => error
        let result =
            GdalHandling::load_tile_data_with_dataset_retry::<u8>(&mut gdc, &ds, gdal_read_advice);
        match result {
            Err(IpcProcessError::GdalError { .. }) => {}
            _ => panic!("expected GdalError error"),
        }
    }

    #[test]
    #[serial_test::serial]
    fn it_creates_no_data_only_for_http_404() {
        let server = Server::run();

        server.expect(
            Expectation::matching(request::method_path("HEAD", "/non_existing.tif"))
                .times(1)
                .respond_with(responders::cycle![responders::status_code(404),]),
        );

        server.expect(
            Expectation::matching(request::method_path("HEAD", "/internal_error.tif"))
                .times(1)
                .respond_with(responders::cycle![responders::status_code(500),]),
        );

        let ds = GdalDatasetParameters {
            file_path: format!("/vsicurl/{}", server.url_str("/non_existing.tif")).into(),
            rasterband_channel: 1,
            geo_transform: TestDefault::test_default(),
            width: 100,
            height: 100,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: Some(vec![
                (
                    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS".to_owned(),
                    ".tif".to_owned(),
                ),
                (
                    "GDAL_DISABLE_READDIR_ON_OPEN".to_owned(),
                    "EMPTY_DIR".to_owned(),
                ),
                ("GDAL_HTTP_NETRC".to_owned(), "NO".to_owned()),
                ("GDAL_HTTP_MAX_RETRY".to_owned(), "0".to_string()),
            ]),
            allow_alphaband_as_mask: true,
            retry: None,
        };

        // 404 => no data
        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), [8, 8].into()),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let mut gdc = GdalDatasetHolder::new();

        let result =
            GdalHandling::load_tile_data_with_dataset_retry::<u8>(&mut gdc, &ds, gdal_read_advice);

        // file not found => specific error
        match result {
            Err(IpcProcessError::GdalError { kind, .. })
                if kind == IpcProcessGdalErrorKind::FileNotFound => {}
            _ => panic!("expected FileNotFound error"),
        }

        let ds = GdalDatasetParameters {
            file_path: format!("/vsicurl/{}", server.url_str("/internal_error.tif")).into(),
            rasterband_channel: 1,
            geo_transform: TestDefault::test_default(),
            width: 100,
            height: 100,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: Some(vec![
                (
                    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS".to_owned(),
                    ".tif".to_owned(),
                ),
                (
                    "GDAL_DISABLE_READDIR_ON_OPEN".to_owned(),
                    "EMPTY_DIR".to_owned(),
                ),
                ("GDAL_HTTP_NETRC".to_owned(), "NO".to_owned()),
                ("GDAL_HTTP_MAX_RETRY".to_owned(), "0".to_string()),
            ]),
            allow_alphaband_as_mask: true,
            retry: None,
        };

        // 500 => error
        let result =
            GdalHandling::load_tile_data_with_dataset_retry::<u8>(&mut gdc, &ds, gdal_read_advice);
        match result {
            Err(IpcProcessError::GdalError { .. }) => {}
            _ => panic!("expected GdalError error"),
        }
    }

    #[test]
    #[serial_test::serial]
    fn it_retries_only_after_clearing_vsi_cache() {
        hide_gdal_errors();

        let server = Server::run();

        server.expect(
            Expectation::matching(request::method_path("HEAD", "/foo.tif"))
                .times(2)
                .respond_with(responders::cycle![
                    // first generic error
                    responders::status_code(500),
                    // then 404 file not found
                    responders::status_code(404)
                ]),
        );

        let file_path: PathBuf = format!("/vsicurl/{}", server.url_str("/foo.tif")).into();

        let options = Some(vec![
            (
                "CPL_VSIL_CURL_ALLOWED_EXTENSIONS".to_owned(),
                ".tif".to_owned(),
            ),
            (
                "GDAL_DISABLE_READDIR_ON_OPEN".to_owned(),
                "EMPTY_DIR".to_owned(),
            ),
            ("GDAL_HTTP_NETRC".to_owned(), "NO".to_owned()),
            ("GDAL_HTTP_MAX_RETRY".to_owned(), "0".to_string()),
        ]);

        let _thread_local_configs = options
            .as_ref()
            .map(|config_options| GdalConfigOptions::new(config_options));

        // first fail
        let result = gdal_open_ex_gdal_error(
            file_path.as_path(),
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_RASTER,
                ..DatasetOptions::default()
            },
        );

        // it failed, but not with file not found
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(!GdalHandling::error_is_gdal_file_not_found(&error));
        }

        // second fail doesn't even try, so still not "file not found", even though it should be now
        let result = gdal_open_ex_gdal_error(
            file_path.as_path(),
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_RASTER,
                ..DatasetOptions::default()
            },
        );

        assert!(result.is_err());
        if let Err(error) = result {
            assert!(!GdalHandling::error_is_gdal_file_not_found(&error));
        }

        GdalHandling::clear_gdal_vsi_cache_for_path(file_path.as_path());

        // after clearing the cache, it tries again
        let result = gdal_open_ex_gdal_error(
            file_path.as_path(),
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_RASTER,
                ..DatasetOptions::default()
            },
        );

        // now we get the file not found error
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(GdalHandling::error_is_gdal_file_not_found(&error));
        }
    }

    #[test]
    #[serial_test::serial]
    #[allow(clippy::too_many_lines)]
    fn read_up_side_down_raster() {
        let up_side_down_params = GdalDatasetParameters {
            file_path: test_data!(
                "raster/modis_ndvi/flipped_axis_y/MOD13A2_M_NDVI_2014-01-01_flipped_y.tiff"
            )
            .into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., -90.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: 0.1,
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(0.),
            properties_mapping: Some(vec![
                GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: None,
                        key: "AREA_OR_POINT".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                    target_key: RasterPropertiesKey {
                        domain: None,
                        key: "AREA_OR_POINT".to_string(),
                    },
                },
                GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: Some("IMAGE_STRUCTURE".to_string()),
                        key: "COMPRESSION".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                    target_key: RasterPropertiesKey {
                        domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                        key: "COMPRESSION".to_string(),
                    },
                },
            ]),
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        };

        let ge_global_dataset_grid = SpatialGridDefinition::new(
            GeoTransform::new(Coordinate2D::new(-180., 90.), 0.1, -0.1),
            GridBoundingBox2D::new_min_max(0, 1799, 0, 3599).unwrap(),
        );

        let gdal_dataset_grid = ge_global_dataset_grid.flip_axis_y(); // first, flip axis
        assert_approx_eq!(
            Coordinate2D,
            gdal_dataset_grid.geo_transform().origin_coordinate,
            Coordinate2D::new(-180., 90.)
        );
        assert_approx_eq!(f64, gdal_dataset_grid.geo_transform.y_pixel_size(), 0.1);
        assert_approx_eq!(f64, gdal_dataset_grid.geo_transform.x_pixel_size(), 0.1);
        assert_eq!(
            gdal_dataset_grid.grid_bounds,
            GridBoundingBox2D::new_min_max(-1800, -1, 0, 3599).unwrap()
        );

        let gdal_dataset_grid = gdal_dataset_grid
            .with_moved_origin_exact_grid(Coordinate2D::new(-180., -90.))
            .unwrap(); // second, move origin (to other side of axis)
        assert_approx_eq!(
            Coordinate2D,
            gdal_dataset_grid.geo_transform().origin_coordinate,
            Coordinate2D::new(-180., -90.)
        );
        assert_approx_eq!(f64, gdal_dataset_grid.geo_transform.y_pixel_size(), 0.1);
        assert_approx_eq!(f64, gdal_dataset_grid.geo_transform.x_pixel_size(), 0.1);
        assert_eq!(
            gdal_dataset_grid.grid_bounds,
            GridBoundingBox2D::new_min_max(0, 1799, 0, 3599).unwrap()
        );

        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([1466, 1880].into(), [8, 8].into()),
            read_window_bounds: GridBoundingBox2D::new([326, 1880], [326 + 7, 1880 + 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([326, 1880], [326 + 7, 1880 + 7]).unwrap(),
            flip_y: true,
        };

        let mut gdc = GdalDatasetHolder::new();
        let dataset = gdc.get_or_open(&up_side_down_params).unwrap();

        let reader_payload =
            GdalHandling::load_tile_data::<u8>(dataset, &up_side_down_params, gdal_read_advice)
                .unwrap();

        let GridAndProperties { grid, properties } = reader_payload.into();
        assert!(!grid.is_empty());
        let grid = grid.into_materialized_masked_grid();

        assert_eq!(grid.inner_grid.data.len(), 64);

        assert_eq!(
            grid.inner_grid.data,
            &[
                // this is not yet flipped!
                255, 47, 42, 82, 81, 76, 73, 98, 255, 255, 59, 95, 85, 66, 105, 104, 255, 255, 91,
                97, 100, 86, 78, 106, 255, 255, 255, 97, 102, 91, 73, 72, 255, 255, 255, 255, 255,
                68, 81, 93, 255, 255, 255, 255, 255, 255, 53, 47, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            ]
        );

        assert_eq!(grid.validity_mask.data.len(), 64);
        assert_eq!(grid.validity_mask.data, &[true; 64]);

        assert!(properties.offset_option().is_none());
        assert!(properties.scale_option().is_none());
    }

    #[test]
    #[serial_test::serial]
    fn read_raster_and_offset_scale() {
        let up_side_down_params = GdalDatasetParameters {
            file_path: test_data!("raster/modis_ndvi/cropped/MOD13A2_M_NDVI_2014-04-01_30x30.tif")
                .into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (8.0, 57.4).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 30,
            height: 30,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(255.),
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        };

        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), [8, 8].into()),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let mut gdc = GdalDatasetHolder::new();
        let dataset = gdc.get_or_open(&up_side_down_params).unwrap();

        let GridAndProperties { grid, properties } =
            GdalHandling::load_tile_data::<u8>(dataset, &up_side_down_params, gdal_read_advice)
                .unwrap()
                .into();

        assert!(!grid.is_empty());

        let grid = grid.into_materialized_masked_grid();

        assert_eq!(grid.inner_grid.data.len(), 64);
        // pixel value are the top left 8x8 block from MOD13A2_M_NDVI_2014-04-01_27x27_bytes.txt
        assert_eq!(
            grid.inner_grid.data,
            &[
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 127, 107, 255, 255, 255, 255, 255, 164, 185, 182,
                255, 255, 255, 175, 186, 190, 167, 140, 255, 255, 161, 175, 184, 173, 170, 188,
                255, 255, 128, 177, 165, 145, 191, 174, 255, 117, 100, 174, 159, 147, 99, 135
            ]
        );

        assert_eq!(grid.validity_mask.data.len(), 64);
        // pixel mask is pixel > 0 from the top left 8x8 block from MOD13A2_M_NDVI_2014-04-01_27x27_bytes.txt
        assert_eq!(
            grid.validity_mask.data,
            &[
                false, false, false, false, false, false, false, false, false, false, false, false,
                false, false, false, false, false, false, false, false, false, false, true, true,
                false, false, false, false, false, true, true, true, false, false, false, true,
                true, true, true, true, false, false, true, true, true, true, true, true, false,
                false, true, true, true, true, true, true, false, true, true, true, true, true,
                true, true
            ]
        );

        assert_eq!(properties.offset_option(), Some(1.));
        assert_eq!(properties.scale_option(), Some(2.));

        assert!(approx_eq!(f64, properties.offset(), 1.));
        assert!(approx_eq!(f64, properties.scale(), 2.));
    }
}
