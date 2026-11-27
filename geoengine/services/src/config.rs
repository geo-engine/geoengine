use crate::datasets::upload::VolumeName;
use crate::error::{self, Result};
use crate::util::parsing::{deserialize_api_prefix, deserialize_base_url_option};
use config::{Config, ConfigError, Environment, File, Map, Source, Value, ValueKind};
use geoengine_datatypes::operations::image::RasterColorizer;
use geoengine_datatypes::primitives::{CacheTtlSeconds, MAX_CACHE_TTL_SECONDS, TimeInterval};
use geoengine_datatypes::util::test::TestDefault;
use geoengine_expression::ExpressionDependencies;
use geoengine_operators::cache::new_raster_cache::{
    BlockReadMode, DEFAULT_MIN_BLOCK_TTL_SECONDS, NewRasterCacheEnum,
};
use geoengine_operators::processing::ExpressionCacheConfig;
use geoengine_operators::util::WarpMethods;
use geoengine_operators::util::raster_stream_to_geotiff::GdalCompressionNumThreads;
use serde::Deserialize;
use snafu::ResultExt;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;
use url::Url;

static SETTINGS: LazyLock<RwLock<Config>> = LazyLock::new(init_settings);

fn init_settings() -> RwLock<Config> {
    let mut settings = Config::builder();

    let dir: PathBuf = retrieve_settings_dir().expect("settings directory should exist");

    #[cfg(test)]
    let files = ["Settings-default.toml", "Settings-test.toml"];

    #[cfg(not(test))]
    let files = ["Settings-default.toml", "Settings.toml"];

    let files: Vec<WorkerOptionNamesInLowerCase<File<_, _>>> = files
        .iter()
        .map(|f| dir.join(f))
        .filter(|p| p.exists())
        .map(|p| WorkerOptionNamesInLowerCase(File::from(p)))
        .collect();

    settings = settings.add_source(files);

    // Override config with environment variables that start with `GEOENGINE__`,
    // e.g. `GEOENGINE__LOGGING__LOG_SPEC=debug`
    // Note: Since variables contain underscores, we need to use something different
    // for separating groups, for instance double underscores `__`
    settings = settings.add_source(Environment::with_prefix("geoengine").separator("__"));

    let config = settings
        .build()
        .expect("it should crash the program if this fails");
    RwLock::new(config)
}

/// A settings file whose GDAL options and environment variables of the workers
/// (`gdal_process_pool.worker.gdal_config_options` and `.environment`) are named in lower case.
///
/// GDAL ignores the case of an option's name, the settings do not; the workers get the names in
/// upper case. Environment variables always
/// arrive in lower case, while `Settings-default.toml` spells the options in upper case, so an
/// override by `GEOENGINE__GDAL_PROCESS_POOL__WORKER__GDAL_CONFIG_OPTIONS__CPL_VSIL_CURL_CHUNK_SIZE`
/// became a second option next to the default. With the names of the files in lower case too,
/// each option is one key, and the order of the settings decides as for any other key:
/// `Settings-default.toml`, then `Settings.toml`, then the environment.
#[derive(Debug, Clone)]
struct WorkerOptionNamesInLowerCase<S>(S);

impl<S: Source + Clone + Send + Sync + 'static> Source for WorkerOptionNamesInLowerCase<S> {
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(self.clone())
    }

    fn collect(&self) -> std::result::Result<Map<String, Value>, ConfigError> {
        fn table<'a>(value: Option<&'a mut Value>, key: &str) -> Option<&'a mut Value> {
            match &mut value?.kind {
                ValueKind::Table(table) => table.get_mut(key),
                _ => None,
            }
        }

        let mut settings = self.0.collect()?;
        for options in ["gdal_config_options", "environment"] {
            let options = table(
                table(settings.get_mut("gdal_process_pool"), "worker"),
                options,
            );
            if let Some(ValueKind::Table(options)) = options.map(|options| &mut options.kind) {
                *options = std::mem::take(options)
                    .into_iter()
                    .map(|(name, value)| (name.to_ascii_lowercase(), value))
                    .collect();
            }
        }
        Ok(settings)
    }
}

/// test may run in subdirectory
#[cfg(test)]
fn retrieve_settings_dir() -> Result<PathBuf> {
    use crate::error::Error;

    const MAX_PARENT_DIRS: usize = 1;

    let mut settings_dir = std::env::current_dir().context(error::MissingWorkingDirectory)?;

    for _ in 0..=MAX_PARENT_DIRS {
        if settings_dir.join("Settings-default.toml").exists() {
            return Ok(settings_dir);
        }

        // go to parent directory
        if !settings_dir.pop() {
            break;
        }
    }

    Err(Error::MissingSettingsDirectory)
}

#[cfg(not(test))]
fn retrieve_settings_dir() -> Result<PathBuf> {
    std::env::current_dir().context(error::MissingWorkingDirectory)
}

#[cfg(test)]
pub fn set_config<T>(key: &str, value: T) -> Result<()>
where
    T: Into<config::Value>,
{
    let mut settings = SETTINGS
        .write()
        .map_err(|_error| error::Error::ConfigLockFailed)?;

    let builder = Config::builder()
        .add_source(settings.clone())
        .set_override(key, value)
        .context(error::Config)?;

    *settings = builder.build().context(error::Config)?;
    Ok(())
}

pub fn get_config<'a, T>(key: &str) -> Result<T>
where
    T: Deserialize<'a>,
{
    SETTINGS
        .read()
        .map_err(|_error| error::Error::ConfigLockFailed)?
        .get::<T>(key)
        .context(error::Config)
}

pub fn get_config_element<'a, T>() -> Result<T>
where
    T: ConfigElement + Deserialize<'a>,
{
    get_config(T::KEY)
}

pub trait ConfigElement {
    const KEY: &'static str;
}

#[derive(Debug, Deserialize)]
pub struct Web {
    pub bind_address: SocketAddr,
    #[serde(deserialize_with = "deserialize_base_url_option", default)]
    pub external_address: Option<url::Url>,
    /// The api prefix is the path relative to the `bind_adress` where the API is served.
    /// During parsing it is ensured that a slash is at the start and no slash is at the end.
    #[serde(deserialize_with = "deserialize_api_prefix")]
    pub api_prefix: String,
    pub version_api: bool,
    /// HTTP worker threads, `0` for actix' default (one per core)
    pub workers: usize,
    /// Max. blocking threads of each HTTP worker, `0` for `max(512 / workers, 32)`
    pub max_blocking_threads_per_worker: usize,
}

impl Web {
    /// The base address of the API of the server ending in a slash.
    /// Use this e.g. for generating links to API handlers by joining it with a relative path.
    pub fn api_url(&self) -> Result<Url> {
        Ok(self.external_address.clone().unwrap_or(Url::parse(&format!(
            "http://{}{}/",
            self.bind_address, self.api_prefix
        ))?))
    }
}

impl ConfigElement for Web {
    const KEY: &'static str = "web";
}

#[derive(Debug, Deserialize)]
pub struct Postgres {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub schema: String,
    pub user: String,
    pub password: String,
    pub clear_database_on_start: bool,
}

impl ConfigElement for Postgres {
    const KEY: &'static str = "postgres";
}

#[derive(Debug, Deserialize)]
pub struct ProjectService {
    pub list_limit: u32,
}

impl ConfigElement for ProjectService {
    const KEY: &'static str = "project_service";
}

#[derive(Debug, Deserialize)]
pub struct TilingSpecification {
    pub tile_shape_pixels_x: usize,
    pub tile_shape_pixels_y: usize,
}

impl From<TilingSpecification> for geoengine_datatypes::raster::TilingSpecification {
    fn from(ts: TilingSpecification) -> geoengine_datatypes::raster::TilingSpecification {
        geoengine_datatypes::raster::TilingSpecification {
            tile_size_in_pixels: geoengine_datatypes::raster::GridShape2D::from([
                ts.tile_shape_pixels_y,
                ts.tile_shape_pixels_x,
            ]),
        }
    }
}

impl ConfigElement for TilingSpecification {
    const KEY: &'static str = "raster.tiling_specification";
}

#[derive(Debug, Deserialize)]
pub struct QueryContext {
    pub chunk_byte_size: usize,
    /// Threads of the Rayon pool for operators, `0` for one per core
    pub rayon_threads: usize,
    /// GDAL tile reads a `GdalSource` query keeps in flight
    pub gdal_source_read_ahead: NonZeroUsize,
    /// GDAL tile reads a `MultiBandGdalSource` query keeps in flight
    pub multi_band_gdal_source_read_ahead: NonZeroUsize,
    /// Sub-queries of one query that may run at once, see `QueryParallelism`
    pub available_parallelism_per_query: usize,
    /// Upper limit in MB for the memory that a `Warp` with the downsampling method `mode` keeps per
    /// output tile
    pub warp_mode_max_memory_per_tile_mb: NonZeroUsize,
    /// The largest error in input pixels of the approximate coordinate transformation of a raster
    /// reprojection, for a `Warp` without `approximationErrorThreshold`; `0` projects exactly
    pub reprojection_approximation_error_px: geoengine_datatypes::util::NonNegativeFiniteF64,
}

impl QueryContext {
    /// The upper limit in bytes for the memory that a `Warp` with the downsampling method `mode`
    /// keeps per output tile
    pub fn warp_mode_max_memory_per_tile(&self) -> usize {
        self.warp_mode_max_memory_per_tile_mb
            .get()
            .saturating_mul(1024 * 1024)
    }

    pub fn gdal_read_ahead(&self) -> geoengine_operators::engine::GdalReadAhead {
        geoengine_operators::engine::GdalReadAhead {
            gdal_source: self.gdal_source_read_ahead.get(),
            multi_band_gdal_source: self.multi_band_gdal_source_read_ahead.get(),
        }
    }
}

impl ConfigElement for QueryContext {
    const KEY: &'static str = "query_context";
}

/// The compiled libraries of expression operators
#[derive(Debug, Deserialize)]
pub struct Expression {
    /// Seconds a library stays in the cache without being requested
    pub library_idle_ttl_seconds: u64,
    /// Seconds between cleanup runs
    pub library_sweep_interval_seconds: NonZeroU64,
    /// Cache size for libraries in MB
    pub library_cache_size_in_mb: u64,
    /// Directory for the libraries
    pub workspace_path: PathBuf,
    /// Directory with the dependencies built with this binary, empty or missing for the build directory
    #[serde(default)]
    pub dependencies_path: Option<PathBuf>,
    /// Max. number of compilers running at the same time
    pub max_concurrent_compilations: NonZeroUsize,
}

impl ConfigElement for Expression {
    const KEY: &'static str = "expression";
}

impl From<Expression> for ExpressionCacheConfig {
    fn from(config: Expression) -> Self {
        Self {
            idle_ttl: Duration::from_secs(config.library_idle_ttl_seconds),
            sweep_interval: Duration::from_secs(config.library_sweep_interval_seconds.get()),
            max_cache_size_in_bytes: config.library_cache_size_in_mb.saturating_mul(1024 * 1024),
            max_concurrent_compilations: config.max_concurrent_compilations,
            workspace_path: config.workspace_path,
            dependencies_path: config
                .dependencies_path
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or_else(|| {
                    ExpressionDependencies::default_dependencies_dir().to_path_buf()
                }),
        }
    }
}

/// The Tokio runtime of the server, which runs the queries
#[derive(Debug, Deserialize)]
pub struct ProcessingRuntime {
    /// Worker threads, `0` for one per core
    pub worker_threads: usize,
    pub max_blocking_threads: NonZeroUsize,
}

impl ConfigElement for ProcessingRuntime {
    const KEY: &'static str = "processing_runtime";
}

#[derive(Debug, Deserialize)]
pub struct DatasetService {
    pub list_limit: u32,
    pub json_size_limit: usize,
}

impl ConfigElement for DatasetService {
    const KEY: &'static str = "dataset_service";
}

#[derive(Debug, Deserialize)]
pub struct LayerService {
    pub list_limit: u32,
}

impl ConfigElement for LayerService {
    const KEY: &'static str = "layer_service";
}

#[derive(Debug, Deserialize)]
pub struct TaskManager {
    pub list_limit: u32,
    pub list_default_limit: u32,
}

impl ConfigElement for TaskManager {
    const KEY: &'static str = "task_manager";
}

#[derive(Debug, Deserialize)]
pub struct Upload {
    pub path: PathBuf,
}

impl ConfigElement for Upload {
    const KEY: &'static str = "upload";
}

#[derive(Debug, Deserialize)]
pub struct Logging {
    pub log_spec: String,
    pub log_to_file: bool,
    pub filename_prefix: String,
    pub log_directory: Option<String>,
    pub raw_error_messages: bool,
}

impl ConfigElement for Logging {
    const KEY: &'static str = "logging";
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum OgcDefaultTime {
    #[serde(alias = "now")]
    Now,
    #[serde(alias = "value")]
    Value(TimeStartEnd),
}

impl OgcDefaultTime {
    ///  # Panics
    ///
    /// Panics if configuration is invalid
    /// # TODO: return result instead
    ///
    pub fn time_interval(&self) -> TimeInterval {
        match self {
            OgcDefaultTime::Now => geoengine_datatypes::primitives::TimeInterval::new_instant(
                geoengine_datatypes::primitives::TimeInstance::now(),
            )
            .expect("config error"),
            OgcDefaultTime::Value(value) => {
                geoengine_datatypes::primitives::TimeInterval::new(&value.start, &value.end)
                    .expect("config error")
            }
        }
    }
}

pub trait DefaultTime {
    fn default_time(&self) -> Option<TimeInterval>;
}

/// The default time of the config section `S`, else of [`Ogc`], else now.
///
/// # Panics
/// Panics if a configured default time is invalid.
pub fn default_time_from_config<S: ConfigElement + DefaultTime + for<'a> Deserialize<'a>>()
-> TimeInterval {
    get_config_element::<S>()
        .ok()
        .and_then(|section| section.default_time())
        .or_else(|| {
            get_config_element::<Ogc>()
                .ok()
                .and_then(|ogc| ogc.default_time.map(|time| time.time_interval()))
        })
        .unwrap_or_else(|| {
            TimeInterval::new_instant(geoengine_datatypes::primitives::TimeInstance::now())
                .expect("is a valid time interval")
        })
}

#[derive(Debug, Deserialize)]
pub struct TimeStartEnd {
    pub start: geoengine_datatypes::primitives::DateTime,
    pub end: geoengine_datatypes::primitives::DateTime,
}

#[derive(Debug, Deserialize)]
pub struct Ogc {
    pub default_time: Option<OgcDefaultTime>,
    pub tiles: Option<OgcApiTilesConfig>,
    pub cors: Option<CorsConfig>,
    /// How WMS `GetMap` and OGC API tiles resample rasters onto the requested grid
    #[serde(default)]
    pub map_resampling: MapResampling,
}

/// How rendered maps (WMS `GetMap`, OGC API tiles) resample rasters onto the requested grid.
/// WCS always uses nearest neighbor, as it returns data values.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum MapResampling {
    /// The methods for the colorizer: the most common class for a palette, the average and
    /// bilinear interpolation for gradients and RGB composites
    #[default]
    Symbology,
    /// Nearest neighbor, which is faster
    Nearest,
}

impl MapResampling {
    /// The methods for rendering with `colorizer`
    pub fn methods(self, colorizer: &RasterColorizer) -> WarpMethods {
        match self {
            Self::Symbology => WarpMethods::for_symbology(colorizer),
            Self::Nearest => WarpMethods::NEAREST,
        }
    }
}

impl ConfigElement for Ogc {
    const KEY: &'static str = "ogc";
}

#[derive(Debug, Deserialize)]
pub struct OgcApiTilesConfig {
    pub request_timeout_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[allow(
    clippy::struct_field_names,
    reason = "field names match the CORS nomenclature"
)]
pub struct CorsConfig {
    pub allow_origin: Option<String>,
    pub allow_headers: Vec<String>,
    pub allow_methods: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct Wcs {
    pub tile_limit: usize,
    pub default_time: Option<OgcDefaultTime>,
    pub request_timeout_seconds: Option<u64>,
}

impl ConfigElement for Wcs {
    const KEY: &'static str = "wcs";
}

impl DefaultTime for Wcs {
    fn default_time(&self) -> Option<TimeInterval> {
        self.default_time
            .as_ref()
            .map(OgcDefaultTime::time_interval)
    }
}

#[derive(Debug, Deserialize)]
pub struct Wfs {
    pub default_time: Option<OgcDefaultTime>,
    pub request_timeout_seconds: Option<u64>,
}

impl ConfigElement for Wfs {
    const KEY: &'static str = "wfs";
}

impl DefaultTime for Wfs {
    fn default_time(&self) -> Option<TimeInterval> {
        self.default_time
            .as_ref()
            .map(OgcDefaultTime::time_interval)
    }
}

#[derive(Debug, Deserialize)]
pub struct Wms {
    pub default_time: Option<OgcDefaultTime>,
    pub request_timeout_seconds: Option<u64>,
    pub max_image_width: u32,
    pub max_image_height: u32,
}

impl ConfigElement for Wms {
    const KEY: &'static str = "wms";
}

impl DefaultTime for Wms {
    fn default_time(&self) -> Option<TimeInterval> {
        self.default_time
            .as_ref()
            .map(OgcDefaultTime::time_interval)
    }
}

#[derive(Debug, Deserialize)]
pub struct Plots {
    pub request_timeout_seconds: Option<u64>,
}

impl ConfigElement for Plots {
    const KEY: &'static str = "plots";
}

#[derive(Debug, Deserialize)]
#[allow(clippy::struct_field_names)] // TODO: find better group name and remove postfix
pub struct DataProvider {
    pub dataset_defs_path: PathBuf,
    pub provider_defs_path: PathBuf,
    pub layer_defs_path: PathBuf,
    pub layer_collection_defs_path: PathBuf,
    /// Optional key material used to encrypt STAC passwords at rest.
    pub password_encryption_key: Option<String>,
}

impl ConfigElement for DataProvider {
    const KEY: &'static str = "dataprovider";
}

#[derive(Debug, Deserialize)]
pub struct Gdal {
    pub compression_num_threads: GdalCompressionNumThreads,
    pub compression_z_level: Option<u8>,
    pub compression_algorithm: Option<Box<str>>,
    pub allowed_drivers: HashSet<String>,
}

impl ConfigElement for Gdal {
    const KEY: &'static str = "gdal";
}

///  Controlls the global `GdalProcessPool`.
/// - `number_of_processes`: The total number of GDAL worker processes to spawn and maintain in the pool.
/// - `max_active_processes`: The maximum number of active (in-flight) requests allowed across all datasets at any given time.
/// - `max_dataset_processes`: The maximum number of active requests allowed concurrently for the same dataset, enforcing per-dataset concurrency limits.
/// - `header_cache_entries`: The number of file headers kept for reads through cached blocks.
/// - `worker`: Configuration passed to each GDAL worker process at spawn time.
#[derive(Debug, Deserialize)]
#[allow(clippy::struct_field_names)]
pub struct GdalProcessPool {
    pub number_of_processes: u64,
    pub max_active_processes: u64,
    pub max_dataset_processes: u64,
    /// 0 is rejected when the settings are read
    pub header_cache_entries: NonZeroUsize,
    #[serde(default)]
    pub worker: GdalProcessPoolWorkerConfig,
}

impl GdalProcessPool {
    /// Rejects sizes of 0, with which GDAL reads would never be dispatched. Limits above the
    /// number of processes are allowed: they just have no effect.
    pub fn validate(&self) -> Result<()> {
        if self.number_of_processes == 0
            || self.max_active_processes == 0
            || self.max_dataset_processes == 0
        {
            return Err(crate::error::Error::InvalidConfig {
                reason: format!(
                    "gdal_process_pool sizes must be at least 1, got number_of_processes = {}, max_active_processes = {}, max_dataset_processes = {}",
                    self.number_of_processes, self.max_active_processes, self.max_dataset_processes
                ),
            });
        }
        Ok(())
    }
}

/// Configuration for GDAL worker processes, passed from the main process at spawn time.
#[derive(Debug, Default, Deserialize)]
pub struct GdalProcessPoolWorkerConfig {
    #[serde(default)]
    pub gdal_config_options: Option<std::collections::HashMap<String, String>>,
    /// Environment variables of each worker process, set when it starts. Names are applied in
    /// upper case.
    #[serde(default)]
    pub environment: Option<std::collections::HashMap<String, String>>,
    #[serde(default)]
    pub logging: WorkerLoggingConfig,
    #[serde(default)]
    pub open_telemetry: OpenTelemetryConfig,
    #[serde(default)]
    pub retry: geoengine_operators::source::gdal_worker_process::WorkerRetryConfig,
}

#[derive(Debug, Deserialize)]
pub struct WorkerLoggingConfig {
    pub log_spec: String,
    #[serde(default)]
    pub log_to_file: bool,
    #[serde(default = "default_worker_log_prefix")]
    pub filename_prefix: String,
    pub log_directory: Option<String>,
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
        }
    }
}

#[derive(Debug, Deserialize)]
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

impl ConfigElement for GdalProcessPool {
    const KEY: &'static str = "gdal_process_pool";
}

impl From<GdalProcessPoolWorkerConfig>
    for geoengine_operators::source::gdal_worker_process::WorkerConfig
{
    fn from(
        config: GdalProcessPoolWorkerConfig,
    ) -> geoengine_operators::source::gdal_worker_process::WorkerConfig {
        geoengine_operators::source::gdal_worker_process::WorkerConfig {
            // GDAL ignores the case; the settings name the options in lower case
            gdal_config_options: config.gdal_config_options.map(|options| {
                options
                    .into_iter()
                    .map(|(name, value)| (name.to_ascii_uppercase(), value))
                    .collect()
            }),
            // the settings name the variables in lower case, the environment is case-sensitive
            environment: config
                .environment
                .unwrap_or_default()
                .into_iter()
                .map(|(name, value)| (name.to_ascii_uppercase(), value))
                .collect(),
            logging: geoengine_operators::source::gdal_worker_process::WorkerLoggingConfig {
                log_spec: config.logging.log_spec,
                log_to_file: config.logging.log_to_file,
                filename_prefix: config.logging.filename_prefix,
                log_directory: config.logging.log_directory,
                worker_id: 0,
            },
            open_telemetry: geoengine_operators::source::gdal_worker_process::OpenTelemetryConfig {
                enabled: config.open_telemetry.enabled,
                endpoint: config.open_telemetry.endpoint,
            },
            retry: config.retry,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Session {
    pub anonymous_access: bool,
}

impl ConfigElement for Session {
    const KEY: &'static str = "session";
}

#[derive(Debug, Deserialize)]
pub struct GFBio {
    #[serde(deserialize_with = "crate::util::parsing::deserialize_base_url")]
    pub basket_api_base_url: url::Url,
    pub group_abcd_units: bool,
}

impl ConfigElement for GFBio {
    const KEY: &'static str = "gfbio";
}

#[derive(Debug, Deserialize)]
pub struct Data {
    pub volumes: HashMap<VolumeName, PathBuf>,
}

impl ConfigElement for Data {
    const KEY: &'static str = "data";
}

#[derive(Debug, Deserialize)]
pub struct MachineLearning {
    pub storage_volume: VolumeName,
    pub model_path: PathBuf,
    pub list_limit: u32,
}

impl ConfigElement for MachineLearning {
    const KEY: &'static str = "machine_learning";
}

#[derive(Debug, Deserialize)]
pub struct User {
    pub registration: bool,
    pub admin_email: String,
    pub admin_password: String,
}

impl ConfigElement for User {
    const KEY: &'static str = "user";
}

#[derive(Debug, Deserialize)]
pub struct Quota {
    pub mode: QuotaTrackingMode,
    #[serde(default)]
    pub initial_credits: i64,
    pub increment_quota_buffer_size: usize,
    pub increment_quota_buffer_timeout_seconds: u64,
}

impl ConfigElement for Quota {
    const KEY: &'static str = "quota";
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum QuotaTrackingMode {
    Track,
    Check,
    Disabled,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Oidc {
    pub enabled: bool,
    pub issuer: Url,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub scopes: Vec<String>,
    pub token_encryption_password: Option<String>,
}

impl ConfigElement for Oidc {
    const KEY: &'static str = "oidc";
}

#[derive(Debug, Deserialize)]
pub struct OpenTelemetry {
    pub enabled: bool,
    pub endpoint: String,
}

impl ConfigElement for OpenTelemetry {
    const KEY: &'static str = "open_telemetry";
}

#[derive(Debug, Deserialize, Clone)]
pub struct Cache {
    pub enabled: bool,
    pub size_in_mb: usize,
    pub landing_zone_ratio: f64,
    /// use the new raster cache implementation for raster data instead of the old cache
    #[allow(clippy::struct_field_names)]
    pub enable_new_raster_cache: bool,
    /// if `enable_new_raster_cache` is set, the fraction of `size_in_mb` reserved for the new
    /// raster cache; the remainder is used by the old cache (which then only holds vector data).
    /// Ignored if `enable_new_raster_cache` is `false`, in which case the old cache holds both
    /// vector and raster data and gets the full `size_in_mb`.
    pub raster_cache_size_ratio: f64,
    /// names of the raster operators whose tiles the new raster cache stores, `"*"` for all
    pub raster_operators: Vec<String>,
    /// Default cache lifetime for values without an explicit cache TTL.
    pub default_ttl_seconds: u32,
    /// With the new raster cache, which files GDAL sources read through cached file blocks
    pub block_reads: BlockReadMode,
    /// With block reads, the least time file blocks and headers stay cached
    pub min_block_ttl_seconds: u32,
}

impl Cache {
    /// Whether the new raster cache stores the tiles of the operator `name`.
    pub fn caches_raster_operator(&self, name: &str) -> bool {
        self.raster_operators
            .iter()
            .any(|operator| operator == "*" || operator == name)
    }

    pub fn shared_cache_size_in_mb(&self) -> usize {
        self.size_in_mb - self.new_raster_cache_size_in_mb()
    }

    pub fn new_raster_cache_size_in_bytes(&self) -> usize {
        self.new_raster_cache_size_in_mb() * 1024 * 1024
    }

    /// The new raster cache, if this config enables it.
    pub fn new_raster_cache(&self) -> Option<Arc<NewRasterCacheEnum>> {
        (self.enabled && self.enable_new_raster_cache).then(|| {
            Arc::new(
                NewRasterCacheEnum::new_lru(self.new_raster_cache_size_in_bytes())
                    .with_min_block_ttl(CacheTtlSeconds::new(self.min_block_ttl_seconds))
                    .with_block_read_mode(self.block_reads),
            )
        })
    }

    fn new_raster_cache_size_in_mb(&self) -> usize {
        if self.enable_new_raster_cache {
            (self.size_in_mb as f64 * self.raster_cache_size_ratio) as usize
        } else {
            0
        }
    }

    /// Whether the file blocks of data without a TTL stay cached longer than its tiles.
    fn blocks_outlive_default_ttl(&self) -> bool {
        self.enabled
            && self.enable_new_raster_cache
            && self.block_reads != BlockReadMode::Off
            && self.default_ttl_seconds < self.min_block_ttl_seconds
    }

    pub fn validate(&self) -> Result<()> {
        if self.default_ttl_seconds > MAX_CACHE_TTL_SECONDS {
            return Err(crate::error::Error::InvalidConfig {
                reason: format!(
                    "default_ttl_seconds must be at most {MAX_CACHE_TTL_SECONDS}, got {}",
                    self.default_ttl_seconds
                ),
            });
        }
        if self.min_block_ttl_seconds > MAX_CACHE_TTL_SECONDS {
            return Err(crate::error::Error::InvalidConfig {
                reason: format!(
                    "min_block_ttl_seconds must be at most {MAX_CACHE_TTL_SECONDS}, got {}",
                    self.min_block_ttl_seconds
                ),
            });
        }
        if self.blocks_outlive_default_ttl() {
            tracing::warn!(
                "[cache] default_ttl_seconds ({} s) is below min_block_ttl_seconds ({} s): the file blocks of data without its own TTL stay cached longer than its tiles, so changes to its files can take {} s to show",
                self.default_ttl_seconds,
                self.min_block_ttl_seconds,
                self.min_block_ttl_seconds,
            );
        }
        if !(0.0..=1.0).contains(&self.raster_cache_size_ratio) {
            return Err(crate::error::Error::InvalidConfig {
                reason: format!(
                    "raster_cache_size_ratio must be between 0 and 1, got {}",
                    self.raster_cache_size_ratio
                ),
            });
        }
        Ok(())
    }
}

impl TestDefault for Cache {
    fn test_default() -> Self {
        Self {
            enabled: false,
            size_in_mb: 1_000,       // 1 GB
            landing_zone_ratio: 0.1, // 10% of cache size
            enable_new_raster_cache: false,
            raster_cache_size_ratio: 0.5,
            raster_operators: vec!["*".to_string()],
            default_ttl_seconds: 0,
            block_reads: BlockReadMode::Auto,
            min_block_ttl_seconds: DEFAULT_MIN_BLOCK_TTL_SECONDS,
        }
    }
}

impl ConfigElement for Cache {
    const KEY: &'static str = "cache";
}

#[derive(Debug, Deserialize, Clone, Copy)]
pub struct StacCache {
    /// Maximum size of the STAC query cache in MB across all datasets.
    pub size_in_mb: usize,
    /// TTL in seconds for STAC query cache entries.
    pub ttl_secs: u64,
}

impl TestDefault for StacCache {
    fn test_default() -> Self {
        Self {
            size_in_mb: 1_000, // 1 GB
            ttl_secs: 3_600,
        }
    }
}

impl ConfigElement for StacCache {
    const KEY: &'static str = "stac_cache";
}

#[derive(Debug, Deserialize, Clone, Copy)]
pub struct ProviderCache {
    /// Maximum number of cached provider instances in the provider registry.
    pub max_entries: usize,
    /// Maximum idle time in seconds before a cached provider is evicted.
    pub max_idle_secs: u64,
}

impl ConfigElement for ProviderCache {
    const KEY: &'static str = "provider_cache";
}

#[derive(Clone, Debug, Deserialize)]
pub struct Wildlive {
    pub api_endpoint: Url,
    pub oidc: WildliveOidc,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WildliveOidc {
    pub issuer: Url,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub broker_provider: String,
}

impl ConfigElement for Wildlive {
    const KEY: &'static str = "wildlive";
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Settings-default.toml` alone, without the overrides of `Settings-test.toml`
    fn default_settings() -> Config {
        Config::builder()
            .add_source(WorkerOptionNamesInLowerCase(File::from(
                retrieve_settings_dir()
                    .unwrap()
                    .join("Settings-default.toml"),
            )))
            .build()
            .unwrap()
    }

    #[test]
    fn it_converts_the_default_expression_settings() {
        let config: Expression = default_settings().get(Expression::KEY).unwrap();

        let config = ExpressionCacheConfig::from(config);

        assert_eq!(config.max_cache_size_in_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.workspace_path, PathBuf::from("expression_workspace"));
        assert_eq!(
            config.dependencies_path,
            ExpressionDependencies::default_dependencies_dir()
        );
    }

    #[test]
    fn it_resamples_maps_by_symbology_unless_configured_otherwise() {
        use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};

        let config: Ogc = default_settings().get(Ogc::KEY).unwrap();
        assert_eq!(config.map_resampling, MapResampling::Symbology);

        let config = Config::builder()
            .add_source(default_settings())
            .set_override("ogc.map_resampling", "nearest")
            .unwrap()
            .build()
            .unwrap();
        let config: Ogc = config.get(Ogc::KEY).unwrap();
        assert_eq!(config.map_resampling, MapResampling::Nearest);

        let palette = RasterColorizer::SingleBand {
            band: 0,
            band_colorizer: Colorizer::palette(
                [(1.0.try_into().unwrap(), RgbaColor::white())].into(),
                RgbaColor::transparent(),
                RgbaColor::black(),
            )
            .unwrap(),
        };
        assert_eq!(
            MapResampling::Symbology.methods(&palette),
            WarpMethods::CLASSES
        );
        assert_eq!(
            MapResampling::Nearest.methods(&palette),
            WarpMethods::NEAREST
        );
    }

    #[test]
    fn it_rejects_an_expression_sweep_interval_of_zero() {
        let config = Config::builder()
            .add_source(default_settings())
            .set_override("expression.library_sweep_interval_seconds", 0)
            .unwrap()
            .build()
            .unwrap();

        assert!(config.get::<Expression>(Expression::KEY).is_err());
    }

    fn pool(
        number_of_processes: u64,
        max_active_processes: u64,
        max_dataset_processes: u64,
    ) -> GdalProcessPool {
        GdalProcessPool {
            number_of_processes,
            max_active_processes,
            max_dataset_processes,
            header_cache_entries:
                geoengine_operators::source::gdal_worker_process::DEFAULT_HEADER_CACHE_ENTRIES,
            worker: GdalProcessPoolWorkerConfig::default(),
        }
    }

    #[test]
    fn it_accepts_valid_pool_sizes() {
        assert!(pool(32, 16, 8).validate().is_ok());
        assert!(pool(2, 2, 2).validate().is_ok());
    }

    #[test]
    fn it_rejects_zero_pool_sizes() {
        assert!(pool(0, 0, 0).validate().is_err());
        assert!(pool(32, 0, 0).validate().is_err());
        assert!(pool(32, 16, 0).validate().is_err());
    }

    #[test]
    fn it_accepts_limits_above_the_number_of_processes() {
        assert!(pool(8, 16, 8).validate().is_ok());
        assert!(pool(32, 8, 16).validate().is_ok());
    }

    /// The worker config from `Settings-default.toml`, a `Settings.toml` with `local`, and the
    /// environment variable `GEOENGINE__GDAL_PROCESS_POOL__WORKER__{variable}` set to `env`.
    fn worker_config(
        local: Option<&str>,
        variable: &str,
        env: Option<&str>,
    ) -> geoengine_operators::source::gdal_worker_process::WorkerConfig {
        let mut config = Config::builder().add_source(default_settings());
        if let Some(local) = local {
            config = config.add_source(WorkerOptionNamesInLowerCase(File::from_str(
                local,
                config::FileFormat::Toml,
            )));
        }
        let variable = format!("GEOENGINE__GDAL_PROCESS_POOL__WORKER__{variable}");
        let config = config
            .add_source(
                Environment::with_prefix("geoengine")
                    .separator("__")
                    .source(Some(
                        env.map(|env| (variable, env.to_string()))
                            .into_iter()
                            .collect(),
                    )),
            )
            .build()
            .unwrap();
        let pool: GdalProcessPool = config.get(GdalProcessPool::KEY).unwrap();
        pool.worker.into()
    }

    /// The chunk sizes the workers get from `Settings-default.toml`, a `Settings.toml` with
    /// `local` and the environment variable set to `env`.
    fn worker_chunk_sizes(local: Option<&str>, env: Option<&str>) -> Vec<(String, String)> {
        worker_config(local, "GDAL_CONFIG_OPTIONS__CPL_VSIL_CURL_CHUNK_SIZE", env)
            .gdal_config_options
            .unwrap()
            .into_iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case("CPL_VSIL_CURL_CHUNK_SIZE"))
            .collect()
    }

    #[test]
    fn it_reads_the_worker_retry_limits() {
        use geoengine_operators::source::gdal_worker_process::WorkerRetryConfig;

        let retry = |local: Option<&str>, env: Option<&str>| {
            worker_config(local, "RETRY__MAX_DURATION_SECONDS", env).retry
        };

        assert_eq!(retry(None, None), WorkerRetryConfig::default());
        assert_eq!(
            retry(None, None),
            WorkerRetryConfig {
                max_backoff_seconds: 8,
                max_duration_seconds: 180,
            }
        );
        // a local file may set one limit and keeps the other; the environment wins
        let local = "[gdal_process_pool.worker.retry]\nmax_backoff_seconds = 2\n";
        assert_eq!(
            retry(Some(local), None),
            WorkerRetryConfig {
                max_backoff_seconds: 2,
                max_duration_seconds: 180,
            }
        );
        assert_eq!(
            retry(Some(local), Some("20")),
            WorkerRetryConfig {
                max_backoff_seconds: 2,
                max_duration_seconds: 20,
            }
        );
    }

    #[test]
    fn it_lets_the_order_of_the_settings_decide_a_worker_environment_variable() {
        let gaps = |local: Option<&str>, env: Option<&str>| {
            worker_config(local, "ENVIRONMENT__OPJ_PLT_MIN_GAP", env)
                .environment
                .into_iter()
                .filter(|(name, _)| name.eq_ignore_ascii_case("OPJ_PLT_MIN_GAP"))
                .collect::<Vec<_>>()
        };
        let gap = |size: &str| vec![("OPJ_PLT_MIN_GAP".to_string(), size.to_string())];
        let local =
            |name: &str| format!("[gdal_process_pool.worker.environment]\n{name} = \"131072\"\n");

        // the default sets no such variable, but one BLAS thread; a local table keeps those
        assert_eq!(gaps(None, None), vec![]);
        for local in [None, Some(local("OPJ_PLT_MIN_GAP"))] {
            let mut environment =
                worker_config(local.as_deref(), "ENVIRONMENT__OPJ_PLT_MIN_GAP", None).environment;
            environment.retain(|(name, _)| name.ends_with("_NUM_THREADS"));
            environment.sort();
            assert_eq!(
                environment,
                [("OMP_NUM_THREADS", "1"), ("OPENBLAS_NUM_THREADS", "1")]
                    .map(|(name, value)| (name.to_string(), value.to_string()))
            );
        }
        assert_eq!(gaps(None, Some("65536")), gap("65536"));
        // a local file sets it in any spelling, as one upper-case name; the environment wins
        for name in ["OPJ_PLT_MIN_GAP", "opj_plt_min_gap", "Opj_Plt_Min_Gap"] {
            assert_eq!(gaps(Some(&local(name)), None), gap("131072"), "{name}");
            assert_eq!(
                gaps(Some(&local(name)), Some("65536")),
                gap("65536"),
                "{name}"
            );
        }
    }

    #[test]
    fn it_lets_the_order_of_the_settings_decide_a_worker_gdal_option() {
        let chunk_size =
            |size: &str| vec![("CPL_VSIL_CURL_CHUNK_SIZE".to_string(), size.to_string())];
        let local = |name: &str| {
            format!("[gdal_process_pool.worker.gdal_config_options]\n{name} = \"262144\"\n")
        };

        assert_eq!(worker_chunk_sizes(None, None), chunk_size("1048576"));
        // the environment overrides the default file, which spells the name in upper case
        assert_eq!(worker_chunk_sizes(None, Some("65536")), chunk_size("65536"));
        // a local file overrides the default in any spelling, the environment overrides both
        for name in [
            "CPL_VSIL_CURL_CHUNK_SIZE",
            "cpl_vsil_curl_chunk_size",
            "Cpl_Vsil_Curl_Chunk_Size",
        ] {
            assert_eq!(
                worker_chunk_sizes(Some(&local(name)), None),
                chunk_size("262144"),
                "{name}"
            );
            assert_eq!(
                worker_chunk_sizes(Some(&local(name)), Some("65536")),
                chunk_size("65536"),
                "{name}"
            );
        }
    }

    fn default_element<T: ConfigElement + for<'a> Deserialize<'a>>() -> T {
        default_settings().get(T::KEY).unwrap()
    }

    fn rejects<T: ConfigElement + for<'a> Deserialize<'a>>(key: &str, value: i64) -> bool {
        Config::builder()
            .add_source(default_settings())
            .set_override(format!("{}.{key}", T::KEY), value)
            .unwrap()
            .build()
            .unwrap()
            .get::<T>(T::KEY)
            .is_err()
    }

    #[test]
    fn it_processing_runtime_defaults_are_the_tokio_defaults() {
        let config: ProcessingRuntime = default_element();
        // not set, so Tokio uses one per core
        assert_eq!(config.worker_threads, 0);
        assert_eq!(config.max_blocking_threads.get(), 512);

        assert!(rejects::<ProcessingRuntime>("max_blocking_threads", 0));
        assert!(!rejects::<ProcessingRuntime>("worker_threads", 0));
    }

    #[test]
    fn it_gdal_read_ahead_defaults_are_the_previous_values() {
        let config: QueryContext = default_element();
        assert_eq!(
            config.gdal_read_ahead(),
            geoengine_operators::engine::GdalReadAhead {
                gdal_source: 8,
                multi_band_gdal_source: 16,
            }
        );
        assert_eq!(
            config.gdal_read_ahead(),
            geoengine_operators::engine::GdalReadAhead::default()
        );

        assert!(rejects::<QueryContext>("gdal_source_read_ahead", 0));
        assert!(rejects::<QueryContext>(
            "multi_band_gdal_source_read_ahead",
            0
        ));
    }

    #[test]
    fn it_defaults_the_warp_mode_memory_to_the_operators_default() {
        let config: QueryContext = default_element();
        assert_eq!(
            config.warp_mode_max_memory_per_tile(),
            geoengine_operators::engine::DEFAULT_WARP_MODE_MAX_MEMORY_PER_TILE
        );
        assert!(rejects::<QueryContext>(
            "warp_mode_max_memory_per_tile_mb",
            0
        ));
    }

    #[test]
    fn it_defaults_the_reprojection_approximation_error_to_gdals_default() {
        let config: QueryContext = default_element();
        assert_eq!(
            config.reprojection_approximation_error_px.get(),
            0.125,
            "GDAL's default for `gdalwarp -et`"
        );
        assert!(rejects::<QueryContext>(
            "reprojection_approximation_error_px",
            -1
        ));
    }

    #[test]
    fn it_web_pool_defaults_are_the_previous_values() {
        let config: Web = default_element();
        assert_eq!(config.workers, 0);
        assert_eq!(config.max_blocking_threads_per_worker, 0);
    }

    #[test]
    fn it_selects_the_raster_operators_to_cache_by_name() {
        let cache = |operators: &[&str]| Cache {
            raster_operators: operators.iter().map(ToString::to_string).collect(),
            ..Cache::test_default()
        };
        assert!(default_element::<Cache>().caches_raster_operator("Expression"));
        assert!(cache(&["Expression"]).caches_raster_operator("Expression"));
        assert!(!cache(&["Expression"]).caches_raster_operator("Reprojection"));
        assert!(!cache(&[]).caches_raster_operator("Expression"));
    }

    #[test]
    fn it_warns_when_blocks_outlive_the_default_ttl_only_with_block_reads() {
        let cache =
            |enabled, enable_new_raster_cache, default_ttl_seconds, min_block_ttl_seconds| Cache {
                enabled,
                enable_new_raster_cache,
                block_reads: BlockReadMode::Auto,
                default_ttl_seconds,
                min_block_ttl_seconds,
                ..Cache::test_default()
            };
        assert!(cache(true, true, 0, 60).blocks_outlive_default_ttl());
        assert!(cache(true, true, 0, 60).validate().is_ok());
        assert!(!cache(true, true, 60, 60).blocks_outlive_default_ttl());
        assert!(!cache(true, true, 300, 60).blocks_outlive_default_ttl());
        assert!(!cache(true, true, 0, 0).blocks_outlive_default_ttl());
        assert!(!cache(true, false, 0, 60).blocks_outlive_default_ttl());
        assert!(!cache(false, true, 0, 60).blocks_outlive_default_ttl());
        assert!(
            !Cache {
                block_reads: BlockReadMode::Off,
                ..cache(true, true, 0, 60)
            }
            .blocks_outlive_default_ttl()
        );
        assert!(
            cache(true, true, 0, MAX_CACHE_TTL_SECONDS + 1)
                .validate()
                .is_err()
        );
        assert!(
            Cache {
                default_ttl_seconds: MAX_CACHE_TTL_SECONDS + 1,
                ..Cache::test_default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn it_gives_the_new_raster_cache_the_block_settings() {
        let cache = Cache {
            enabled: true,
            enable_new_raster_cache: true,
            min_block_ttl_seconds: 7,
            ..Cache::test_default()
        };
        let raster_cache = cache.new_raster_cache().unwrap();
        assert_eq!(raster_cache.min_block_ttl().seconds(), 7);
        assert_eq!(raster_cache.block_read_mode(), BlockReadMode::Auto);
        assert_eq!(
            Cache {
                block_reads: BlockReadMode::On,
                ..cache
            }
            .new_raster_cache()
            .unwrap()
            .block_read_mode(),
            BlockReadMode::On
        );
    }
}
