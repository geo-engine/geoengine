use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::RwLock;

use crate::contexts::SessionId;
use crate::error::{self, Result};
use crate::util::parsing::{deserialize_base_url, deserialize_base_url_option};

use config::{Config, Environment, File};
use geoengine_datatypes::primitives::{DateTime, TimeInstance, TimeInterval};
use geoengine_operators::util::raster_stream_to_geotiff::GdalCompressionNumThreads;
use lazy_static::lazy_static;
use serde::Deserialize;
use snafu::ResultExt;
use url::Url;

lazy_static! {
    static ref SETTINGS: RwLock<Config> = RwLock::new({
        let mut settings = Config::builder();

        let dir: PathBuf = retrieve_settings_dir().expect("settings directory must exist");

        #[cfg(test)]
        let files = ["Settings-default.toml", "Settings-test.toml"];

        #[cfg(not(test))]
        let files = ["Settings-default.toml", "Settings.toml"];

        let files: Vec<File<_, _>> = files
            .iter()
            .map(|f| dir.join(f))
            .filter(|p| p.exists())
            .map(File::from)
            .collect();

        settings = settings.add_source(files);

        // Override config with environment variables that start with `GEOENGINE_`,
        // e.g. `GEOENGINE_WEB__EXTERNAL_ADDRESS=https://path.to.geoengine.io`
        // Note: Since variables contain underscores, we need to use something different
        // for seperating groups, for instance double underscores `__`
        settings = settings.add_source(Environment::with_prefix("geoengine").separator("__"));

        settings.build().unwrap()
    });
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
    pub backend: Backend,
    pub version_api: bool,
}

impl Web {
    pub fn external_address(&self) -> Result<Url> {
        Ok(self
            .external_address
            .clone()
            .unwrap_or(Url::parse(&format!("http://{}/", self.bind_address))?))
    }
}

impl ConfigElement for Web {
    const KEY: &'static str = "web";
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    InMemory,
    Postgres,
}

impl ConfigElement for Backend {
    const KEY: &'static str = "backend";
}

#[derive(Debug, Deserialize)]
pub struct Postgres {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub schema: String,
    pub user: String,
    pub password: String,
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
    pub origin_coordinate_x: f64,
    pub origin_coordinate_y: f64,
    pub tile_shape_pixels_x: usize,
    pub tile_shape_pixels_y: usize,
}

impl From<TilingSpecification> for geoengine_datatypes::raster::TilingSpecification {
    fn from(ts: TilingSpecification) -> geoengine_datatypes::raster::TilingSpecification {
        geoengine_datatypes::raster::TilingSpecification {
            origin_coordinate: geoengine_datatypes::primitives::Coordinate2D::new(
                ts.origin_coordinate_x,
                ts.origin_coordinate_y,
            ),
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
}

impl ConfigElement for QueryContext {
    const KEY: &'static str = "query_context";
}

#[derive(Debug, Deserialize)]
pub struct DatasetService {
    pub list_limit: u32,
}

impl ConfigElement for DatasetService {
    const KEY: &'static str = "dataset_service";
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
    pub fn time_interval(&self) -> TimeInterval {
        match self {
            OgcDefaultTime::Now => {
                TimeInterval::new_instant(TimeInstance::now()).expect("config error")
            }
            OgcDefaultTime::Value(value) => {
                TimeInterval::new(&value.start, &value.end).expect("config error")
            }
        }
    }
}

pub trait DefaultTime {
    fn default_time(&self) -> Option<TimeInterval>;
}

#[derive(Debug, Deserialize)]
pub struct TimeStartEnd {
    pub start: DateTime,
    pub end: DateTime,
}

#[derive(Debug, Deserialize)]
pub struct Ogc {
    pub default_time: Option<OgcDefaultTime>,
}

impl ConfigElement for Ogc {
    const KEY: &'static str = "ogc";
}

#[derive(Debug, Deserialize)]
pub struct Wcs {
    pub tile_limit: usize,
    pub default_time: Option<OgcDefaultTime>,
}

impl ConfigElement for Wcs {
    const KEY: &'static str = "wcs";
}

#[derive(Debug, Deserialize)]
pub struct Wfs {
    pub default_time: Option<OgcDefaultTime>,
}

impl ConfigElement for Wfs {
    const KEY: &'static str = "wfs";
}

#[derive(Debug, Deserialize)]
pub struct Wms {
    pub default_time: Option<OgcDefaultTime>,
}

impl ConfigElement for Wms {
    const KEY: &'static str = "wms";
}

#[derive(Debug, Deserialize)]
pub struct Odm {
    #[serde(deserialize_with = "deserialize_base_url")]
    pub endpoint: url::Url,
}

impl ConfigElement for Odm {
    const KEY: &'static str = "odm";
}

#[derive(Debug, Deserialize)]
pub struct DataProvider {
    pub dataset_defs_path: PathBuf,
    pub provider_defs_path: PathBuf,
    pub layer_defs_path: PathBuf,
    pub layer_collection_defs_path: PathBuf,
}

impl ConfigElement for DataProvider {
    const KEY: &'static str = "dataprovider";
}

#[derive(Debug, Deserialize)]
pub struct Gdal {
    pub compression_num_threads: GdalCompressionNumThreads,
    pub compression_z_level: Option<u8>,
    pub compression_algorithm: Option<Box<str>>,
}

impl ConfigElement for Gdal {
    const KEY: &'static str = "gdal";
}

#[derive(Debug, Deserialize)]
pub struct Session {
    pub anonymous_access: bool,
    pub fixed_session_token: Option<SessionId>,
    pub admin_session_token: Option<SessionId>,
}

impl ConfigElement for Session {
    const KEY: &'static str = "session";
}

#[cfg(feature = "nfdi")]
#[derive(Debug, Deserialize)]
pub struct GFBio {
    #[serde(deserialize_with = "deserialize_base_url")]
    pub basket_api_base_url: url::Url,
    pub group_abcd_units: bool,
}

#[cfg(feature = "nfdi")]
impl ConfigElement for GFBio {
    const KEY: &'static str = "gfbio";
}
