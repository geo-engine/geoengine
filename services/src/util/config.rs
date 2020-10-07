use std::sync::RwLock;

use crate::error;
use crate::error::Result;
use config::{Config, File};
use lazy_static::lazy_static;
use serde::Deserialize;
use snafu::ResultExt;
use std::path::Path;

lazy_static! {
    static ref SETTINGS: RwLock<Config> = RwLock::new({
        let mut settings = Config::default();
        // test may run in subdirectory
        #[cfg(test)]
        let paths = [
            "../Settings-default.toml",
            "Settings-default.toml",
            "../Settings-test.toml",
            "Settings-test.toml",
        ];

        #[cfg(not(test))]
        let paths = ["Settings-default.toml", "Settings.toml"];

        #[allow(clippy::filter_map)]
        let files: Vec<File<_>> = paths
            .iter()
            .map(Path::new)
            .filter(|p| p.exists())
            .map(File::from)
            .collect();

        settings.merge(files).unwrap();

        settings
    });
}

pub fn get_config<'a, T>(key: &str) -> Result<T>
where
    T: Deserialize<'a>,
{
    SETTINGS
        .read()
        .map_err(|_| error::Error::ConfigLockFailed)?
        .get::<T>(key)
        .context(error::Config)
}

pub fn get_config_element<'a, T>() -> Result<T>
where
    T: ConfigElement + Deserialize<'a>,
{
    SETTINGS
        .read()
        .map_err(|_| error::Error::ConfigLockFailed)?
        .get::<T>(T::KEY)
        .context(error::Config)
}

pub trait ConfigElement {
    const KEY: &'static str;
}

#[derive(Debug, Deserialize)]
pub struct Web {
    pub bind_address: String,
    pub external_address: Option<String>,
}

impl ConfigElement for Web {
    const KEY: &'static str = "web";
}

#[derive(Debug, Deserialize)]
pub enum Backend {
    InMemory,
    Postgres,
}

impl ConfigElement for Backend {
    const KEY: &'static str = "backend";
}

#[derive(Debug, Deserialize)]
pub struct Postgres {
    pub config: String,
}

impl ConfigElement for Postgres {
    const KEY: &'static str = "postgres";
}

#[derive(Debug, Deserialize)]
pub struct ProjectService {
    pub list_limit: usize,
}

impl ConfigElement for ProjectService {
    const KEY: &'static str = "project_service";
}

#[derive(Debug, Deserialize)]
pub struct GdalSource {
    pub raster_data_root_path: String, // TODO: Path?
}

impl ConfigElement for GdalSource {
    const KEY: &'static str = "operators.gdal_source";
}
