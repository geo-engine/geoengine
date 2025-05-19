use super::{Result, WildliveLayerId, error};
use chrono::{DateTime, Utc};
use snafu::ResultExt;
use std::{
    collections::{HashMap, hash_map::Entry},
    fmt::Write,
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::RwLock;

pub trait DatasetFilename {
    /// GeoJSON uses the file name as the layer name without the extension.
    fn layer_name(&self) -> Result<String>;

    /// The filename of the dataset.
    fn filename(&self) -> Result<String>;
}

/// A cache for the Wildlive datasets.
/// Caches datasets for the current day.
///
/// Note: Currently, we store the data in a temporary directory.
/// In the future, we might want to use a more clever solution, like a database or a cache.
/// The data is also not cleaned up after use, so we need to be careful about memory usage.
///
/// TODO: store in Postgres?
#[derive(Debug, Clone)]
pub struct WildliveCache {
    cache_dir: Arc<tempfile::TempDir>,
    cache: Arc<RwLock<HashMap<String, PathBuf>>>,
}

impl WildliveCache {
    pub fn new() -> Result<Self> {
        Ok(Self {
            cache_dir: dbg!(Arc::new(
                tempfile::tempdir().context(error::TempDirCreation)?
            )),
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn get(&self, filename: &String) -> Result<Option<PathBuf>> {
        let cache = self.cache.read().await;

        Ok(cache.get(filename).cloned())
    }

    pub async fn add(&self, filename: String, file_contents: String) -> Result<PathBuf> {
        let mut cache = self.cache.write().await;

        let entry = match cache.entry(filename.clone()) {
            Entry::Occupied(entry) => return Ok(entry.get().clone()),
            Entry::Vacant(entry) => entry,
        };

        let file_path = self.cache_dir.path().join(&filename);
        tokio::fs::write(&file_path, file_contents)
            .await
            .context(error::UnableToWriteDataset)?;
        entry.insert(file_path.clone());

        Ok(file_path)
    }
}

impl DatasetFilename for WildliveLayerId {
    fn layer_name(&self) -> Result<String> {
        let mut filename = String::new();

        let current_date = DateTime::<Utc>::from(Utc::now())
            .date_naive()
            .format("%Y-%m-%d");
        write!(&mut filename, "{current_date}_").context(error::UnableToCreateDatasetFilename)?;

        match self {
            WildliveLayerId::Projects => filename.push_str("projects"),
            WildliveLayerId::Stations { project_id } => {
                filename.push_str("stations_");
                insert_valid_file_chars(&mut filename, project_id);
            }
            WildliveLayerId::Captures { project_id } => {
                filename.push_str("captures_");
                insert_valid_file_chars(&mut filename, project_id);
            }
        };

        Ok(filename)
    }

    fn filename(&self) -> Result<String> {
        let mut filename = self.layer_name()?;

        filename.push_str(".geojson");

        Ok(filename)
    }
}

fn insert_valid_file_chars(filename: &mut String, input: &str) {
    for c in input.chars() {
        if c.is_alphanumeric() || c == '_' || c == '-' {
            filename.push(c)
        } else {
            filename.push('_');
        }
    }
}
