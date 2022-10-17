use crate::util::{gdal::gdal_open_dataset_ex, Result};
use gdal::{Dataset, DatasetOptions, GdalOpenFlags};
use lru::LruCache;
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock};

/// A limited pool of GDAL datasets.
/// The pool is limited to `max_pool_size` concurrently opened datasets.
/// If a new dataset is requested while the pool is full, the oldest dataset
/// not used for the longest time is closed.
/// If datasets are requested from multiple consumers, the datasets are shared between them.
pub struct GdalDatasetPool {
    pool: Mutex<LruCache<GdalDatasetPoolKey, Arc<RwLock<Dataset>>>>,
    // max_pool_size: usize,
}

unsafe impl Sync for GdalDatasetPool {}
// unsafe impl Send for GdalDatasetPool {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GdalDatasetPoolKey
where
    Self: Send + Sync,
{
    pub path: PathBuf,
    pub options: GdalDatasetOptions,
}

impl GdalDatasetPool {
    pub fn new(max_pool_size: NonZeroUsize) -> Self {
        Self {
            pool: Mutex::new(LruCache::new(max_pool_size)),
        }
    }

    pub async fn get<O>(&self, path: &Path, dataset_options: O) -> Result<Arc<RwLock<Dataset>>>
    where
        O: Into<GdalDatasetOptions>,
    {
        let dataset_options: GdalDatasetOptions = dataset_options.into();
        let key = GdalDatasetPoolKey {
            path: path.to_owned(),
            options: dataset_options,
        };

        let mut pool = self.pool.lock().await;

        if let Some(dataset) = pool.get(&key) {
            let dataset = dataset.clone();

            return Ok(dataset);
        }

        todo!("open dataset")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GdalDatasetOptions {
    pub open_flags: GdalOpenFlags,
    pub allowed_drivers: Option<Vec<String>>,
    pub open_options: Option<Vec<String>>,
    pub sibling_files: Option<Vec<String>>,
}

impl From<DatasetOptions<'_>> for GdalDatasetOptions {
    fn from(options: DatasetOptions) -> Self {
        Self {
            open_flags: options.open_flags,
            allowed_drivers: options
                .allowed_drivers
                .map(|v| v.iter().map(|s| (*s).to_owned()).collect()),
            open_options: options
                .open_options
                .map(|v| v.iter().map(|s| (*s).to_owned()).collect()),
            sibling_files: options
                .sibling_files
                .map(|v| v.iter().map(|s| (*s).to_owned()).collect()),
        }
    }
}

impl GdalDatasetOptions {
    pub fn open(&self, path: &Path) -> Result<Dataset> {
        let allowed_drivers: Option<Vec<&str>> = self
            .allowed_drivers
            .as_ref()
            .map(|v| v.iter().map(String::as_str).collect());
        let open_options: Option<Vec<&str>> = self
            .open_options
            .as_ref()
            .map(|v| v.iter().map(String::as_str).collect());
        let sibling_files: Option<Vec<&str>> = self
            .sibling_files
            .as_ref()
            .map(|v| v.iter().map(String::as_str).collect());

        let dataset_options = DatasetOptions {
            open_flags: self.open_flags,
            allowed_drivers: allowed_drivers.as_deref(),
            open_options: open_options.as_deref(),
            sibling_files: sibling_files.as_deref(),
        };

        gdal_open_dataset_ex(path, dataset_options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<GdalDatasetPool>();
    }
}
