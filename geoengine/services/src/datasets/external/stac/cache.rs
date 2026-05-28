use super::StacProviderDataset;
use geoengine_datatypes::primitives::{SpatialPartition2D, TimeInterval};
use geoengine_operators::source::TileFile;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::debug;

/// Maximum total number of `TileFile` entries held in memory across all datasets.
const DEFAULT_MAX_TILE_FILES: usize = 10_000;

/// Default TTL for cached entries (1 hour).
const DEFAULT_TTL_SECS: u64 = 60 * 60;

/// In-memory cache for STAC query results, keyed by dataset name and (spatial
/// bounds, time interval).  The cache is held inside the long-lived
/// `StacDataProvider` and survives across individual requests.
///
/// # Containment semantics
///
/// * **Cache hit** – a stored entry whose spatial bounds *contain* the query
///   bbox **and** whose time interval *contains* the query time interval.  The
///   relevant subset of tile-files and time-steps is extracted and returned.
///
/// * **Superset insertion** – when storing a new result, any already-cached
///   entry that is fully contained by the new result is evicted first, so only
///   the larger result is kept.
#[derive(Debug)]
pub struct StacQueryCache {
    inner: Mutex<StacQueryCacheInner>,
    max_tile_files: usize,
    ttl: Duration,
}

struct StacQueryCacheInner {
    /// Per-dataset list of cache entries.
    by_dataset: Vec<DatasetCacheEntries>,
    /// Total number of `TileFile` instances currently held in the cache.
    total_tile_files: usize,
}

struct DatasetCacheEntries {
    dataset: StacProviderDataset,
    entries: Vec<CacheEntry>,
}

struct CacheEntry {
    spatial_bounds: SpatialPartition2D,
    time_interval: TimeInterval,
    time_steps: Vec<TimeInterval>,
    tile_files: Vec<TileFile>,
    inserted_at: Instant,
    last_used: Instant,
}

impl std::fmt::Debug for StacQueryCacheInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StacQueryCacheInner")
            .field("datasets", &self.by_dataset.len())
            .field("total_tile_files", &self.total_tile_files)
            .finish()
    }
}

impl Default for StacQueryCache {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_TILE_FILES, Duration::from_secs(DEFAULT_TTL_SECS))
    }
}

impl StacQueryCache {
    pub fn new(max_tile_files: usize, ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(StacQueryCacheInner {
                by_dataset: Vec::new(),
                total_tile_files: 0,
            }),
            max_tile_files,
            ttl,
        }
    }

    /// Look up a cache entry that fully contains `(spatial_bounds, time_interval)`.
    ///
    /// Returns `Some((time_steps, tile_files))` with only those elements that
    /// fall within the queried bounds, or `None` on a cache miss.
    pub async fn lookup(
        &self,
        dataset: &StacProviderDataset,
        spatial_bounds: &SpatialPartition2D,
        time_interval: TimeInterval,
    ) -> Option<(Vec<TimeInterval>, Vec<TileFile>)> {
        let mut inner = self.inner.lock().await;
        Self::evict_expired(&mut inner, self.ttl);

        debug!(
            dataset = %dataset.name,
            spatial_bounds = ?spatial_bounds,
            time_interval = ?time_interval,
            "STAC cache lookup"
        );

        let entries = inner
            .by_dataset
            .iter_mut()
            .find(|dataset_entries| dataset_entries.dataset == *dataset)?
            .entries
            .as_mut_slice();

        for entry in entries {
            let spatial_match = entry.spatial_bounds.contains(spatial_bounds);
            let time_match = entry.time_interval.contains(&time_interval);

            if spatial_match && time_match {
                entry.last_used = Instant::now();

                debug!(
                    dataset = %dataset.name,
                    entry_spatial_bounds = ?entry.spatial_bounds,
                    entry_time_interval = ?entry.time_interval,
                    time_steps = entry.time_steps.len(),
                    tile_files = entry.tile_files.len(),
                    "STAC cache hit"
                );

                let time_steps: Vec<TimeInterval> = entry
                    .time_steps
                    .iter()
                    .filter(|ts| time_interval.intersects(ts))
                    .copied()
                    .collect();

                let tile_files: Vec<TileFile> = entry
                    .tile_files
                    .iter()
                    .filter(|tf| {
                        time_interval.intersects(&tf.time)
                            && tf.spatial_partition.intersects(spatial_bounds)
                    })
                    .cloned()
                    .collect();

                return Some((time_steps, tile_files));
            }

            debug!(
                dataset = %dataset.name,
                entry_spatial_bounds = ?entry.spatial_bounds,
                entry_time_interval = ?entry.time_interval,
                spatial_match,
                time_match,
                entry_time_steps = entry.time_steps.len(),
                entry_tile_files = entry.tile_files.len(),
                "STAC cache candidate did not match"
            );
        }

        debug!(dataset = %dataset.name, "STAC cache miss");
        None
    }

    /// Store a new query result in the cache.
    ///
    /// Before inserting:
    /// 1. Expired entries are removed.
    /// 2. Any cached entry for the same dataset that is *fully contained* by
    ///    the new result is evicted (the new, larger result supersedes it).
    /// 3. LRU entries are evicted until the total tile-file count stays within
    ///    `max_tile_files`.
    pub async fn insert(
        &self,
        dataset: &StacProviderDataset,
        spatial_bounds: SpatialPartition2D,
        time_interval: TimeInterval,
        time_steps: Vec<TimeInterval>,
        tile_files: Vec<TileFile>,
    ) {
        let new_count = tile_files.len();
        let mut inner = self.inner.lock().await;

        Self::evict_expired(&mut inner, self.ttl);

        debug!(
            dataset = %dataset.name,
            spatial_bounds = ?spatial_bounds,
            time_interval = ?time_interval,
            time_steps = time_steps.len(),
            tile_files = new_count,
            "STAC cache insert"
        );

        let dataset_entries = Self::dataset_entries_mut(&mut inner, dataset);

        // Evict all cached entries that are fully contained by the new result.
        let mut freed = 0usize;
        dataset_entries.retain(|entry| {
            let evict = spatial_bounds.contains(&entry.spatial_bounds)
                && time_interval.contains(&entry.time_interval);

            if evict {
                debug!(
                    dataset = %dataset.name,
                    entry_spatial_bounds = ?entry.spatial_bounds,
                    entry_time_interval = ?entry.time_interval,
                    entry_time_steps = entry.time_steps.len(),
                    entry_tile_files = entry.tile_files.len(),
                    "STAC cache evict superseded entry"
                );
                freed += entry.tile_files.len();
                false
            } else {
                true
            }
        });
        inner.total_tile_files -= freed;

        // Enforce the memory cap by evicting LRU entries across all datasets.
        while new_count > 0 && inner.total_tile_files + new_count > self.max_tile_files {
            if !Self::evict_lru_one(&mut inner) {
                break;
            }
        }

        let now = Instant::now();
        let inserted_entry = CacheEntry {
                spatial_bounds,
                time_interval,
                time_steps,
                tile_files,
                inserted_at: now,
                last_used: now,
            };
        debug!(
            dataset = %dataset.name,
            inserted_spatial_bounds = ?inserted_entry.spatial_bounds,
            inserted_time_interval = ?inserted_entry.time_interval,
            inserted_time_steps = inserted_entry.time_steps.len(),
            inserted_tile_files = inserted_entry.tile_files.len(),
            total_tile_files = inner.total_tile_files + inserted_entry.tile_files.len(),
            "STAC cache stored entry"
        );
        Self::dataset_entries_mut(&mut inner, dataset).push(inserted_entry);
        inner.total_tile_files += new_count;
    }

    fn dataset_entries_mut<'a>(
        inner: &'a mut StacQueryCacheInner,
        dataset: &StacProviderDataset,
    ) -> &'a mut Vec<CacheEntry> {
        if let Some(idx) = inner
            .by_dataset
            .iter()
            .position(|dataset_entries| dataset_entries.dataset == *dataset)
        {
            return &mut inner.by_dataset[idx].entries;
        }

        inner.by_dataset.push(DatasetCacheEntries {
            dataset: dataset.clone(),
            entries: Vec::new(),
        });

        inner
            .by_dataset
            .last_mut()
            .expect("dataset entries were just inserted")
            .entries
            .as_mut()
    }

    /// Remove all entries older than `ttl`, updating the tile-file count.
    fn evict_expired(inner: &mut StacQueryCacheInner, ttl: Duration) {
        let mut freed = 0usize;
        for dataset_entries in &mut inner.by_dataset {
            dataset_entries.entries.retain(|e| {
                if e.inserted_at.elapsed() > ttl {
                    debug!(
                        dataset = %dataset_entries.dataset.name,
                        expired_spatial_bounds = ?e.spatial_bounds,
                        expired_time_interval = ?e.time_interval,
                        expired_tile_files = e.tile_files.len(),
                        "STAC cache evict expired entry"
                    );
                    freed += e.tile_files.len();
                    false
                } else {
                    true
                }
            });
        }
        inner.total_tile_files -= freed;
    }

    /// Remove the single least-recently-used entry across all datasets.
    ///
    /// Returns `true` if an entry was removed.
    fn evict_lru_one(inner: &mut StacQueryCacheInner) -> bool {
        let oldest = inner
            .by_dataset
            .iter()
            .enumerate()
            .flat_map(|(dataset_idx, dataset_entries)| {
                dataset_entries
                    .entries
                    .iter()
                    .enumerate()
                    .map(move |(entry_idx, entry)| (dataset_idx, entry_idx, entry.last_used))
            })
            .min_by_key(|(_, _, last_used)| *last_used);

        if let Some((dataset_idx, entry_idx, _)) = oldest {
            if let Some(dataset_entries) = inner.by_dataset.get_mut(dataset_idx) {
                let removed = dataset_entries.entries.swap_remove(entry_idx);
                debug!(
                    dataset = %dataset_entries.dataset.name,
                    evicted_spatial_bounds = ?removed.spatial_bounds,
                    evicted_time_interval = ?removed.time_interval,
                    evicted_tile_files = removed.tile_files.len(),
                    "STAC cache evict LRU entry"
                );
                inner.total_tile_files -= removed.tile_files.len();
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::primitives::SpatialResolution;
    use geoengine_datatypes::raster::{GeoTransform, GridBoundingBox2D, GridIdx2D, RasterDataType};
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceAuthority};
    use geoengine_operators::engine::SpatialGridDescriptor;
    use geoengine_operators::source::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalRetryOptions,
    };
    use std::path::PathBuf;

    fn dataset(name: &str) -> StacProviderDataset {
        StacProviderDataset {
            name: name.to_owned(),
            description: String::new(),
            data_type: RasterDataType::U16,
            resolution: SpatialResolution::new_unchecked(10.0, 10.0),
            projection: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new((399_960.0, 5_700_000.0).into(), 10.0, -10.0),
                GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([10979, 10979]))
                    .expect("grid bounds"),
            ),
            bands: vec![],
        }
    }

    fn bounds(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> SpatialPartition2D {
        SpatialPartition2D::new((min_x, max_y).into(), (max_x, min_y).into())
            .expect("valid spatial bounds")
    }

    fn time(start: i64, end: i64) -> TimeInterval {
        TimeInterval::new_unchecked(start, end)
    }

    fn tile_file(time: TimeInterval, spatial_partition: SpatialPartition2D) -> TileFile {
        TileFile {
            time,
            spatial_partition,
            band: 0,
            z_index: 0,
            params: GdalDatasetParameters {
                file_path: PathBuf::from("/tmp/mock.tif"),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: (0.0, 0.0).into(),
                    x_pixel_size: 1.0,
                    y_pixel_size: -1.0,
                },
                width: 10,
                height: 10,
                file_not_found_handling: FileNotFoundHandling::Error,
                no_data_value: None,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: false,
                retry: Some(GdalRetryOptions { max_retries: 1 }),
            },
        }
    }

    #[tokio::test]
    async fn lookup_uses_dataset_key_and_containment() {
        let cache = StacQueryCache::new(100, Duration::from_secs(60));
        let a = dataset("A");
        let b = dataset("B");

        cache
            .insert(
                &a,
                bounds(0.0, 0.0, 10.0, 10.0),
                time(0, 10),
                vec![time(0, 5), time(5, 10)],
                vec![],
            )
            .await;

        let hit = cache
            .lookup(&a, &bounds(2.0, 2.0, 8.0, 8.0), time(2, 8))
            .await;
        assert!(hit.is_some());

        let miss = cache
            .lookup(&b, &bounds(2.0, 2.0, 8.0, 8.0), time(2, 8))
            .await;
        assert!(miss.is_none());
    }

    #[tokio::test]
    async fn larger_insert_evicts_contained_entry() {
        let cache = StacQueryCache::new(100, Duration::from_secs(60));
        let a = dataset("A");

        cache
            .insert(
                &a,
                bounds(2.0, 2.0, 4.0, 4.0),
                time(2, 4),
                vec![time(2, 4)],
                vec![],
            )
            .await;

        cache
            .insert(
                &a,
                bounds(0.0, 0.0, 10.0, 10.0),
                time(0, 10),
                vec![time(0, 10)],
                vec![],
            )
            .await;

        // The old small entry must be gone, but the larger one must satisfy the old query.
        let old_query = cache
            .lookup(&a, &bounds(2.0, 2.0, 4.0, 4.0), time(2, 4))
            .await;
        assert!(old_query.is_some());
    }

    #[tokio::test]
    async fn enforces_lru_tile_file_limit() {
        let cache = StacQueryCache::new(1, Duration::from_secs(60));
        let a = dataset("A");

        cache
            .insert(
                &a,
                bounds(0.0, 0.0, 10.0, 10.0),
                time(0, 10),
                vec![time(0, 10)],
                vec![tile_file(time(0, 10), bounds(0.0, 0.0, 10.0, 10.0))],
            )
            .await;

        cache
            .insert(
                &a,
                bounds(10.0, 10.0, 20.0, 20.0),
                time(10, 20),
                vec![time(10, 20)],
                vec![tile_file(time(10, 20), bounds(10.0, 10.0, 20.0, 20.0))],
            )
            .await;

        let first_is_gone = cache
            .lookup(&a, &bounds(0.0, 0.0, 10.0, 10.0), time(0, 10))
            .await;
        assert!(first_is_gone.is_none());

        let second_is_present = cache
            .lookup(&a, &bounds(10.0, 10.0, 20.0, 20.0), time(10, 20))
            .await;
        assert!(second_is_present.is_some());
    }

    #[tokio::test]
    async fn expires_entries_after_ttl() {
        let cache = StacQueryCache::new(10, Duration::from_millis(1));
        let a = dataset("A");

        cache
            .insert(
                &a,
                bounds(0.0, 0.0, 10.0, 10.0),
                time(0, 10),
                vec![time(0, 10)],
                vec![],
            )
            .await;

        tokio::time::sleep(Duration::from_millis(5)).await;

        let hit = cache
            .lookup(&a, &bounds(0.0, 0.0, 10.0, 10.0), time(0, 10))
            .await;
        assert!(hit.is_none());
    }

}
