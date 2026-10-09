use crate::config::{StacCache, get_config_element};
use crate::datasets::external::stac::StacProviderDataset;
use futures::FutureExt;
use geoengine_datatypes::primitives::TimeInterval;
use geoengine_datatypes::raster::GridIdx2D;
use geoengine_operators::{
    error::Error,
    source::{TileFile, gdal_worker_process::GdalMetadataMapping},
};
use moka::future::Cache;
use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};

pub(crate) type StacQueryCacheResult = Result<Arc<Vec<TileFile>>, Arc<Error>>;

/// Approximate result payload, key, and value size for Moka's weighted capacity.
/// It includes the actual key and Arc value sizes plus known nested allocations.
/// It excludes cache internals, allocator overhead, the dataset registry, and active requests.
pub(crate) fn result_bytes(result: &[TileFile]) -> usize {
    let mut total = std::mem::size_of::<Key>()
        + std::mem::size_of::<Arc<Vec<TileFile>>>()
        + std::mem::size_of::<Vec<TileFile>>();
    for file in result {
        total += std::mem::size_of::<TileFile>() + file.params.file_path.as_os_str().len();
        if let Some(mappings) = &file.params.properties_mapping {
            for mapping in mappings {
                total += std::mem::size_of::<GdalMetadataMapping>()
                    + mapping.source_key.key.len()
                    + mapping.target_key.key.len();
                total += mapping.source_key.domain.as_ref().map_or(0, String::len)
                    + mapping.target_key.domain.as_ref().map_or(0, String::len);
            }
        }
        if let Some(options) = &file.params.gdal_open_options {
            total += options.iter().map(String::len).sum::<usize>();
        }
        if let Some(options) = &file.params.gdal_config_options {
            total += options
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum::<usize>();
        }
    }
    total
}

/// Moka-backed cache and in-flight request coalescer.
///
/// Moka's `try_get_with` is the sole owner of same-key in-flight matching.
/// The dataset registry assigns stable compact keys while retaining full
/// `PartialEq` comparisons, including fields containing floating-point data.
pub(crate) struct StacQueryCache {
    cache: Cache<Key, Arc<Vec<TileFile>>>,
    datasets: Mutex<Vec<StacProviderDataset>>,
    max_bytes: usize,
    ttl: Duration,
}

impl std::fmt::Debug for StacQueryCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StacQueryCache")
            .field("max_bytes", &self.max_bytes)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Key {
    dataset_id: u64,
    cell: GridIdx2D,
    time_start: i64,
    time_end: i64,
}

#[derive(Debug)]
enum InitError {
    Fetch(Arc<Error>),
    /// Moka shares initializer errors with current waiters but does not cache
    /// them, so successful results rejected by admission use this variant.
    Uncached(Arc<Vec<TileFile>>),
}

impl Default for StacQueryCache {
    fn default() -> Self {
        let config = get_config_element::<StacCache>()
            .expect("StacCache config must be present in Settings-default.toml");
        Self::new(
            config.size_in_mb * 1024 * 1024,
            Duration::from_secs(config.ttl_secs),
        )
    }
}

impl StacQueryCache {
    pub(crate) fn new(max_bytes: usize, ttl: Duration) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_bytes as u64)
            .time_to_live(ttl)
            .weigher(|_key: &Key, result: &Arc<Vec<TileFile>>| {
                u32::try_from(result_bytes(result.as_slice())).unwrap_or(u32::MAX)
            })
            .build();
        Self {
            cache,
            datasets: Mutex::new(Vec::new()),
            max_bytes,
            ttl,
        }
    }

    /// Fetches a cell once for concurrent callers and caches successful results that fit.
    ///
    /// Fetches run in a detached task so cancellation of the initiating tile
    /// request does not cancel work shared with other callers. Errors are shared
    /// with current waiters and remain retryable; successful results rejected
    /// by size or TTL admission are still returned to current waiters.
    pub(crate) async fn get_or_fetch<F, Fut>(
        self: &Arc<Self>,
        dataset: StacProviderDataset,
        cell: GridIdx2D,
        time: TimeInterval,
        fetch: F,
    ) -> StacQueryCacheResult
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<Vec<TileFile>, Error>> + Send + 'static,
    {
        let key = Key {
            dataset_id: self.dataset_id(dataset),
            cell,
            time_start: time.start().inner(),
            time_end: time.end().inner(),
        };

        // Keep warm hits off the task-spawn and try_get_with paths.
        if let Some(result) = self.cache.get(&key).await {
            return Ok(result);
        }

        // Detaching the task is essential: dropping the request that first
        // missed must not cancel Moka's initializer and restart the HTTP call.
        let cache = self.clone();
        match tokio::spawn(async move { cache.get_or_initialize(key, fetch).await }).await {
            Ok(output) => output,
            Err(error) => Err(Arc::new(Error::QueryingProcessorFailed {
                source: Box::new(error),
            })),
        }
    }

    /// Runs Moka's shared initializer, translating fetch panics and uncached successes
    /// into values that current waiters can share without retaining them in the cache.
    async fn get_or_initialize<F, Fut>(&self, key: Key, fetch: F) -> StacQueryCacheResult
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<Vec<TileFile>, Error>> + Send + 'static,
    {
        let cache = &self.cache;
        let max_bytes = self.max_bytes;
        let ttl = self.ttl;

        match cache
            .try_get_with(key, async move {
                // Invocation is inside the caught async body, so both a
                // synchronous panic and a panic while polling the future are
                // published as one shared terminal error.
                let fetched = AssertUnwindSafe(async move { fetch().await })
                    .catch_unwind()
                    .await;
                let result = match fetched {
                    Ok(Ok(result)) => Arc::new(result),
                    Ok(Err(error)) => return Err(InitError::Fetch(Arc::new(error))),
                    Err(_) => {
                        return Err(InitError::Fetch(Arc::new(Error::QueryingProcessorFailed {
                            source: "STAC cell search worker panicked".into(),
                        })));
                    }
                };

                let bytes = result_bytes(result.as_slice());
                // These successful values must reach current waiters while
                // remaining absent from the reusable cache.
                if max_bytes == 0
                    || ttl.is_zero()
                    || bytes > max_bytes
                    || u32::try_from(bytes).is_err()
                {
                    return Err(InitError::Uncached(result));
                }

                Ok(result)
            })
            .await
        {
            Ok(result) => Ok(result),
            Err(error) => match error.as_ref() {
                InitError::Fetch(error) => Err(error.clone()),
                InitError::Uncached(result) => Ok(result.clone()),
            },
        }
    }

    fn dataset_id(&self, dataset: StacProviderDataset) -> u64 {
        let mut datasets = self
            .datasets
            .lock()
            .expect("STAC dataset registry mutex is not poisoned");
        if let Some(id) = datasets.iter().position(|known| known == &dataset) {
            return id as u64;
        }

        // A registry position avoids deriving Eq or Hash for dataset fields
        // containing floats while keeping full PartialEq identity semantics.
        let id = datasets.len() as u64;
        datasets.push(dataset);
        id
    }
}

#[cfg(test)]
mod tests {
    use super::{StacQueryCache, StacQueryCacheResult, TileFile, result_bytes};
    use crate::datasets::external::stac::{
        StacAssetBand, StacProviderDataset, StacProviderDatasetBand,
    };
    use geoengine_datatypes::{
        primitives::{SpatialResolution, TimeInterval},
        raster::{GeoTransform, GridBoundingBox2D, GridIdx2D, RasterDataType},
        spatial_reference::SpatialReference,
    };
    use geoengine_operators::{engine::SpatialGridDescriptor, error::Error};
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    fn dataset(description: &str) -> StacProviderDataset {
        StacProviderDataset {
            name: "cache contract".into(),
            description: description.into(),
            data_type: RasterDataType::U16,
            resolution: SpatialResolution::new_unchecked(1., 1.),
            projection: SpatialReference::epsg_4326(),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new((0., 1.).into(), 1., -1.),
                GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([0, 0])).unwrap(),
            ),
            bands: vec![StacProviderDatasetBand::new_unitless(StacAssetBand {
                asset_title: "a".into(),
                band_name: None,
            })],
        }
    }

    async fn get<F, Fut>(
        cache: Arc<StacQueryCache>,
        dataset: StacProviderDataset,
        x: isize,
        time: TimeInterval,
        fetch: F,
    ) -> StacQueryCacheResult
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<Vec<TileFile>, Error>> + Send + 'static,
    {
        cache
            .get_or_fetch(dataset, GridIdx2D::new_y_x(0, x), time, fetch)
            .await
    }

    #[tokio::test]
    async fn successful_contract() {
        let time = TimeInterval::new_unchecked(0_i64, 10_i64);
        let cache = Arc::new(StacQueryCache::new(4096, Duration::from_mins(1)));
        let count = Arc::new(AtomicUsize::new(0));
        let first_count = count.clone();
        get(
            cache.clone(),
            dataset("same"),
            0,
            time,
            move || async move {
                first_count.fetch_add(1, Ordering::SeqCst);
                Ok(Vec::new())
            },
        )
        .await
        .unwrap();

        // Equality is the full provider definition. Independently changing
        // resolution, projection, or band addressing under the same dataset name
        // must miss for the same cell and time.
        let mut resolution_variant = dataset("same");
        resolution_variant.resolution = SpatialResolution::new_unchecked(2., 1.);
        let mut projection_variant = dataset("same");
        projection_variant.projection = SpatialReference::web_mercator();
        let mut band_variant = dataset("same");
        band_variant.bands[0].asset_band.asset_title = "different asset".to_owned();
        for variant in [resolution_variant, projection_variant, band_variant] {
            let variant_count = count.clone();
            get(cache.clone(), variant, 0, time, move || async move {
                variant_count.fetch_add(1, Ordering::SeqCst);
                Ok(Vec::new())
            })
            .await
            .unwrap();
        }
        let changed_time_count = count.clone();
        let changed_time = TimeInterval::new_unchecked(10_i64, 20_i64);
        get(
            cache.clone(),
            dataset("same"),
            0,
            changed_time,
            move || async move {
                changed_time_count.fetch_add(1, Ordering::SeqCst);
                Ok(Vec::new())
            },
        )
        .await
        .unwrap();
        let warm_count = count.clone();
        get(
            cache.clone(),
            dataset("same"),
            0,
            time,
            move || async move {
                warm_count.fetch_add(1, Ordering::SeqCst);
                Ok(Vec::new())
            },
        )
        .await
        .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 5);

        // A different key progresses while the first initializer is held. This
        // checks that the cache does not serialize independent requests.
        let gate = Arc::new(tokio::sync::Notify::new());
        let started = tokio::sync::oneshot::channel();
        let gate_for_fetch = gate.clone();
        let (started_tx, started_rx) = started;
        let held_cache = cache.clone();
        let held = tokio::spawn(async move {
            get(held_cache, dataset("same"), 1, time, move || async move {
                let _ = started_tx.send(());
                gate_for_fetch.notified().await;
                Ok(Vec::new())
            })
            .await
        });
        started_rx.await.unwrap();
        let independent_cache = cache.clone();
        tokio::time::timeout(
            Duration::from_secs(2),
            get(
                independent_cache,
                dataset("same"),
                2,
                time,
                move || async move { Ok(Vec::new()) },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        gate.notify_one();
        held.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn coalescing_and_cancellation_contract() {
        let time = TimeInterval::new_unchecked(0_i64, 10_i64);
        let cache = Arc::new(StacQueryCache::new(4096, Duration::from_mins(1)));
        let count = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(tokio::sync::Notify::new());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let initiating_cache = cache.clone();
        let initiating_count = count.clone();
        let initiating_gate = gate.clone();
        let initiating = tokio::spawn(async move {
            get(
                initiating_cache,
                dataset("same"),
                0,
                time,
                move || async move {
                    initiating_count.fetch_add(1, Ordering::SeqCst);
                    let _ = started_tx.send(());
                    initiating_gate.notified().await;
                    Ok(Vec::new())
                },
            )
            .await
        });
        started_rx.await.unwrap();
        initiating.abort();

        let follower_cache = cache.clone();
        let follower_count = count.clone();
        let follower = tokio::spawn(async move {
            get(
                follower_cache,
                dataset("same"),
                0,
                time,
                move || async move {
                    follower_count.fetch_add(1, Ordering::SeqCst);
                    Ok(Vec::new())
                },
            )
            .await
        });
        // Give the follower a chance to enter Moka's pending same-key lookup
        // before releasing the initializer that survived caller cancellation.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        gate.notify_one();
        follower.await.unwrap().unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    async fn shared_uncached_success_contract(max_bytes: usize, ttl: Duration) {
        let time = TimeInterval::new_unchecked(0_i64, 10_i64);
        let cache = Arc::new(StacQueryCache::new(max_bytes, ttl));
        let count = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(tokio::sync::Notify::new());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let first_cache = cache.clone();
        let first_count = count.clone();
        let first_gate = gate.clone();
        let first = tokio::spawn(async move {
            get(first_cache, dataset("same"), 0, time, move || async move {
                first_count.fetch_add(1, Ordering::SeqCst);
                let _ = started_tx.send(());
                first_gate.notified().await;
                Ok(Vec::new())
            })
            .await
        });
        started_rx.await.unwrap();
        let second_cache = cache.clone();
        let second_count = count.clone();
        let second = tokio::spawn(async move {
            get(second_cache, dataset("same"), 0, time, move || async move {
                second_count.fetch_add(1, Ordering::SeqCst);
                Ok(Vec::new())
            })
            .await
        });
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        gate.notify_one();
        let first = first.await.unwrap().unwrap();
        let second = second.await.unwrap().unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    async fn shared_failure_and_retry_contract(panic: bool) {
        let time = TimeInterval::new_unchecked(0_i64, 10_i64);
        let cache = Arc::new(StacQueryCache::new(4096, Duration::from_mins(1)));
        let count = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(tokio::sync::Notify::new());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let first_cache = cache.clone();
        let first_count = count.clone();
        let first_gate = gate.clone();
        let first = tokio::spawn(async move {
            get(first_cache, dataset("same"), 0, time, move || async move {
                first_count.fetch_add(1, Ordering::SeqCst);
                let _ = started_tx.send(());
                first_gate.notified().await;
                assert!(!panic, "shared STAC initializer panic");
                Err(Error::QueryingProcessorFailed {
                    source: "shared test failure".into(),
                })
            })
            .await
        });
        started_rx.await.unwrap();
        let second_cache = cache.clone();
        let second_count = count.clone();
        let second = tokio::spawn(async move {
            get(second_cache, dataset("same"), 0, time, move || async move {
                second_count.fetch_add(1, Ordering::SeqCst);
                Err(Error::QueryingProcessorFailed {
                    source: "unexpected second initializer".into(),
                })
            })
            .await
        });
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        gate.notify_one();
        let first = first.await.unwrap().unwrap_err();
        let second = second.await.unwrap().unwrap_err();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(count.load(Ordering::SeqCst), 1);

        get(cache, dataset("same"), 0, time, move || async move {
            Ok(Vec::new())
        })
        .await
        .unwrap();
    }

    async fn uncached_success_contract(max_bytes: usize, ttl: Duration) {
        let time = TimeInterval::new_unchecked(0_i64, 10_i64);
        let cache = Arc::new(StacQueryCache::new(max_bytes, ttl));
        let calls = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let calls = calls.clone();
            get(
                cache.clone(),
                dataset("same"),
                0,
                time,
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(Vec::new())
                },
            )
            .await
            .unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn error_retry_contract() {
        let time = TimeInterval::new_unchecked(0_i64, 10_i64);
        let cache = Arc::new(StacQueryCache::new(4096, Duration::from_mins(1)));
        let error = get(
            cache.clone(),
            dataset("same"),
            0,
            time,
            move || async move {
                Err(Error::QueryingProcessorFailed {
                    source: "shared test failure".into(),
                })
            },
        )
        .await;
        assert!(error.is_err());
        get(cache, dataset("same"), 0, time, move || async move {
            Ok(Vec::new())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn synchronous_panic_retry_contract() {
        let time = TimeInterval::new_unchecked(0_i64, 10_i64);
        let cache = Arc::new(StacQueryCache::new(0, Duration::from_mins(1)));
        let failed = get(
            cache.clone(),
            dataset("same"),
            0,
            time,
            move || -> std::future::Ready<Result<Vec<TileFile>, Error>> {
                panic!("synchronous panic before returning the fetch future");
            },
        )
        .await;
        assert!(failed.is_err());
        get(cache, dataset("same"), 0, time, move || async move {
            Ok(Vec::new())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn ttl_expiry_contract() {
        let time = TimeInterval::new_unchecked(0_i64, 10_i64);
        let cache = Arc::new(StacQueryCache::new(4096, Duration::from_millis(5)));
        let calls = Arc::new(AtomicUsize::new(0));
        let first_calls = calls.clone();
        get(
            cache.clone(),
            dataset("same"),
            0,
            time,
            move || async move {
                first_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Vec::new())
            },
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let second_calls = calls.clone();
        get(cache, dataset("same"), 0, time, move || async move {
            second_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn admission_rejection_no_retention() {
        uncached_success_contract(0, Duration::from_mins(1)).await;
        uncached_success_contract(4096, Duration::ZERO).await;
        let r: Vec<TileFile> = Vec::new();
        uncached_success_contract(result_bytes(&r) - 1, Duration::from_mins(1)).await;
    }

    #[tokio::test]
    async fn shared_uncached_success() {
        let small = result_bytes(&[]) - 1;
        shared_uncached_success_contract(0, Duration::from_mins(1)).await;
        shared_uncached_success_contract(small, Duration::from_mins(1)).await;
        shared_uncached_success_contract(4096, Duration::ZERO).await;
    }

    #[tokio::test]
    async fn shared_errors_and_panic_retry() {
        shared_failure_and_retry_contract(false).await;
        shared_failure_and_retry_contract(true).await;
    }

    #[tokio::test]
    async fn weighted_capacity_after_maintenance() {
        let t = TimeInterval::new_unchecked(0_i64, 10_i64);
        let max = result_bytes(&[]);
        let c = Arc::new(StacQueryCache::new(max, Duration::from_mins(1)));
        for x in [0, 1] {
            get(c.clone(), dataset("same"), x, t, move || async move {
                Ok(Vec::new())
            })
            .await
            .unwrap();
        }
        c.cache.run_pending_tasks().await;
        assert!(c.cache.weighted_size() <= max as u64);
    }
}
