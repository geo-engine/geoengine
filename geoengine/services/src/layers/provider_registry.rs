use super::external::DataProvider;
use crate::config::{ProviderCache, get_config_element};
use crate::error::Result;
use geoengine_datatypes::dataset::DataProviderId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
struct CachedDataConnectorEntry {
    provider: Arc<dyn DataProvider>,
    last_used: Instant,
}

/// A registry that caches data connectors ([`DataProvider`] instances) for reuse.
/// This avoids expensive database queries and connector construction on every request.
/// It also allows data connectors to keep state (e.g. cached metadata) across requests, improving performance.
#[derive(Debug)]
pub struct DataConnectorRegistry {
    entries: Mutex<HashMap<DataProviderId, CachedDataConnectorEntry>>,
    max_entries: usize,
    max_idle: Duration,
}

impl Default for DataConnectorRegistry {
    fn default() -> Self {
        let config = get_config_element::<ProviderCache>()
            .expect("ProviderCache config must be present in Settings-default.toml");
        Self {
            entries: Mutex::new(HashMap::default()),
            max_entries: config.max_entries,
            max_idle: Duration::from_secs(config.max_idle_secs),
        }
    }
}

impl DataConnectorRegistry {
    /// Get a cached data connector, or initialise one and cache it.
    ///
    /// # TOCTOU race (known, acceptable)
    ///
    /// The lock is released between the first cache check and the call to
    /// `initialize()`.  If two concurrent requests arrive for the same key,
    /// both will miss the cache, both will call `initialize()` (expensive:
    /// DB query + connector construction), and the second one to re-acquire
    /// the lock will discard its freshly-built connector in favour of the
    /// one the first request already stored.  This wastes work under high
    /// concurrency but does **not** corrupt state because the double-check
    /// inside the lock guarantees only one result is stored.
    ///
    /// A future optimisation could serialise initialisation per key with a
    /// secondary `Mutex` or a `tokio::sync::OnceCell`, but for the expected
    /// call volume the current design is adequate.
    pub async fn get_or_try_insert_with<F, Fut>(
        &self,
        key: DataProviderId,
        initialize: F,
    ) -> Result<Arc<dyn DataProvider>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Arc<dyn DataProvider>>>,
    {
        {
            let mut entries = self.entries.lock().await;
            Self::evict(&mut entries, self.max_idle);

            if let Some(entry) = entries.get_mut(&key) {
                entry.last_used = Instant::now();
                return Ok(entry.provider.clone());
            }
        }

        let provider = initialize().await?;

        let mut entries = self.entries.lock().await;
        Self::evict(&mut entries, self.max_idle);

        if let Some(entry) = entries.get_mut(&key) {
            entry.last_used = Instant::now();
            return Ok(entry.provider.clone());
        }

        if entries.len() >= self.max_entries {
            Self::evict_lru_one(&mut entries);
        }

        entries.insert(
            key,
            CachedDataConnectorEntry {
                provider: provider.clone(),
                last_used: Instant::now(),
            },
        );

        Ok(provider)
    }

    pub async fn invalidate_provider(&self, provider_id: DataProviderId) {
        let mut entries = self.entries.lock().await;
        entries.retain(|key, _| *key != provider_id);
    }

    fn evict(entries: &mut HashMap<DataProviderId, CachedDataConnectorEntry>, max_idle: Duration) {
        let now = Instant::now();
        entries.retain(|_, entry| now.duration_since(entry.last_used) <= max_idle);
    }

    fn evict_lru_one(entries: &mut HashMap<DataProviderId, CachedDataConnectorEntry>) {
        if let Some((lru_key, _)) = entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, entry)| (*key, entry.last_used))
        {
            entries.remove(&lru_key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::layers::layer::{LayerCollection, LayerCollectionListOptions};
    use crate::layers::listing::{LayerCollectionId, ProviderCapabilities};
    use async_trait::async_trait;
    use geoengine_datatypes::dataset::DataId;
    use geoengine_datatypes::dataset::LayerId;
    use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
    use geoengine_operators::engine::{
        MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor,
    };
    use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
    use geoengine_operators::source::{
        GdalLoadingInfo, MultiBandGdalLoadingInfo, MultiBandGdalLoadingInfoQueryRectangle,
        OgrSourceDataset,
    };
    use geoengine_operators::util::Result as OpResult;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Minimal mock provider that carries a name for identification in tests.
    #[derive(Debug)]
    struct MockProvider {
        name: String,
    }

    impl MockProvider {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
            }
        }
    }

    // ── LayerCollectionProvider ────────────────────────────────────────────

    #[async_trait]
    impl crate::layers::listing::LayerCollectionProvider for MockProvider {
        fn capabilities(&self) -> ProviderCapabilities {
            ProviderCapabilities {
                listing: false,
                search: crate::layers::listing::SearchCapabilities::none(),
            }
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn description(&self) -> &'static str {
            ""
        }

        #[allow(clippy::unimplemented, reason = "ok in tests")]
        async fn load_layer_collection(
            &self,
            _collection: &LayerCollectionId,
            _options: LayerCollectionListOptions,
        ) -> crate::error::Result<LayerCollection> {
            unimplemented!()
        }

        #[allow(clippy::unimplemented, reason = "ok in tests")]
        async fn get_root_layer_collection_id(&self) -> crate::error::Result<LayerCollectionId> {
            unimplemented!()
        }

        #[allow(clippy::unimplemented, reason = "ok in tests")]
        async fn load_layer(
            &self,
            _id: &LayerId,
        ) -> crate::error::Result<crate::layers::layer::Layer> {
            unimplemented!()
        }
    }

    // ── MetaDataProvider impls (4×) ────────────────────────────────────────

    #[async_trait]
    #[allow(clippy::unimplemented, reason = "ok in tests")]
    impl
        MetaDataProvider<
            MockDatasetDataSourceLoadingInfo,
            VectorResultDescriptor,
            VectorQueryRectangle,
        > for MockProvider
    {
        async fn meta_data(
            &self,
            _id: &DataId,
        ) -> OpResult<
            Box<
                dyn MetaData<
                        MockDatasetDataSourceLoadingInfo,
                        VectorResultDescriptor,
                        VectorQueryRectangle,
                    >,
            >,
        > {
            unimplemented!()
        }
    }

    #[async_trait]
    #[allow(clippy::unimplemented, reason = "ok in tests")]
    impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
        for MockProvider
    {
        async fn meta_data(
            &self,
            _id: &DataId,
        ) -> OpResult<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > {
            unimplemented!()
        }
    }

    #[async_trait]
    #[allow(clippy::unimplemented, reason = "ok in tests")]
    impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
        for MockProvider
    {
        async fn meta_data(
            &self,
            _id: &DataId,
        ) -> OpResult<
            Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        > {
            unimplemented!()
        }
    }

    #[async_trait]
    #[allow(clippy::unimplemented, reason = "ok in tests")]
    impl
        MetaDataProvider<
            MultiBandGdalLoadingInfo,
            RasterResultDescriptor,
            MultiBandGdalLoadingInfoQueryRectangle,
        > for MockProvider
    {
        async fn meta_data(
            &self,
            _id: &DataId,
        ) -> OpResult<
            Box<
                dyn MetaData<
                        MultiBandGdalLoadingInfo,
                        RasterResultDescriptor,
                        MultiBandGdalLoadingInfoQueryRectangle,
                    >,
            >,
        > {
            unimplemented!()
        }
    }

    // ── DataProvider ───────────────────────────────────────────────────────

    #[async_trait]
    #[allow(clippy::unimplemented, reason = "ok in tests")]
    impl super::super::external::DataProvider for MockProvider {
        async fn provenance(
            &self,
            _id: &DataId,
        ) -> crate::error::Result<crate::datasets::listing::ProvenanceOutput> {
            unimplemented!()
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    fn make_id(n: u8) -> DataProviderId {
        DataProviderId::from_u128(u128::from(n))
    }

    /// Returns a registry with a tiny capacity so eviction tests don't need
    /// many entries.
    fn small_registry() -> DataConnectorRegistry {
        DataConnectorRegistry {
            entries: Mutex::new(HashMap::default()),
            max_entries: 2,
            max_idle: Duration::from_mins(1),
        }
    }

    // ── Tests ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn cache_hit_returns_same_instance() {
        let registry = small_registry();
        let id = make_id(1);

        let p1 = registry
            .get_or_try_insert_with(id, || async {
                Ok(Arc::new(MockProvider::new("a")) as Arc<dyn DataProvider>)
            })
            .await
            .unwrap();

        let p2 = registry
            .get_or_try_insert_with(id, || async { panic!("should not be called on cache hit") })
            .await
            .unwrap();

        assert!(Arc::ptr_eq(&p1, &p2));
    }

    #[tokio::test]
    async fn different_keys_get_different_providers() {
        let registry = small_registry();
        let id_a = make_id(1);
        let id_b = make_id(2);

        let pa = registry
            .get_or_try_insert_with(id_a, || async {
                Ok(Arc::new(MockProvider::new("a")) as Arc<dyn DataProvider>)
            })
            .await
            .unwrap();

        let pb = registry
            .get_or_try_insert_with(id_b, || async {
                Ok(Arc::new(MockProvider::new("b")) as Arc<dyn DataProvider>)
            })
            .await
            .unwrap();

        assert!(!Arc::ptr_eq(&pa, &pb));
        assert_eq!(pa.name(), "a");
        assert_eq!(pb.name(), "b");
    }

    #[tokio::test]
    async fn init_error_is_propagated() {
        let registry = small_registry();
        let id = make_id(1);

        let result = registry
            .get_or_try_insert_with(id, || async { Err(Error::UnknownProviderId) })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn retry_after_failed_init() {
        let registry = small_registry();
        let id = make_id(1);
        let call_count = Arc::new(AtomicUsize::new(0));

        // First call fails
        let r1 = registry
            .get_or_try_insert_with(id, || {
                let count = call_count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Err(Error::UnknownProviderId)
                }
            })
            .await;
        assert!(r1.is_err());

        // Retry – should try initialising again
        let r2 = registry
            .get_or_try_insert_with(id, || {
                let count = call_count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(MockProvider::new("ok")) as Arc<dyn DataProvider>)
                }
            })
            .await;
        assert!(r2.is_ok());
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn lru_eviction_removes_oldest() {
        let registry = small_registry(); // max_entries = 2
        let id_a = make_id(1);
        let id_b = make_id(2);
        let id_c = make_id(3);

        registry
            .get_or_try_insert_with(id_a, || async {
                Ok(Arc::new(MockProvider::new("a")) as Arc<dyn DataProvider>)
            })
            .await
            .unwrap();

        registry
            .get_or_try_insert_with(id_b, || async {
                Ok(Arc::new(MockProvider::new("b")) as Arc<dyn DataProvider>)
            })
            .await
            .unwrap();

        // Insert a third – "a" should be evicted
        registry
            .get_or_try_insert_with(id_c, || async {
                Ok(Arc::new(MockProvider::new("c")) as Arc<dyn DataProvider>)
            })
            .await
            .unwrap();

        // "a" must be re-initialised (cache miss)
        let call_count = Arc::new(AtomicUsize::new(0));
        let _ = registry
            .get_or_try_insert_with(id_a, || {
                let c = call_count.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(MockProvider::new("a-new")) as Arc<dyn DataProvider>)
                }
            })
            .await
            .unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn invalidate_removes_entry() {
        let registry = small_registry();
        let id = make_id(1);

        registry
            .get_or_try_insert_with(id, || async {
                Ok(Arc::new(MockProvider::new("x")) as Arc<dyn DataProvider>)
            })
            .await
            .unwrap();

        registry.invalidate_provider(id).await;

        // Must re-initialise
        let call_count = Arc::new(AtomicUsize::new(0));
        let _ = registry
            .get_or_try_insert_with(id, || {
                let c = call_count.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(MockProvider::new("y")) as Arc<dyn DataProvider>)
                }
            })
            .await
            .unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn invalidate_does_not_affect_other_keys() {
        let registry = small_registry();
        let id_a = make_id(1);
        let id_b = make_id(2);

        registry
            .get_or_try_insert_with(id_a, || async {
                Ok(Arc::new(MockProvider::new("a")) as Arc<dyn DataProvider>)
            })
            .await
            .unwrap();

        registry
            .get_or_try_insert_with(id_b, || async {
                Ok(Arc::new(MockProvider::new("b")) as Arc<dyn DataProvider>)
            })
            .await
            .unwrap();

        registry.invalidate_provider(id_a).await;

        // "a" is re-initialised
        let call_count_a = Arc::new(AtomicUsize::new(0));
        let _ = registry
            .get_or_try_insert_with(id_a, || {
                let c = call_count_a.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(MockProvider::new("a-new")) as Arc<dyn DataProvider>)
                }
            })
            .await
            .unwrap();
        assert_eq!(call_count_a.load(Ordering::SeqCst), 1);

        // "b" is still cached
        let call_count_b = Arc::new(AtomicUsize::new(0));
        let _ = registry
            .get_or_try_insert_with(id_b, || {
                let c = call_count_b.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(MockProvider::new("b-again")) as Arc<dyn DataProvider>)
                }
            })
            .await
            .unwrap();
        assert_eq!(call_count_b.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn idle_entry_is_evicted_after_tti() {
        let registry = DataConnectorRegistry {
            entries: Mutex::new(HashMap::default()),
            max_entries: 10,
            max_idle: Duration::from_millis(10),
        };
        let id = make_id(1);

        registry
            .get_or_try_insert_with(id, || async {
                Ok(Arc::new(MockProvider::new("x")) as Arc<dyn DataProvider>)
            })
            .await
            .unwrap();

        // Wait past the idle time
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Must re-initialise
        let call_count = Arc::new(AtomicUsize::new(0));
        let _ = registry
            .get_or_try_insert_with(id, || {
                let c = call_count.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(MockProvider::new("y")) as Arc<dyn DataProvider>)
                }
            })
            .await
            .unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn access_refreshes_idle_timer() {
        let registry = DataConnectorRegistry {
            entries: Mutex::new(HashMap::default()),
            max_entries: 10,
            max_idle: Duration::from_millis(30),
        };
        let id = make_id(1);

        registry
            .get_or_try_insert_with(id, || async {
                Ok(Arc::new(MockProvider::new("x")) as Arc<dyn DataProvider>)
            })
            .await
            .unwrap();

        // Access just before expiry – timer resets
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _ = registry
            .get_or_try_insert_with(id, || async { panic!("should not be called") })
            .await
            .unwrap();

        // Access again before the new timer expires
        tokio::time::sleep(Duration::from_millis(20)).await;
        let call_count = Arc::new(AtomicUsize::new(0));
        let _ = registry
            .get_or_try_insert_with(id, || {
                let c = call_count.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(MockProvider::new("miss")) as Arc<dyn DataProvider>)
                }
            })
            .await
            .unwrap();
        // After the second access + 20ms the entry should still be alive
        // (timer was reset at ~20ms, so another 20ms = 40ms total < 60ms needed for expiry)
        assert_eq!(call_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn concurrent_access_all_get_same_provider() {
        let registry = Arc::new(small_registry());
        let id = make_id(1);

        let mut handles = Vec::new();
        for _ in 0..10 {
            let reg = registry.clone();
            handles.push(tokio::spawn(async move {
                reg.get_or_try_insert_with(id, || async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(Arc::new(MockProvider::new("shared")) as Arc<dyn DataProvider>)
                })
                .await
            }));
        }

        let mut results: Vec<_> = futures_util::future::join_all(handles).await;
        let first = results
            .first_mut()
            .expect("at least one result")
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();

        for r in &results {
            let provider = r.as_ref().unwrap().as_ref().unwrap();
            assert!(
                Arc::ptr_eq(&first, provider),
                "all concurrent callers must receive the same provider instance"
            );
        }
    }
}
