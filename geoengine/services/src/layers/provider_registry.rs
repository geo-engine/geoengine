use super::external::DataProvider;
use crate::config::{ProviderCache, get_config_element};
use crate::error::Result;
use geoengine_datatypes::dataset::DataProviderId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
struct CachedProviderEntry {
    provider: Arc<dyn DataProvider>,
    last_used: Instant,
}

#[derive(Debug)]
pub struct DataProviderRegistry {
    entries: Mutex<HashMap<DataProviderId, CachedProviderEntry>>,
    max_entries: usize,
    max_idle: Duration,
}

impl Default for DataProviderRegistry {
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

impl DataProviderRegistry {
    /// Get a cached provider, or initialise one and cache it.
    ///
    /// # TOCTOU race (known, acceptable)
    ///
    /// The lock is released between the first cache check and the call to
    /// `initialize()`.  If two concurrent requests arrive for the same key,
    /// both will miss the cache, both will call `initialize()` (expensive:
    /// DB query + provider construction), and the second one to re-acquire
    /// the lock will discard its freshly-built provider in favour of the
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
            CachedProviderEntry {
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

    fn evict(entries: &mut HashMap<DataProviderId, CachedProviderEntry>, max_idle: Duration) {
        let now = Instant::now();
        entries.retain(|_, entry| now.duration_since(entry.last_used) <= max_idle);
    }

    fn evict_lru_one(entries: &mut HashMap<DataProviderId, CachedProviderEntry>) {
        if let Some((lru_key, _)) = entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, entry)| (key.clone(), entry.last_used))
        {
            entries.remove(&lru_key);
        }
    }
}
