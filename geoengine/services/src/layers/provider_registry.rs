use super::external::DataProvider;
use crate::error::Result;
use geoengine_datatypes::dataset::DataProviderId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const DEFAULT_MAX_ENTRIES: usize = 256;
const DEFAULT_MAX_IDLE_SECS: u64 = 30 * 60;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProviderCacheKey {
    pub user_id: String,
    pub provider_id: DataProviderId,
}

#[derive(Debug, Clone)]
struct CachedProviderEntry {
    provider: Arc<dyn DataProvider>,
    last_used: Instant,
}

#[derive(Debug)]
pub struct DataProviderRegistry {
    entries: Mutex<HashMap<ProviderCacheKey, CachedProviderEntry>>,
    max_entries: usize,
    max_idle: Duration,
}

impl Default for DataProviderRegistry {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::default()),
            max_entries: DEFAULT_MAX_ENTRIES,
            max_idle: Duration::from_secs(DEFAULT_MAX_IDLE_SECS),
        }
    }
}

impl DataProviderRegistry {
    pub async fn get_or_try_insert_with<F, Fut>(
        &self,
        key: ProviderCacheKey,
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
        entries.retain(|key, _| key.provider_id != provider_id);
    }

    fn evict(entries: &mut HashMap<ProviderCacheKey, CachedProviderEntry>, max_idle: Duration) {
        let now = Instant::now();
        entries.retain(|_, entry| now.duration_since(entry.last_used) <= max_idle);
    }

    fn evict_lru_one(entries: &mut HashMap<ProviderCacheKey, CachedProviderEntry>) {
        if let Some((lru_key, _)) = entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, entry)| (key.clone(), entry.last_used))
        {
            entries.remove(&lru_key);
        }
    }
}
