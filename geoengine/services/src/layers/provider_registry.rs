use super::external::DataProvider;
use crate::config::{ProviderCache, get_config_element};
use crate::error::Result;
use geoengine_datatypes::dataset::DataProviderId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::sync::Notify;

#[derive(Debug, Clone)]
struct CachedProviderEntry {
    provider: Arc<dyn DataProvider>,
    last_used: Instant,
}

/// Tracks an in-flight provider initialisation so concurrent requests for the
/// same key can wait for the result instead of duplicating the work.
#[derive(Debug)]
struct PendingEntry {
    notify: Notify,
    state: std::sync::Mutex<PendingState>,
}

/// State of an in-flight provider initialisation.
#[derive(Debug)]
enum PendingState {
    /// Initialisation is still running.
    Running,
    /// Initialisation completed successfully.
    Ready(Arc<dyn DataProvider>),
    /// Initialisation failed.
    Failed,
}

#[derive(Debug)]
struct RegistryInner {
    entries: HashMap<DataProviderId, CachedProviderEntry>,
    pending: HashMap<DataProviderId, Arc<PendingEntry>>,
}

#[derive(Debug)]
pub struct DataProviderRegistry {
    inner: Mutex<RegistryInner>,
    max_entries: usize,
    max_idle: Duration,
}

impl Default for DataProviderRegistry {
    fn default() -> Self {
        let config = get_config_element::<ProviderCache>()
            .expect("ProviderCache config must be present in Settings-default.toml");
        Self {
            inner: Mutex::new(RegistryInner {
                entries: HashMap::default(),
                pending: HashMap::default(),
            }),
            max_entries: config.max_entries,
            max_idle: Duration::from_secs(config.max_idle_secs),
        }
    }
}

impl DataProviderRegistry {
    /// Get a cached provider, or initialise one and cache it.
    ///
    /// Concurrent requests for the same key coordinate via
    /// [`Notify`](tokio::sync::Notify): only one caller performs the expensive
    /// `initialize()` while the others wait and then receive the cached result.
    pub async fn get_or_try_insert_with<F, Fut>(
        &self,
        key: DataProviderId,
        initialize: F,
    ) -> Result<Arc<dyn DataProvider>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Arc<dyn DataProvider>>>,
    {
        // Fast path – try the cache without any coordination.
        {
            let mut inner = self.inner.lock().await;
            Self::evict(&mut inner.entries, self.max_idle);

            if let Some(entry) = inner.entries.get_mut(&key) {
                entry.last_used = Instant::now();
                return Ok(entry.provider.clone());
            }
        }

        // Claim the initialisation slot, or subscribe to one that is already
        // in-flight.
        let pending = {
            let mut inner = self.inner.lock().await;
            Self::evict(&mut inner.entries, self.max_idle);

            // Double-check after acquiring the lock.
            if let Some(entry) = inner.entries.get_mut(&key) {
                entry.last_used = Instant::now();
                return Ok(entry.provider.clone());
            }

            match inner.pending.entry(key) {
                std::collections::hash_map::Entry::Occupied(o) => {
                    // Another task is already initialising – wait for it.
                    let entry = o.get().clone(); // Arc bump
                    drop(inner);

                    entry.notify.notified().await;

                    // Read the outcome from the pending entry directly.
                    // (The initialiser may not have re-acquired the cache lock
                    // yet, but the state is already final.)
                    match *entry.state.lock().unwrap() {
                        PendingState::Ready(ref provider) => {
                            return Ok(provider.clone());
                        }
                        PendingState::Failed => {
                            return Err(crate::error::Error::UnknownProviderId);
                        }
                        PendingState::Running => {
                            // Should not happen after `notified()`, but fall
                            // through to the cache check as a safety net.
                        }
                    }

                    // Fallback: re-check the cache.
                    let mut inner = self.inner.lock().await;
                    Self::evict(&mut inner.entries, self.max_idle);
                    if let Some(cached) = inner.entries.get_mut(&key) {
                        cached.last_used = Instant::now();
                        return Ok(cached.provider.clone());
                    }

                    return Err(crate::error::Error::UnknownProviderId);
                }
                std::collections::hash_map::Entry::Vacant(v) => {
                    let entry = Arc::new(PendingEntry {
                        notify: Notify::new(),
                        state: std::sync::Mutex::new(PendingState::Running),
                    });
                    v.insert(entry.clone());
                    entry
                }
            }
        };

        // ── We are the designated initialiser ──────────────────────────────
        let init_result = initialize().await;

        // Store the outcome, populate the cache on success, and wake waiters.
        {
            let mut inner = self.inner.lock().await;

            // Remove the pending marker **before** inserting into the cache so
            // that a racing `invalidate_provider` that clears the cache will
            // also remove the pending entry (guarantee: no stale pending entry
            // survives after cache eviction).
            inner.pending.remove(&key);

            match init_result {
                Ok(ref provider) => {
                    // Record the result so waiters that already retrieved the
                    // `Arc` before the cache entry was created can still get
                    // the provider.
                    *pending.state.lock().unwrap() = PendingState::Ready(provider.clone());

                    Self::evict(&mut inner.entries, self.max_idle);
                    if inner.entries.len() >= self.max_entries {
                        Self::evict_lru_one(&mut inner.entries);
                    }
                    inner.entries.insert(
                        key,
                        CachedProviderEntry {
                            provider: provider.clone(),
                            last_used: Instant::now(),
                        },
                    );
                }
                Err(ref _e) => {
                    *pending.state.lock().unwrap() = PendingState::Failed;
                }
            }

            pending.notify.notify_waiters();
        }

        init_result
    }

    pub async fn invalidate_provider(&self, provider_id: DataProviderId) {
        let mut inner = self.inner.lock().await;
        inner.entries.retain(|key, _| *key != provider_id);
        // Also remove any in-flight pending entry – a provider that is being
        // invalidated should not be re-cached.
        inner.pending.remove(&provider_id);
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
