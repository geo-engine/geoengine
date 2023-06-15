use serde::{Deserialize, Serialize};

use super::DateTime;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CacheTtlSeconds(pub Option<u32>);

impl CacheTtlSeconds {
    pub fn cache_until(self) -> CacheUntil {
        CacheUntil(self.0.map(|ttl| {
            let ttl = chrono::Duration::seconds(ttl.into());
            let now = chrono::offset::Utc::now();
            let now = now + ttl;
            DateTime::from(now)
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CacheUntil(pub Option<DateTime>);

impl CacheUntil {
    #[must_use]
    pub fn merged(&self, other: &CacheUntil) -> CacheUntil {
        match (self.0, other.0) {
            (Some(a), Some(b)) => CacheUntil(Some(a.min(b))),
            (Some(a), None) => CacheUntil(Some(a)),
            (None, Some(b)) => CacheUntil(Some(b)),
            (None, None) => CacheUntil(None),
        }
    }

    pub fn merge_with(&mut self, other: &CacheUntil) {
        self.0 = self.merged(other).0;
    }

    pub fn is_expired(&self) -> bool {
        match self.0 {
            Some(expiration) => expiration > DateTime::now(),
            None => false,
        }
    }
}
