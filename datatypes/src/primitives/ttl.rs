use serde::{Deserialize, Serialize};

use super::DateTime;

/// Config parameter to indicate how long a value may be cached
/// TODO: json deserialization
#[derive(Default, Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum CacheTtl {
    #[default]
    NoCache,
    Seconds(u32), // NonZeroU32(?) or merge types as Seconds(0)
    Unlimited,
}

impl From<CacheTtl> for CacheHint {
    fn from(value: CacheTtl) -> Self {
        Self {
            created: DateTime::now(),
            expires: match value {
                CacheTtl::NoCache => CacheExpiration::NoCache,
                CacheTtl::Seconds(seconds) if seconds == 0 => CacheExpiration::NoCache,
                CacheTtl::Seconds(seconds) => CacheExpiration::Expires(
                    (chrono::offset::Utc::now() + chrono::Duration::seconds(seconds.into())).into(),
                ),
                CacheTtl::Unlimited => CacheExpiration::Unlimited,
            },
        }
    }
}

/// Field for cachable values to indicate when they were created and when they expire
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CacheHint {
    created: DateTime,
    expires: CacheExpiration,
}

impl Default for CacheHint {
    fn default() -> Self {
        Self {
            created: DateTime::now(),
            expires: CacheExpiration::NoCache,
        }
    }
}

impl CacheHint {
    pub fn new(expires: CacheExpiration) -> Self {
        Self {
            created: DateTime::now(),
            expires,
        }
    }

    pub fn no_cache() -> Self {
        Self {
            created: DateTime::now(),
            expires: CacheExpiration::NoCache,
        }
    }

    pub fn unlimited() -> Self {
        Self {
            created: DateTime::now(),
            expires: CacheExpiration::Unlimited,
        }
    }

    pub fn seconds(seconds: u32) -> Self {
        Self {
            created: DateTime::now(),
            expires: CacheExpiration::Expires(
                (chrono::offset::Utc::now() + chrono::Duration::seconds(seconds.into())).into(),
            ),
        }
    }

    #[must_use]
    pub fn merged(&self, other: &CacheHint) -> CacheHint {
        Self {
            created: DateTime::now(),
            expires: self.expires.merged(&other.expires),
        }
    }

    pub fn merge_with(&mut self, other: &CacheHint) {
        *self = self.merged(other);
    }

    pub fn is_expired(&self) -> bool {
        self.expires.is_expired()
    }

    pub fn may_cache(&self) -> bool {
        !self.is_expired()
    }

    pub fn created(&self) -> DateTime {
        self.created
    }

    pub fn expires(&self) -> CacheExpiration {
        self.expires
    }
}

/// Type for storing the expiration data to avoid recomputing it from creation date time and ttl
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum CacheExpiration {
    NoCache,   // TODO: rename: Always?
    Unlimited, // TODO: rename: Never?
    Expires(DateTime),
}

impl CacheExpiration {
    #[must_use]
    pub fn merged(&self, other: &CacheExpiration) -> CacheExpiration {
        match (self, other) {
            (CacheExpiration::NoCache, _) | (_, CacheExpiration::NoCache) => {
                CacheExpiration::NoCache
            }
            (CacheExpiration::Expires(a), CacheExpiration::Unlimited)
            | (CacheExpiration::Unlimited, CacheExpiration::Expires(a)) => {
                CacheExpiration::Expires(*a)
            }
            (CacheExpiration::Expires(a), CacheExpiration::Expires(b)) => {
                CacheExpiration::Expires(*a.min(b))
            }
            (CacheExpiration::Unlimited, CacheExpiration::Unlimited) => CacheExpiration::Unlimited,
        }
    }

    pub fn merge_with(&mut self, other: &CacheExpiration) {
        *self = self.merged(other);
    }

    pub fn is_expired(&self) -> bool {
        match self {
            CacheExpiration::NoCache => true,
            CacheExpiration::Unlimited => false,
            CacheExpiration::Expires(expiration) => *expiration > DateTime::now(),
        }
    }
}
