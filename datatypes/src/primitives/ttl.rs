use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::DateTime;

/// Config parameter to indicate how long a value may be cached
/// TODO: json deserialization
#[derive(Default, Debug, Clone, Copy, PartialEq)]
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

/// A custom deserializer s.t. the ttl can be specified as a simple field
///
/// "cacheTtl": 1234
/// or
/// "cacheTtl": "unlimited"
/// or if there should be no caching:
/// "cacheTtl": null
impl<'de> Deserialize<'de> for CacheTtl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val: serde_json::Value = Deserialize::deserialize(deserializer)?;

        match val {
            serde_json::Value::Number(n) => {
                if let Some(num) = n.as_u64() {
                    Ok(CacheTtl::Seconds(num as u32))
                } else {
                    Err(D::Error::custom("Invalid number for CacheTtl::Seconds"))
                }
            }
            serde_json::Value::String(s) if s.eq_ignore_ascii_case("unlimited") => {
                Ok(CacheTtl::Unlimited)
            }
            serde_json::Value::Null => Ok(CacheTtl::NoCache),
            _ => Err(D::Error::custom("Invalid value for CacheTtl")),
        }
    }
}

/// Corresponding serialize implementation
impl Serialize for CacheTtl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            CacheTtl::NoCache => serializer.serialize_none(),
            CacheTtl::Seconds(num) => serializer.serialize_u32(num),
            CacheTtl::Unlimited => serializer.serialize_str("unlimited"),
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
            CacheExpiration::Expires(expiration) => *expiration < DateTime::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_deserializes_ttl() {
        assert_eq!(
            serde_json::from_str::<CacheTtl>("1234").unwrap(),
            CacheTtl::Seconds(1234)
        );
        assert_eq!(
            serde_json::from_str::<CacheTtl>("\"unlimited\"").unwrap(),
            CacheTtl::Unlimited
        );
        assert_eq!(
            serde_json::from_value::<CacheTtl>(serde_json::Value::Null).unwrap(),
            CacheTtl::NoCache
        );
    }

    #[test]
    fn it_serializes_ttl() {
        assert_eq!(
            serde_json::to_string(&CacheTtl::Seconds(1234)).unwrap(),
            "1234".to_string()
        );
        assert_eq!(
            serde_json::to_string(&CacheTtl::Unlimited).unwrap(),
            "\"unlimited\"".to_string()
        );
        assert_eq!(
            serde_json::to_value(CacheTtl::NoCache).unwrap(),
            serde_json::Value::Null
        );
    }
}
