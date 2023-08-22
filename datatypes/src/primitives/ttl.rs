use super::DateTime;
use chrono::Utc;
use postgres_types::{FromSql, ToSql};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};

const MAX_CACHE_TTL_SECONDS: u32 = 31_536_000; // 1 year

/// Config parameter to indicate how long a value may be cached (0 = must not be cached)
///
/// We derive the Serializer here because it makes sense to output the concrete cache ttl.
/// For the deserializer we have a custom implementation to allow "max" as a value.
#[derive(Default, Debug, Clone, Copy, PartialEq, Serialize, PartialOrd)]
pub struct CacheTtlSeconds(u32);

impl CacheTtlSeconds {
    pub fn new(seconds: u32) -> Self {
        Self(seconds.min(MAX_CACHE_TTL_SECONDS))
    }

    pub fn max() -> Self {
        Self(MAX_CACHE_TTL_SECONDS)
    }
}

impl From<CacheTtlSeconds> for CacheHint {
    fn from(value: CacheTtlSeconds) -> Self {
        Self {
            created: DateTime::now(),
            expires: CacheExpiration::seconds(value.0),
        }
    }
}

/// A custom deserializer s.t. the ttl can be specified as a simple field
///
/// "cacheTtl": 1234
/// or
/// "cacheTtl": "max"
/// or if there should be no caching:
/// "cacheTtl": 0
/// or
/// "cacheTtl": null
impl<'de> Deserialize<'de> for CacheTtlSeconds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val: serde_json::Value = Deserialize::deserialize(deserializer)?;

        match val {
            serde_json::Value::Number(n) => {
                if let Some(num) = n.as_u64() {
                    Ok(Self(num as u32))
                } else {
                    Err(D::Error::custom("Invalid number for CacheTtl::Seconds"))
                }
            }
            serde_json::Value::String(s) if s.eq_ignore_ascii_case("max") => {
                Ok(Self(MAX_CACHE_TTL_SECONDS))
            }
            serde_json::Value::Null => Ok(Self(0)),
            _ => Err(D::Error::custom("Invalid value for CacheTtl")),
        }
    }
}

impl ToSql for CacheTtlSeconds {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        w: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        <i32 as ToSql>::to_sql(&(self.0 as i32), ty, w)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <i32 as ToSql>::accepts(ty)
    }

    postgres_types::to_sql_checked!();
}

impl<'a> FromSql<'a> for CacheTtlSeconds {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(<i32 as FromSql>::from_sql(ty, raw)? as u32))
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <i32 as FromSql>::accepts(ty)
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
        Self::no_cache()
    }
}

impl CacheHint {
    pub fn new(expires: CacheExpiration) -> Self {
        Self {
            created: DateTime::now(),
            expires,
        }
    }

    pub fn with_created_and_expires(created: DateTime, expires: CacheExpiration) -> Self {
        Self { created, expires }
    }

    pub fn no_cache() -> Self {
        Self {
            created: DateTime::now(),
            expires: DateTime::now().into(),
        }
    }

    pub fn max_duration() -> Self {
        Self::seconds(MAX_CACHE_TTL_SECONDS)
    }

    pub fn seconds(seconds: u32) -> Self {
        Self {
            created: DateTime::now(),
            expires: (chrono::offset::Utc::now() + chrono::Duration::seconds(seconds.into()))
                .into(),
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

    pub fn total_ttl_seconds(&self) -> CacheTtlSeconds {
        CacheTtlSeconds::new((self.expires.0 - self.created).num_seconds() as u32)
    }

    #[must_use]
    pub fn clone_with_current_datetime(&self) -> Self {
        Self {
            created: DateTime::now(),
            expires: self.expires,
        }
    }
}

/// Type for storing the expiration data to avoid recomputing it from creation date time and ttl
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CacheExpiration(DateTime);

impl From<DateTime> for CacheExpiration {
    fn from(datetime: DateTime) -> Self {
        Self(datetime)
    }
}

impl From<chrono::DateTime<Utc>> for CacheExpiration {
    fn from(datetime: chrono::DateTime<Utc>) -> Self {
        Self(datetime.into())
    }
}

impl CacheExpiration {
    pub fn seconds(seconds: u32) -> Self {
        Self((chrono::offset::Utc::now() + chrono::Duration::seconds(seconds.into())).into())
    }

    pub fn max() -> Self {
        Self::seconds(MAX_CACHE_TTL_SECONDS)
    }

    pub fn no_cache() -> Self {
        Self::seconds(0)
    }

    #[must_use]
    pub fn merged(&self, other: &CacheExpiration) -> CacheExpiration {
        CacheExpiration(self.0.min(other.0))
    }

    pub fn merge_with(&mut self, other: &CacheExpiration) {
        *self = self.merged(other);
    }

    pub fn is_expired(&self) -> bool {
        self.0 < DateTime::now()
    }

    pub fn datetime(&self) -> DateTime {
        self.0
    }

    pub fn seconds_to_expiration(&self) -> u32 {
        (self.0 - DateTime::now()).num_seconds().max(0) as u32
    }
}

impl From<CacheExpiration> for CacheHint {
    fn from(expiration: CacheExpiration) -> Self {
        Self {
            created: DateTime::now(),
            expires: expiration,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_deserializes_ttl() {
        assert_eq!(
            serde_json::from_str::<CacheTtlSeconds>("1234").unwrap(),
            CacheTtlSeconds(1234)
        );
        assert_eq!(
            serde_json::from_str::<CacheTtlSeconds>("\"max\"").unwrap(),
            CacheTtlSeconds(MAX_CACHE_TTL_SECONDS)
        );
        assert_eq!(
            serde_json::from_value::<CacheTtlSeconds>(serde_json::Value::Null).unwrap(),
            CacheTtlSeconds(0)
        );
    }

    #[test]
    fn it_merges_expiration() {
        let now = chrono::offset::Utc::now().into();

        assert_eq!(
            CacheExpiration::max().merged(&CacheExpiration(now)),
            CacheExpiration(now)
        );
    }
}
