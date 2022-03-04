use crate::primitives::error;
use crate::util::Result;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
#[cfg(feature = "postgres")]
use postgres_types::private::BytesMut;
#[cfg(feature = "postgres")]
use postgres_types::{FromSql, IsNull, ToSql, Type};
use serde::{Deserialize, Serialize};
use snafu::ensure;
#[cfg(feature = "postgres")]
use snafu::Error;
use std::{
    convert::TryFrom,
    fmt::Formatter,
    ops::{Add, Sub},
    str::FromStr,
};

#[derive(Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(C)]
pub struct TimeInstance(i64);

impl TimeInstance {
    pub fn from_millis(millis: i64) -> Result<Self> {
        ensure!(
            Self::MIN.inner() <= millis && millis <= Self::MAX.inner(),
            error::InvalidTimeInstance {
                min: Self::MIN,
                max: Self::MAX,
                is: millis
            }
        );

        Ok(TimeInstance(millis))
    }

    pub const fn from_millis_unchecked(millis: i64) -> Self {
        TimeInstance(millis)
    }

    pub fn as_utc_date_time(self) -> Option<DateTime<Utc>> {
        Utc.timestamp_millis_opt(self.0).single()
    }

    pub fn as_naive_date_time(self) -> Option<NaiveDateTime> {
        self.as_utc_date_time().map(|t| t.naive_utc())
    }

    pub fn as_rfc3339(self) -> String {
        let instance = self.clamp(TimeInstance::MIN, TimeInstance::MAX);

        instance
            .as_utc_date_time()
            .expect("TimeInstance is not valid")
            .to_rfc3339()
    }

    pub const fn inner(self) -> i64 {
        self.0
    }

    pub fn now() -> Self {
        Self::from(chrono::offset::Utc::now())
    }

    pub const MIN: Self = TimeInstance::from_millis_unchecked(-8_334_632_851_200_001 + 1);
    pub const MAX: Self = TimeInstance::from_millis_unchecked(8_210_298_412_800_000 - 1);
}

impl std::fmt::Display for TimeInstance {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_rfc3339())
    }
}

impl From<NaiveDateTime> for TimeInstance {
    fn from(date_time: NaiveDateTime) -> Self {
        TimeInstance::from_millis(date_time.timestamp_millis()).expect("valid for chrono datetimes")
    }
}

impl From<DateTime<Utc>> for TimeInstance {
    fn from(date_time: DateTime<Utc>) -> Self {
        TimeInstance::from_millis(date_time.timestamp_millis()).expect("valid for chrono datetimes")
    }
}

impl TryFrom<i64> for TimeInstance {
    type Error = crate::error::Error;

    fn try_from(milliseconds: i64) -> Result<Self> {
        TimeInstance::from_millis(milliseconds)
    }
}

impl From<TimeInstance> for i64 {
    fn from(time_instance: TimeInstance) -> Self {
        time_instance.inner()
    }
}

#[cfg(feature = "postgres")]
impl ToSql for TimeInstance {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        self.as_utc_date_time().unwrap().to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        <DateTime<Utc> as ToSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.as_utc_date_time().unwrap().to_sql_checked(ty, out)
    }
}

#[cfg(feature = "postgres")]
impl<'a> FromSql<'a> for TimeInstance {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        DateTime::<Utc>::from_sql(ty, raw).map(Into::into)
    }

    fn accepts(ty: &Type) -> bool {
        <DateTime<Utc> as FromSql>::accepts(ty)
    }
}

impl Add<i64> for TimeInstance {
    type Output = Self;

    fn add(self, rhs: i64) -> Self::Output {
        TimeInstance::from_millis_unchecked(self.0 + rhs)
    }
}

impl Sub<i64> for TimeInstance {
    type Output = Self;

    fn sub(self, rhs: i64) -> Self::Output {
        TimeInstance::from_millis_unchecked(self.0 - rhs)
    }
}

impl FromStr for TimeInstance {
    type Err = chrono::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let date_time = DateTime::<chrono::FixedOffset>::parse_from_rfc3339(s)?;
        let date_time = date_time.with_timezone(&Utc);
        Ok(date_time.into())
    }
}

impl<'de> Deserialize<'de> for TimeInstance {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IsoStringOrUnixTimestamp;

        impl<'de> serde::de::Visitor<'de> for IsoStringOrUnixTimestamp {
            type Value = TimeInstance;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("RFC 3339 timestamp string or Unix timestamp integer")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                TimeInstance::from_str(value).map_err(E::custom)
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                TimeInstance::from_millis(v).map_err(E::custom)
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Self::visit_i64(self, v as i64)
            }
        }

        deserializer.deserialize_any(IsoStringOrUnixTimestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_wrt_chrono() {
        assert_eq!(TimeInstance::MIN, TimeInstance::from(chrono::MIN_DATETIME));
        assert_eq!(TimeInstance::MAX, TimeInstance::from(chrono::MAX_DATETIME));
    }
}
