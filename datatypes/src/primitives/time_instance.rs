use super::datetime::DateTimeError;
use super::{DateTime, Duration};
use crate::primitives::error;
use crate::util::Result;
#[cfg(feature = "postgres")]
use postgres_types::private::BytesMut;
#[cfg(feature = "postgres")]
use postgres_types::{FromSql, IsNull, ToSql, Type};
use serde::{Deserialize, Serialize, Serializer};
use snafu::ensure;
#[cfg(feature = "postgres")]
use snafu::Error;
use std::ops::AddAssign;
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

    pub fn as_datetime_string(self) -> String {
        let instance = self.clamp(TimeInstance::MIN, TimeInstance::MAX);

        instance
            .as_date_time()
            .expect("TimeInstance is not valid")
            .to_datetime_string()
    }

    pub fn as_datetime_string_with_millis(self) -> String {
        let instance = self.clamp(TimeInstance::MIN, TimeInstance::MAX);

        instance
            .as_date_time()
            .expect("TimeInstance is not valid")
            .to_datetime_string_with_millis()
    }

    pub const fn inner(self) -> i64 {
        self.0
    }

    pub fn now() -> Self {
        Self::from(DateTime::now())
    }

    /// Converts a `TimeInstance` to a `DateTime`.
    /// If this would overflow the range of `DateTime`, the result is `None`.
    pub fn as_date_time(self) -> Option<DateTime> {
        DateTime::try_from(self).ok()
    }

    /// Returns true if this instance equals `Self::MIN`, i.e., represents the start of time.
    #[inline]
    pub fn is_min(self) -> bool {
        self == Self::MIN
    }

    /// Returns true if this instance equals `Self::MAX`, i.e., represents the end of time.
    #[inline]
    pub fn is_max(self) -> bool {
        self == Self::MAX
    }

    pub const MIN: Self = TimeInstance::from_millis_unchecked(-8_334_632_851_200_001 + 1);
    pub const MAX: Self = TimeInstance::from_millis_unchecked(8_210_298_412_800_000 - 1);

    pub const EPOCH_START: Self = TimeInstance::from_millis_unchecked(0);
}

impl std::fmt::Display for TimeInstance {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let instance = self.clamp(&TimeInstance::MIN, &TimeInstance::MAX);
        let datetime = instance
            .as_date_time()
            .expect("time instance was clamped into valid range");
        write!(f, "{datetime}")
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
        self.as_date_time().to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        <DateTime as ToSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.as_date_time().to_sql_checked(ty, out)
    }
}

#[cfg(feature = "postgres")]
impl<'a> FromSql<'a> for TimeInstance {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        DateTime::from_sql(ty, raw).map(Into::into)
    }

    fn accepts(ty: &Type) -> bool {
        <DateTime as FromSql>::accepts(ty)
    }
}

impl Add<i64> for TimeInstance {
    type Output = Self;

    fn add(self, rhs: i64) -> Self::Output {
        if self.is_min() || self.is_max() {
            // begin and end of time are special values, we don't want to do arithmetics on them
            return self;
        }
        TimeInstance::from_millis_unchecked(self.0 + rhs)
    }
}

impl AddAssign<i64> for TimeInstance {
    fn add_assign(&mut self, rhs: i64) {
        *self = *self + rhs;
    }
}

impl Sub<i64> for TimeInstance {
    type Output = Self;

    fn sub(self, rhs: i64) -> Self::Output {
        if self.is_min() || self.is_max() {
            // begin and end of time are special values, we don't want to do arithmetics on them
            return self;
        }
        TimeInstance::from_millis_unchecked(self.0 - rhs)
    }
}

impl Add<Duration> for TimeInstance {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        if self.is_min() || self.is_max() {
            // begin and end of time are special values, we don't want to do arithmetics on them
            return self;
        }
        TimeInstance::from_millis_unchecked(self.0 + rhs.num_milliseconds())
    }
}

impl Sub<Duration> for TimeInstance {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        if self.is_min() || self.is_max() {
            // begin and end of time are special values, we don't want to do arithmetics on them
            return self;
        }
        TimeInstance::from_millis_unchecked(self.0 - rhs.num_milliseconds())
    }
}

impl Sub<TimeInstance> for TimeInstance {
    type Output = Duration;

    fn sub(self, rhs: Self) -> Self::Output {
        Duration::milliseconds(self.0 - rhs.0)
    }
}

impl FromStr for TimeInstance {
    type Err = DateTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let date_time = DateTime::from_str(s)?;
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

pub fn serialize_as_rfc_3339<S>(
    time_instance: TimeInstance,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    time_instance.as_date_time().serialize(serializer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_wrt_chrono() {
        assert_eq!(TimeInstance::MIN, TimeInstance::from(DateTime::MIN));
        assert_eq!(TimeInstance::MAX, TimeInstance::from(DateTime::MAX));
    }

    #[test]
    fn time_limits() {
        assert_eq!(TimeInstance::MIN + 1, TimeInstance::MIN);
        assert_eq!(TimeInstance::MIN - 1, TimeInstance::MIN);
        assert_eq!(TimeInstance::MAX + 1, TimeInstance::MAX);
        assert_eq!(TimeInstance::MAX - 1, TimeInstance::MAX);
    }

    #[test]
    fn serialize_as_chrono() {
        let time_instance = TimeInstance::from_millis(1_234_567_890_000).unwrap();
        let mut writer = Vec::new();
        let mut serializer = serde_json::Serializer::new(&mut writer);
        serialize_as_rfc_3339(time_instance, &mut serializer).unwrap();
        assert_eq!(
            r#""2009-02-13T23:31:30.000Z""#,
            String::from_utf8(writer).unwrap()
        );
    }
}
