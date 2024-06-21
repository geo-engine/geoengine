use super::datetime::DateTimeError;
use super::{DateTime, Duration};
use crate::primitives::error;
use crate::util::helpers::json_schema_help_link;
use crate::util::Result;
use postgres_types::{FromSql, ToSql};
use schemars::gen::SchemaGenerator;
use schemars::schema::Schema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::borrow::Cow;
use std::ops::AddAssign;
use std::{
    convert::TryFrom,
    fmt::Formatter,
    ops::{Add, Sub},
    str::FromStr,
};

#[derive(Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord, Debug, FromSql, ToSql)]
#[repr(C)]
#[postgres(transparent)]
pub struct TimeInstance(i64);

impl JsonSchema for TimeInstance {
    fn schema_name() -> String {
        "TimeInstance".to_owned()
    }

    fn schema_id() -> Cow<'static, str> {
        Cow::Borrowed(concat!(module_path!(), "::TimeInstance"))
    }

    fn is_referenceable() -> bool {
        true
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
        use schemars::schema::*;
        Schema::Object(SchemaObject {
            subschemas: Some(Box::new(SubschemaValidation {
                one_of: Some(vec![
                    Schema::Object(SchemaObject {
                        metadata: Some(Box::new(Metadata {
                            title: Some("Unix Timestamp".to_owned()),
                            description: Some("Unix timestamp in milliseconds".to_owned()),
                            ..Default::default()
                        })),
                        instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::Integer))),
                        ..Default::default()
                    }),
                    Schema::Object(SchemaObject {
                        metadata: Some(Box::new(Metadata {
                            title: Some("Datetime String".to_owned()),
                            description: Some(
                                "Date and time as defined in RFC 3339, section 5.6".to_owned(),
                            ),
                            ..Default::default()
                        })),
                        instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::String))),
                        format: Some("date-time".to_owned()),
                        extensions: schemars::Map::from([json_schema_help_link(
                            "http://tools.ietf.org/html/rfc3339",
                        )]),
                        ..Default::default()
                    }),
                ]),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

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

    #[allow(clippy::missing_panics_doc)]
    pub fn as_datetime_string(self) -> String {
        let instance = self.clamp(TimeInstance::MIN, TimeInstance::MAX);

        instance
            .as_date_time()
            .expect("TimeInstance is not valid")
            .to_datetime_string()
    }

    #[allow(clippy::missing_panics_doc)]
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

    pub const MIN: Self = TimeInstance::from_millis_unchecked(-8_334_601_228_800_000);
    pub const MAX: Self = TimeInstance::from_millis_unchecked(8_210_266_876_799_999);

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
}
