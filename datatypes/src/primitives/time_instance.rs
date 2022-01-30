use crate::primitives::error;
use crate::util::Result;
#[cfg(feature = "postgres")]
use postgres_types::private::BytesMut;
#[cfg(feature = "postgres")]
use postgres_types::{FromSql, IsNull, ToSql, Type};
use serde::{Deserialize, Serialize};
use snafu::ensure;
#[cfg(feature = "postgres")]
use snafu::Error;
use std::{convert::TryFrom, ops::Add};
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::{Duration, OffsetDateTime, PrimitiveDateTime, UtcOffset};

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
#[repr(transparent)]
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

    pub fn as_utc_date_time(self) -> Option<OffsetDateTime> {
        let seconds = self.inner() / 1000;
        let millis = self.inner() % 1000;

        let date_time =
            OffsetDateTime::from_unix_timestamp(seconds).ok()? + Duration::milliseconds(millis);

        Some(date_time)
    }

    pub fn as_naive_date_time(self) -> Option<PrimitiveDateTime> {
        let utc_date_time = self.as_utc_date_time()?;
        let naive_date_time = PrimitiveDateTime::new(utc_date_time.date(), utc_date_time.time());
        Some(naive_date_time)
    }

    pub fn as_rfc3339(self) -> String {
        let instance = self.clamp(TimeInstance::MIN, TimeInstance::MAX);

        let utc_date_time = instance
            .as_utc_date_time()
            .expect("works because it is clamped");

        if utc_date_time.year() >= 0 {
            return utc_date_time
                .format(&Rfc3339)
                .expect("Formatting into RFC 3339 does not fail");
        }

        let format = format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond][offset_hour sign:mandatory]:[offset_minute]"
        );

        utc_date_time.format(&format).expect("valid format")
    }

    pub const fn inner(self) -> i64 {
        self.0
    }

    pub fn now() -> Self {
        Self::from(OffsetDateTime::now_utc())
    }

    pub const MIN: Self = TimeInstance::from_millis_unchecked(-377_705_116_800_000);
    pub const MAX: Self = TimeInstance::from_millis_unchecked(253_402_300_799_999);
}

impl From<PrimitiveDateTime> for TimeInstance {
    fn from(date_time: PrimitiveDateTime) -> Self {
        let date_time = date_time.assume_utc();
        let unix_timestamp_seconds = date_time.unix_timestamp();
        let unix_timestamp_millis =
            unix_timestamp_seconds * 1_000 + i64::from(date_time.millisecond());
        TimeInstance::from_millis(unix_timestamp_millis).expect("valid for `time` datetimes")
    }
}

impl From<OffsetDateTime> for TimeInstance {
    fn from(date_time: OffsetDateTime) -> Self {
        let date_time = date_time.to_offset(UtcOffset::UTC);
        let unix_timestamp_seconds = date_time.unix_timestamp();
        let unix_timestamp_millis =
            unix_timestamp_seconds * 1_000 + i64::from(date_time.millisecond());
        TimeInstance::from_millis(unix_timestamp_millis).expect("valid for `time` datetimes")
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
        <OffsetDateTime as ToSql>::accepts(ty)
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
        OffsetDateTime::from_sql(ty, raw).map(Into::into)
    }

    fn accepts(ty: &Type) -> bool {
        <OffsetDateTime as FromSql>::accepts(ty)
    }
}

impl Add<i64> for TimeInstance {
    type Output = Self;

    fn add(self, rhs: i64) -> Self::Output {
        TimeInstance::from_millis_unchecked(self.0 + rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_wrt_chrono() {
        assert_eq!(
            TimeInstance::MIN,
            TimeInstance::from(PrimitiveDateTime::MIN)
        );
        assert_eq!(
            TimeInstance::MAX,
            TimeInstance::from(PrimitiveDateTime::MAX)
        );
    }
}
