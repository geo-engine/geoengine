use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use postgres_types::private::BytesMut;
use postgres_types::{FromSql, IsNull, ToSql, Type};
use serde::{Deserialize, Serialize};
use snafu::Error;

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(C)]
pub struct TimeInstance(i64);

impl TimeInstance {
    pub const MIN_VISUALIZABLE_VALUE: i64 = -8_334_632_851_200_001 + 1;
    pub const MAX_VISUALIZABLE_VALUE: i64 = 8_210_298_412_800_000 - 1;

    pub fn from_millis(millis: i64) -> Self {
        TimeInstance(millis)
    }

    pub fn as_utc_date_time(self) -> Option<DateTime<Utc>> {
        Utc.timestamp_millis_opt(self.0).single()
    }

    pub fn as_naive_date_time(self) -> Option<NaiveDateTime> {
        self.as_utc_date_time().map(|t| t.naive_utc())
    }

    pub fn as_rfc3339(self) -> String {
        if self.inner() < TimeInstance::MIN_VISUALIZABLE_VALUE {
            "-262144-01-01T00:00:00+00:00".into()
        } else if self.inner() > TimeInstance::MAX_VISUALIZABLE_VALUE {
            "+262143-12-31T23:59:59.999+00:00".into()
        } else {
            self.as_utc_date_time()
                .expect("TimeInstance is not valid")
                .to_rfc3339()
        }
    }

    pub fn inner(self) -> i64 {
        self.0
    }
}

impl From<NaiveDateTime> for TimeInstance {
    fn from(date_time: NaiveDateTime) -> Self {
        TimeInstance::from_millis(date_time.timestamp_millis())
    }
}

impl From<DateTime<Utc>> for TimeInstance {
    fn from(date_time: DateTime<Utc>) -> Self {
        TimeInstance::from_millis(date_time.timestamp_millis())
    }
}

impl Into<TimeInstance> for i64 {
    fn into(self) -> TimeInstance {
        TimeInstance::from_millis(self)
    }
}

impl Into<i64> for TimeInstance {
    fn into(self) -> i64 {
        self.inner()
    }
}

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

impl<'a> FromSql<'a> for TimeInstance {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        DateTime::<Utc>::from_sql(ty, raw).map(Into::into)
    }

    fn accepts(ty: &Type) -> bool {
        <DateTime<Utc> as FromSql>::accepts(ty)
    }
}
