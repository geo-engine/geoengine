use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(C)]
pub struct TimeInstance(i64);

impl TimeInstance {
    const MIN_VISUALIZABLE_VALUE: i64 = -8_334_632_851_200_001 + 1;
    const MAX_VISUALIZABLE_VALUE: i64 = 8_210_298_412_800_000 - 1;

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

    pub fn min_value() -> Self {
        TimeInstance::from_millis(TimeInstance::MIN_VISUALIZABLE_VALUE)
    }

    pub fn max_value() -> Self {
        TimeInstance::from_millis(TimeInstance::MAX_VISUALIZABLE_VALUE)
    }

    pub fn min(a: Self, b: Self) -> Self {
        TimeInstance::from_millis(i64::min(a.inner(), b.inner()))
    }

    pub fn max(a: Self, b: Self) -> Self {
        TimeInstance::from_millis(i64::max(a.inner(), b.inner()))
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
