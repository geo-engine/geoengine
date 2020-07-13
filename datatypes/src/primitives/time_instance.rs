use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct TimeInstance(i64);

impl TimeInstance {
    pub fn from_millis(millis: i64) -> Self {
        TimeInstance(millis)
    }

    pub fn as_utc_date_time(self) -> DateTime<Utc> {
        Utc.timestamp_millis(self.0)
    }

    pub fn as_naive_date_time(self) -> NaiveDateTime {
        self.as_utc_date_time().naive_utc()
    }

    pub fn as_rfc3339(self) -> String {
        const MIN_VISUALIZABLE_VALUE: i64 = -8_334_632_851_200_001 + 1;
        const MAX_VISUALIZABLE_VALUE: i64 = 8_210_298_412_800_000 - 1;

        if self.inner() < MIN_VISUALIZABLE_VALUE {
            "-262144-01-01T00:00:00+00:00".into()
        } else if self.inner() > MAX_VISUALIZABLE_VALUE {
            "+262143-12-31T23:59:59.999+00:00".into()
        } else {
            self.as_utc_date_time().to_rfc3339()
        }
    }

    pub fn inner(self) -> i64 {
        self.0
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

impl Into<DateTime<Utc>> for TimeInstance {
    fn into(self) -> DateTime<Utc> {
        self.as_utc_date_time()
    }
}

impl Into<NaiveDateTime> for TimeInstance {
    fn into(self) -> NaiveDateTime {
        self.as_naive_date_time()
    }
}
