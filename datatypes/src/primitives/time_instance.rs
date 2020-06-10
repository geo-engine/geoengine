use chrono::{DateTime, TimeZone, Utc, NaiveDateTime};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct TimeInstance(i64);

impl TimeInstance {

    pub fn as_utc_date_time(self) -> DateTime<Utc> {
        Utc.timestamp_millis(self.0)
    } 

    pub fn as_naive_date_time(self) -> NaiveDateTime {
        self.as_utc_date_time().naive_utc()
    }
}

impl TimeInstance {
    pub fn inner(self) -> i64 {
        self.0
    }
}

impl Into<TimeInstance> for i64 {
    fn into(self) -> TimeInstance {
        TimeInstance(self)
    }
}

impl Into<i64> for TimeInstance {
    fn into(self) -> i64 {
        self.0
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

impl AsRef<i64> for TimeInstance {
    fn as_ref(&self) -> &i64 {
        &self.0
    }
}
