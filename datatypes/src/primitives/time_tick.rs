use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde::{Deserialize, Serialize};

use crate::primitives::TimeInstance;

#[derive(Eq, PartialEq, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimeTick {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
}

impl TimeTick {
    pub fn new(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> Self {
        Self {
            year,
            month,
            day,
            hour,
            minute,
            second,
        }
    }

    pub fn snap_date<T: Datelike>(&self, date: &T) -> NaiveDate {
        let y = (date.year() / self.year) * self.year;
        let m = (date.month() / self.month) * self.month;
        let d = (date.day() / self.day) * self.day;
        NaiveDate::from_ymd(y, m, d)
    }

    pub fn snap_time<T: Timelike>(&self, time: &T) -> NaiveTime {
        let h = (time.hour() / self.hour) * self.hour;
        let m = (time.minute() / self.minute) * self.minute;
        let s = (time.second() / self.second) * self.second;
        NaiveTime::from_hms(h, m, s)
    }

    pub fn snap_datetime<T: Datelike + Timelike>(&self, datetime: &T) -> NaiveDateTime {
        NaiveDateTime::new(self.snap_date(datetime), self.snap_time(datetime))
    }

    pub fn snap_time_instance(&self, time: TimeInstance) -> Option<TimeInstance> {
        time.as_naive_date_time()
            .map(|naive_datetime| self.snap_datetime(&naive_datetime))
            .map(|snapped_naive_datetime| snapped_naive_datetime.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snap_to_1() {
        // 20. July 2020 07:47:50
        let start = TimeInstance::from_millis(1595231270000);
        let tick = TimeTick::new(1, 1, 1, 1, 1, 1);

        let snapped = tick.snap_time_instance(start);
        assert_eq!(Some(start), snapped);
    }

    #[test]
    fn snap_to_2() {
        // 20. July 2020 07:47:50
        let start = TimeInstance::from_millis(1595231270000);
        let tick = TimeTick::new(2, 2, 2, 2, 2, 2);

        // 20. June 2020 06:46:50
        let expected = TimeInstance::from_millis(1592635610000);
        let snapped = tick.snap_time_instance(start);
        assert_eq!(Some(expected), snapped);
    }
}
