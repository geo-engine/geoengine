use std::{cmp::max, convert::TryInto, ops::Add};

use chrono::{Datelike, Duration, NaiveDate};
use error::Error::NoDateTimeValid;
use serde::{Deserialize, Serialize};

#[cfg(feature = "postgres")]
use postgres_types::{FromSql, ToSql};
use snafu::OptionExt;

use crate::error::{self, Error};
use crate::primitives::TimeInstance;
use crate::util::Result;

use super::TimeInterval;

/// A time granularity.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(ToSql, FromSql))]
pub enum TimeGranularity {
    Millis,
    Seconds,
    Minutes,
    Hours,
    Days,
    Months,
    Years,
}

/// A step in time.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(ToSql, FromSql))]
pub struct TimeStep {
    pub granularity: TimeGranularity,
    pub step: u32, // TODO: ensure on deserialization it is > 0
}

impl TimeStep {
    /// Resolves how many `TimeSteps` fit into a given `TimeInterval`.
    /// Remember that `TimeInterval` is not inclusive.
    ///
    /// # Errors
    /// This method uses chrono and therefore fails if a `TimeInstance` is outside chronos valid date range.
    ///
    pub fn num_steps_in_interval(self, time_interval: TimeInterval) -> Result<u32> {
        let end = time_interval
            .end()
            .as_naive_date_time()
            .ok_or(NoDateTimeValid {
                time_instance: time_interval.end(),
            })?;
        let start = time_interval
            .start()
            .as_naive_date_time()
            .ok_or(NoDateTimeValid {
                time_instance: time_interval.start(),
            })?;

        let duration = end - start;

        if duration.is_zero() {
            return Ok(0);
        }

        let num_steps: i64 = match self.granularity {
            TimeGranularity::Millis => {
                let s = duration.num_milliseconds() / i64::from(self.step);
                if (duration - Duration::milliseconds(s * i64::from(self.step))).is_zero() {
                    s - 1
                } else {
                    s
                }
            }

            TimeGranularity::Seconds => {
                let s = duration.num_seconds() / i64::from(self.step);
                if (duration - Duration::seconds(s * i64::from(self.step))).is_zero() {
                    s - 1
                } else {
                    s
                }
            }
            TimeGranularity::Minutes => {
                let s = duration.num_minutes() / i64::from(self.step);
                if (duration - Duration::minutes(s * i64::from(self.step))).is_zero() {
                    s - 1
                } else {
                    s
                }
            }
            TimeGranularity::Hours => {
                let s = duration.num_hours() / i64::from(self.step);
                if (duration - Duration::hours(s * i64::from(self.step))).is_zero() {
                    s - 1
                } else {
                    s
                }
            }
            TimeGranularity::Days => {
                let s = duration.num_days() / i64::from(self.step);
                if (duration - Duration::days(s * i64::from(self.step))).is_zero() {
                    s - 1
                } else {
                    s
                }
            }
            TimeGranularity::Months => {
                let diff_years = i64::from(end.year() - start.year());
                let diff_months =
                    i64::from(end.month()) - i64::from(start.month()) + diff_years * 12;
                let steps = diff_months / i64::from(self.step);

                let shifted_start = (time_interval.start()
                    + TimeStep {
                        granularity: TimeGranularity::Months,
                        step: self.step * steps as u32,
                    })
                .expect("is in valid range");

                if (end
                    - shifted_start
                        .as_naive_date_time()
                        .expect("is in valid range"))
                .is_zero()
                {
                    steps - 1
                } else {
                    steps
                }
            }
            TimeGranularity::Years => {
                let steps = i64::from(end.year() - start.year()) / i64::from(self.step);

                let shifted_start = start
                    .with_year(start.year() + (i64::from(self.step) * steps) as i32)
                    .expect("is in valid range");

                if (end - shifted_start).is_zero() {
                    steps - 1
                } else {
                    steps
                }
            }
        };

        Ok(max(0, num_steps as u32))
    }

    /// Snaps a `TimeInstance` relative to a given reference `TimeInstance`.
    ///
    /// # Errors
    /// This method uses chrono and therefore fails if a `TimeInstance` is outside chronos valid date range.
    ///
    pub fn snap_relative<T>(self, reference: T, time_to_snap: T) -> Result<TimeInstance>
    where
        T: TryInto<TimeInstance>,
        Error: From<T::Error>,
    {
        let reference = reference.try_into()?;
        let time_to_snap = time_to_snap.try_into()?;

        let ref_date_time = reference.as_naive_date_time().ok_or(NoDateTimeValid {
            time_instance: reference,
        })?;
        let time_to_snap_date_time = time_to_snap.as_naive_date_time().ok_or(NoDateTimeValid {
            time_instance: time_to_snap,
        })?;

        let snapped_date_time = match self.granularity {
            TimeGranularity::Millis => {
                let diff_duration = time_to_snap_date_time - ref_date_time;
                let snapped_millis =
                    (diff_duration.num_milliseconds() as f64 / f64::from(self.step)).floor() as i64
                        * i64::from(self.step);
                ref_date_time + Duration::milliseconds(snapped_millis)
            }
            TimeGranularity::Seconds => {
                let diff_duration = time_to_snap_date_time - ref_date_time;
                let snapped_seconds = (diff_duration.num_seconds() as f64 / f64::from(self.step))
                    .floor() as i64
                    * i64::from(self.step);
                ref_date_time + Duration::seconds(snapped_seconds)
            }
            TimeGranularity::Minutes => {
                let diff_duration = time_to_snap_date_time - ref_date_time;
                let snapped_minutes = (diff_duration.num_minutes() as f64 / f64::from(self.step))
                    .floor() as i64
                    * i64::from(self.step);
                ref_date_time + Duration::minutes(snapped_minutes)
            }
            TimeGranularity::Hours => {
                let diff_duration = time_to_snap_date_time - ref_date_time;
                let snapped_hours = (diff_duration.num_hours() as f64 / f64::from(self.step))
                    .floor() as i64
                    * i64::from(self.step);
                ref_date_time + Duration::hours(snapped_hours)
            }
            TimeGranularity::Days => {
                let diff_duration = time_to_snap_date_time - ref_date_time;
                let snapped_days = (diff_duration.num_days() as f64 / f64::from(self.step)).floor()
                    as i64
                    * i64::from(self.step);
                ref_date_time + Duration::days(snapped_days)
            }
            TimeGranularity::Months => {
                // first, calculate the total difference in months
                let diff_months = (time_to_snap_date_time.year() - ref_date_time.year()) * 12
                    + (time_to_snap_date_time.month() as i32 - ref_date_time.month() as i32);

                // get the difference in time steps
                let snapped_months = (f64::from(diff_months) / f64::from(self.step)).floor() as i32
                    * self.step as i32;

                let snapped_year = if snapped_months.is_negative() {
                    ref_date_time.year() + (ref_date_time.month() as i32 + snapped_months - 12) / 12
                } else {
                    ref_date_time.year() + (ref_date_time.month() as i32 + snapped_months - 1) / 12
                };

                let mut snapped_month =
                    (ref_date_time.month() as i32 + snapped_months).rem_euclid(12);

                if snapped_month == 0 {
                    snapped_month = 12;
                }

                NaiveDate::from_ymd(snapped_year, snapped_month as u32, ref_date_time.day())
                    .and_time(ref_date_time.time())
            }
            TimeGranularity::Years => {
                let diff = (time_to_snap_date_time.year() - ref_date_time.year()) as i32;
                let snapped_year = ref_date_time.year()
                    + ((f64::from(diff) / f64::from(self.step)).floor() as i32 * self.step as i32);

                NaiveDate::from_ymd(snapped_year, ref_date_time.month(), ref_date_time.day())
                    .and_time(ref_date_time.time())
            }
        };

        Ok(TimeInstance::from(snapped_date_time))
    }
}

impl Add<TimeStep> for TimeInstance {
    type Output = Result<TimeInstance>;

    fn add(self, rhs: TimeStep) -> Self::Output {
        let date_time = self.as_naive_date_time().ok_or(NoDateTimeValid {
            time_instance: self,
        })?;

        let res_date_time = match rhs.granularity {
            TimeGranularity::Millis => date_time + Duration::milliseconds(i64::from(rhs.step)),
            TimeGranularity::Seconds => date_time + Duration::seconds(i64::from(rhs.step)),
            TimeGranularity::Minutes => date_time + Duration::minutes(i64::from(rhs.step)),
            TimeGranularity::Hours => date_time + Duration::hours(i64::from(rhs.step)),
            TimeGranularity::Days => date_time + Duration::days(i64::from(rhs.step)),
            TimeGranularity::Months => {
                let months = date_time.month0() + rhs.step;
                let month = months % 12 + 1;
                let years_from_months = (months / 12) as i32;
                let year = date_time.year() + years_from_months;
                let day = date_time.day();
                NaiveDate::from_ymd_opt(year, month, day)
                    .context(error::DateTimeOutOfBounds { year, month, day })?
                    .and_time(date_time.time())
            }
            TimeGranularity::Years => {
                let year = date_time.year() + rhs.step as i32;
                let month = date_time.month();
                let day = date_time.day();
                NaiveDate::from_ymd_opt(year, month, day)
                    .context(error::DateTimeOutOfBounds { year, month, day })?
                    .and_time(date_time.time())
            }
        };

        Ok(TimeInstance::from(res_date_time))
    }
}

/// An `Iterator` to iterate over time in steps
#[derive(Debug, Clone)]
pub struct TimeStepIter {
    reference_time: TimeInstance,
    time_step: TimeStep,
    curr: u32,
    max: u32,
}

impl TimeStepIter {
    /// Create a new `TimeStepIter` which will include the start `TimeInstance`.
    /// # Errors
    /// This method fails if the interval [start, max) is not valid in chrono.
    pub fn new_incl_start(
        reference_time: TimeInstance,
        time_step: TimeStep,
        steps: u32,
    ) -> Result<Self> {
        let _ = (reference_time
            + TimeStep {
                granularity: time_step.granularity,
                step: time_step.step * steps,
            })?;
        Ok(Self::new_incl_start_unchecked(
            reference_time,
            time_step,
            steps,
        ))
    }

    /// Create a new `TimeStepIter` which will include the start `TimeInstance`.
    /// This method does not check if the generated `TimeInstance` values are valid.
    pub fn new_incl_start_unchecked(
        reference_time: TimeInstance,
        time_step: TimeStep,
        steps: u32,
    ) -> Self {
        Self {
            reference_time,
            time_step,
            curr: 0,
            max: steps,
        }
    }

    /// Create a new `TimeStepIter` which will include the start of the provided `TimeInterval`.
    /// # Errors
    /// This method fails if the start or end values of the interval are not valid in chrono.
    pub fn new_with_interval_incl_start(
        time_interval: TimeInterval,
        time_step: TimeStep,
    ) -> Result<Self> {
        let num_steps = if time_interval.start() == time_interval.end() {
            0
        } else {
            time_step.num_steps_in_interval(time_interval)?
        };

        Self::new_incl_start(time_interval.start(), time_step, num_steps)
    }

    /// Create a new `Iterator` which will return `TimeInterval` starting at each `TimeInstance`.
    /// The `Iterator` uses a maximum value `max_t2` if the end of the `TimeInterval` is not valid.
    pub fn into_intervals(
        self,
        step_to_t_2: TimeStep,
        max_t2: TimeInstance,
    ) -> impl Iterator<Item = TimeInterval> {
        self.map(move |t_1| {
            let t_2 = t_1 + step_to_t_2;
            let t_2 = t_2.unwrap_or(max_t2);
            TimeInterval::new_unchecked(t_1, t_2)
        })
    }
}

impl Iterator for TimeStepIter {
    type Item = TimeInstance;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr > self.max {
            return None;
        }

        let next = (self.reference_time
            + TimeStep {
                granularity: self.time_step.granularity,
                step: self.curr * self.time_step.step,
            })
        .unwrap();

        self.curr += 1;

        Some(next)
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;

    use super::*;

    fn test_snap(
        granularity: TimeGranularity,
        t_step: u32,
        t_start: &str,
        t_in: &str,
        t_expect: &str,
    ) {
        let t_ref = TimeInstance::from(
            NaiveDateTime::parse_from_str(t_start, "%Y-%m-%dT%H:%M:%S%.f").unwrap(),
        );
        let t_1 = TimeInstance::from(
            NaiveDateTime::parse_from_str(t_in, "%Y-%m-%dT%H:%M:%S%.f").unwrap(),
        );
        let t_exp = TimeInstance::from(
            NaiveDateTime::parse_from_str(t_expect, "%Y-%m-%dT%H:%M:%S%.f").unwrap(),
        );

        let time_snapper = TimeStep {
            granularity,
            step: t_step,
        };

        assert_eq!(time_snapper.snap_relative(t_ref, t_1).unwrap(), t_exp);
    }

    fn test_num_steps(
        granularity: TimeGranularity,
        t_step: u32,
        t_1: &str,
        t_2: &str,
        steps_expect: u32,
    ) {
        let t_1 =
            TimeInstance::from(NaiveDateTime::parse_from_str(t_1, "%Y-%m-%dT%H:%M:%S%.f").unwrap());
        let t_2 =
            TimeInstance::from(NaiveDateTime::parse_from_str(t_2, "%Y-%m-%dT%H:%M:%S%.f").unwrap());

        let time_snapper = TimeStep {
            granularity,
            step: t_step,
        };

        assert_eq!(
            time_snapper
                .num_steps_in_interval(TimeInterval::new_unchecked(t_1, t_2))
                .unwrap(),
            steps_expect
        );
    }

    fn test_add(granularity: TimeGranularity, t_step: u32, t_1: &str, t_expect: &str) {
        let t_1 =
            TimeInstance::from(NaiveDateTime::parse_from_str(t_1, "%Y-%m-%dT%H:%M:%S%.f").unwrap());
        let t_expect = TimeInstance::from(
            NaiveDateTime::parse_from_str(t_expect, "%Y-%m-%dT%H:%M:%S%.f").unwrap(),
        );

        let time_step = TimeStep {
            granularity,
            step: t_step,
        };

        assert_eq!((t_1 + time_step).unwrap(), t_expect);
    }

    #[test]
    fn test_add_y_0() {
        test_add(
            TimeGranularity::Years,
            0,
            "2000-01-01T00:00:00.000",
            "2000-01-01T00:00:00.000",
        );
    }

    #[test]
    fn test_add_y_1() {
        test_add(
            TimeGranularity::Years,
            1,
            "2000-01-01T00:00:00.0",
            "2001-01-01T00:00:00.0",
        );
    }

    #[test]
    fn test_add_m_0() {
        test_add(
            TimeGranularity::Months,
            0,
            "2000-01-01T00:00:00.0",
            "2000-01-01T00:00:00.0",
        );
    }

    #[test]
    fn test_add_m_1() {
        test_add(
            TimeGranularity::Months,
            1,
            "2000-01-01T00:00:00.0",
            "2000-02-01T00:00:00.0",
        );
    }

    #[test]
    fn test_add_m_11() {
        test_add(
            TimeGranularity::Months,
            11,
            "2000-01-01T00:00:00.0",
            "2000-12-01T00:00:00.0",
        );
    }

    #[test]
    fn test_add_m_12() {
        test_add(
            TimeGranularity::Months,
            12,
            "2000-01-01T00:00:00.0",
            "2001-01-01T00:00:00.0",
        );
    }

    #[test]
    fn test_add_d_0() {
        test_add(
            TimeGranularity::Days,
            0,
            "2000-01-01T00:00:00.0",
            "2000-01-01T00:00:00.0",
        );
    }

    #[test]
    fn test_add_d_1() {
        test_add(
            TimeGranularity::Days,
            1,
            "2000-01-01T00:00:00.0",
            "2000-01-02T00:00:00.0",
        );
    }

    #[test]
    fn test_add_d_31() {
        test_add(
            TimeGranularity::Days,
            31,
            "2000-01-01T00:00:00.0",
            "2000-02-01T00:00:00.0",
        );
    }

    #[test]
    fn test_add_h_0() {
        test_add(
            TimeGranularity::Hours,
            0,
            "2000-01-01T00:00:00.0",
            "2000-01-01T00:00:00.0",
        );
    }

    #[test]
    fn test_add_h_1() {
        test_add(
            TimeGranularity::Hours,
            1,
            "2000-01-01T00:00:00.0",
            "2000-01-01T01:00:00.0",
        );
    }

    #[test]
    fn test_add_h_24() {
        test_add(
            TimeGranularity::Hours,
            24,
            "2000-01-01T00:00:00.0",
            "2000-01-02T00:00:00.0",
        );
    }

    #[test]
    fn test_add_min_0() {
        test_add(
            TimeGranularity::Minutes,
            0,
            "2000-01-01T00:00:00.0",
            "2000-01-01T00:00:00.0",
        );
    }

    #[test]
    fn test_add_min_1() {
        test_add(
            TimeGranularity::Minutes,
            1,
            "2000-01-01T00:00:00.0",
            "2000-01-01T00:01:00.0",
        );
    }

    #[test]
    fn test_add_min_60() {
        test_add(
            TimeGranularity::Minutes,
            60,
            "2000-01-01T00:00:00.0",
            "2000-01-01T01:00:00.0",
        );
    }

    #[test]
    fn test_add_s_0() {
        test_add(
            TimeGranularity::Seconds,
            0,
            "2000-01-01T00:00:00.0",
            "2000-01-01T00:00:00.0",
        );
    }

    #[test]
    fn test_add_s_1() {
        test_add(
            TimeGranularity::Seconds,
            1,
            "2000-01-01T00:00:00.0",
            "2000-01-01T00:00:01.0",
        );
    }

    #[test]
    fn test_add_s_60() {
        test_add(
            TimeGranularity::Seconds,
            60,
            "2000-01-01T00:00:00.0",
            "2000-01-01T00:01:00.0",
        );
    }

    #[test]
    fn test_add_millis_0() {
        test_add(
            TimeGranularity::Millis,
            0,
            "2000-01-01T00:00:00.0",
            "2000-01-01T00:00:00.0",
        );
    }

    #[test]
    fn test_add_millis_1() {
        test_add(
            TimeGranularity::Millis,
            10,
            "2000-01-01T00:00:00.0",
            "2000-01-01T00:00:00.01",
        );
    }

    #[test]
    fn test_add_millis_1000() {
        test_add(
            TimeGranularity::Millis,
            1000,
            "2000-01-01T00:00:00.0",
            "2000-01-01T00:00:01.0",
        );
    }

    #[test]
    fn time_snap_month_n1() {
        test_snap(
            TimeGranularity::Months,
            1,
            "2000-01-01T00:00:00.0",
            "1999-11-01T00:00:00.0",
            "1999-11-01T00:00:00.0",
        );
    }

    #[test]
    fn time_snap_month_n_after_ref() {
        test_snap(
            TimeGranularity::Months,
            1,
            "2000-06-01T00:00:00.0",
            "1998-07-15T00:00:00.0",
            "1998-07-01T00:00:00.0",
        );
    }

    #[test]
    fn time_snap_month_n_before_ref() {
        test_snap(
            TimeGranularity::Months,
            1,
            "2000-06-01T00:00:00.0",
            "1998-05-15T00:00:00.0",
            "1998-05-01T00:00:00.0",
        );
    }

    #[test]
    fn time_snap_month_wrap() {
        test_snap(
            TimeGranularity::Months,
            7,
            "2000-06-01T00:00:00.0",
            "2001-01-01T00:00:00.0",
            "2001-01-01T00:00:00.0",
        );
    }

    #[test]
    fn time_snap_month_1() {
        test_snap(
            TimeGranularity::Months,
            1,
            "2000-01-01T00:00:00.0",
            "2000-11-11T11:11:11.0",
            "2000-11-01T00:00:00.0",
        );
    }

    #[test]
    fn time_snap_month_3() {
        test_snap(
            TimeGranularity::Months,
            3,
            "2000-01-01T00:00:00.0",
            "2000-11-11T11:11:11.0",
            "2000-10-01T00:00:00.0",
        );
    }

    #[test]
    fn time_snap_month_7() {
        test_snap(
            TimeGranularity::Months,
            7,
            "2000-01-01T00:00:00.0",
            "2001-01-01T11:11:11.0",
            "2000-08-01T00:00:00.0",
        );
    }

    #[test]
    fn time_snap_month_wrap2() {
        test_snap(
            TimeGranularity::Months,
            1,
            "2014-01-01T00:00:00.0",
            "2013-12-01T00:00:00.0",
            "2013-12-01T00:00:00.0",
        );
    }

    #[test]
    fn time_snap_year_1() {
        test_snap(
            TimeGranularity::Years,
            1,
            "2010-01-01T00:00:00.0",
            "2014-01-03T01:01:00.0",
            "2014-01-01T00:00:00.0",
        );
    }

    #[test]
    fn time_snap_year_3() {
        test_snap(
            TimeGranularity::Years,
            3,
            "2010-01-01T00:00:00.0",
            "2014-01-03T01:01:00.0",
            "2013-01-01T00:00:00.0",
        );
    }

    #[test]
    fn time_snap_year_3_2() {
        test_snap(
            TimeGranularity::Years,
            3,
            "2010-01-01T00:02:00.0",
            "2014-01-03T01:01:00.0",
            "2013-01-01T00:02:00.0",
        );
    }

    #[test]
    fn time_snap_day_1() {
        test_snap(
            TimeGranularity::Days,
            1,
            "2010-01-01T00:00:00.0",
            "2013-01-01T01:00:00.0",
            "2013-01-01T00:00:00.0",
        );
    }

    #[test]
    fn time_snap_day_1_2() {
        test_snap(
            TimeGranularity::Days,
            1,
            "2010-01-01T00:02:03.0",
            "2013-01-01T00:00:00.0",
            "2012-12-31T00:02:03.0",
        );
    }

    #[test]
    fn time_snap_day_16() {
        test_snap(
            TimeGranularity::Days,
            16,
            "2018-01-01T00:00:00.0",
            "2018-02-16T01:00:00.0",
            "2018-02-02T00:00:00.0",
        );
    }

    #[test]
    fn time_snap_hour_1() {
        test_snap(
            TimeGranularity::Hours,
            1,
            "2010-01-01T00:00:00.0",
            "2013-01-01T01:12:00.0",
            "2013-01-01T01:00:00.0",
        );
    }

    #[test]
    fn time_snap_hour_13() {
        test_snap(
            TimeGranularity::Hours,
            13,
            "2010-01-01T00:00:00.0",
            "2010-01-02T04:00:00.0",
            "2010-01-02T02:00:00.0",
        );
    }

    #[test]
    fn time_snap_hour_13_2() {
        test_snap(
            TimeGranularity::Hours,
            13,
            "2010-01-01T00:00:01.0",
            "2010-01-02T01:00:02.0",
            "2010-01-01T13:00:01.0",
        );
    }

    #[test]
    fn time_snap_minute_1() {
        test_snap(
            TimeGranularity::Minutes,
            1,
            "2010-01-01T00:00:00.0",
            "2013-01-01T01:12:00.0",
            "2013-01-01T01:12:00.0",
        );
    }

    #[test]
    fn time_snap_minute_1_2() {
        test_snap(
            TimeGranularity::Minutes,
            1,
            "2010-01-01T00:00:03.0",
            "2013-01-01T01:12:05.0",
            "2013-01-01T01:12:03.0",
        );
    }

    #[test]
    fn time_snap_minute_15() {
        test_snap(
            TimeGranularity::Minutes,
            15,
            "2010-01-01T00:00:00.0",
            "2013-01-01T01:16:00.0",
            "2013-01-01T01:15:00.0",
        );
    }

    #[test]
    fn time_snap_minute_31() {
        test_snap(
            TimeGranularity::Minutes,
            31,
            "2010-01-01T00:00:00.0",
            "2010-01-01T01:01:00.0",
            "2010-01-01T00:31:00.0",
        );
    }

    #[test]
    fn time_snap_second_1() {
        test_snap(
            TimeGranularity::Seconds,
            1,
            "2010-01-01T00:00:00.0",
            "2010-01-01T01:01:12.0",
            "2010-01-01T01:01:12.0",
        );
    }

    #[test]
    fn time_snap_second_15() {
        test_snap(
            TimeGranularity::Seconds,
            1,
            "2010-01-01T00:00:00.0",
            "2010-01-01T01:01:12.0",
            "2010-01-01T01:01:12.0",
        );
    }

    #[test]
    fn time_snap_second_31() {
        test_snap(
            TimeGranularity::Seconds,
            31,
            "2010-01-01T23:59:00.0",
            "2010-01-02T00:00:02.0",
            "2010-01-02T00:00:02.0",
        );
    }

    #[test]
    fn time_snap_second_31_2() {
        test_snap(
            TimeGranularity::Seconds,
            31,
            "2010-01-01T23:59:00.0",
            "2010-01-02T00:00:01.0",
            "2010-01-01T23:59:31.0",
        );
    }

    #[test]
    fn time_snap_millis_1() {
        test_snap(
            TimeGranularity::Millis,
            1,
            "2010-01-01T01:01:01.0000",
            "2010-01-01T01:01:01.0001",
            "2010-01-01T01:01:01.0000",
        );
    }

    #[test]
    fn time_snap_millis_2() {
        test_snap(
            TimeGranularity::Millis,
            2,
            "2010-01-01T01:01:01.0000",
            "2010-01-01T01:01:01.0002",
            "2010-01-01T01:01:01.0003",
        );
    }

    #[test]
    fn time_snap_millis_500() {
        test_snap(
            TimeGranularity::Millis,
            500,
            "2010-01-01T01:01:01.0000",
            "2010-01-01T01:01:02.7",
            "2010-01-01T01:01:02.5",
        );
    }

    #[test]
    fn time_snap_0() {
        let time_snapper = TimeStep {
            granularity: TimeGranularity::Months,
            step: 1,
        };

        // snap with reference 2014-01-01T00:00:00 and time_to_snap 1970-01-01T00:00:00
        assert_eq!(
            time_snapper.snap_relative(1_388_534_400_000, 0).unwrap(),
            TimeInstance::from_millis_unchecked(0)
        );
    }

    #[test]
    fn num_steps_y_1_0() {
        test_num_steps(
            TimeGranularity::Years,
            1,
            "2001-01-01T01:01:01.0",
            "2001-01-01T01:01:01.0",
            0,
        );
    }

    #[test]
    fn num_steps_y_1_1() {
        test_num_steps(
            TimeGranularity::Years,
            1,
            "2001-01-01T01:01:01.0",
            "2002-01-01T01:01:01.0",
            0,
        );
    }

    #[test]
    fn num_steps_y_1_2() {
        test_num_steps(
            TimeGranularity::Years,
            1,
            "2001-01-01T01:01:01.0",
            "2002-01-01T01:01:02.0",
            1,
        );
    }

    #[test]
    fn num_steps_y_1_3() {
        test_num_steps(
            TimeGranularity::Years,
            1,
            "2001-01-01T01:01:01.0",
            "2003-01-01T01:01:02.0",
            2,
        );
    }

    #[test]
    fn num_steps_y_6() {
        test_num_steps(
            TimeGranularity::Years,
            2,
            "2001-01-01T01:01:01.0",
            "2013-02-02T02:02:02.0",
            6,
        );
    }

    #[test]
    fn num_steps_m_0() {
        test_num_steps(
            TimeGranularity::Months,
            2,
            "2001-01-01T01:01:01.0",
            "2001-02-01T01:01:01.0",
            0,
        );
    }

    #[test]
    fn num_steps_m_1() {
        test_num_steps(
            TimeGranularity::Months,
            1,
            "2001-01-01T01:01:01.0",
            "2001-02-02T02:02:02.0",
            1,
        );
    }

    #[test]
    fn num_steps_m_43() {
        test_num_steps(
            TimeGranularity::Months,
            3,
            "2001-01-01T01:01:01.0",
            "2011-10-02T02:02:02.0",
            43,
        );
    }

    #[test]
    fn num_steps_d_1() {
        test_num_steps(
            TimeGranularity::Days,
            1,
            "2001-01-01T01:01:01.0",
            "2001-01-02T02:02:02.0",
            1,
        );
    }

    #[test]
    fn num_steps_d_366() {
        test_num_steps(
            TimeGranularity::Days,
            2,
            "2001-01-01T01:01:01.0",
            "2003-01-03T02:02:02.0",
            366,
        );
    }

    #[test]
    fn num_steps_h_0() {
        test_num_steps(
            TimeGranularity::Hours,
            1,
            "2001-01-01T01:01:01.0",
            "2001-01-01T01:01:01.0",
            0,
        );
    }

    #[test]
    fn num_steps_h_1() {
        test_num_steps(
            TimeGranularity::Hours,
            1,
            "2001-01-01T01:01:01.0",
            "2001-01-01T02:02:02.0",
            1,
        );
    }

    #[test]
    fn num_steps_h_11() {
        test_num_steps(
            TimeGranularity::Hours,
            6,
            "2001-01-01T01:01:01.0",
            "2001-01-03T19:01:02.0",
            11,
        );
    }

    #[test]
    fn num_steps_min_1() {
        test_num_steps(
            TimeGranularity::Minutes,
            1,
            "2001-01-01T01:01:01.0",
            "2001-01-01T01:02:02.0",
            1,
        );
    }

    #[test]
    fn num_steps_min_7() {
        test_num_steps(
            TimeGranularity::Minutes,
            10,
            "2001-01-01T01:01:01.0",
            "2001-01-01T02:11:02.0",
            7,
        );
    }

    #[test]
    fn num_steps_sec_0() {
        test_num_steps(
            TimeGranularity::Seconds,
            1,
            "2001-01-01T01:01:01.0",
            "2001-01-01T01:01:01.0",
            0,
        );
    }

    #[test]
    fn num_steps_sec_0_1() {
        test_num_steps(
            TimeGranularity::Seconds,
            1,
            "2001-01-01T01:01:01.0",
            "2001-01-01T01:01:02.0",
            0,
        );
    }

    #[test]
    fn num_steps_sec_1() {
        test_num_steps(
            TimeGranularity::Seconds,
            1,
            "2001-01-01T01:01:01.0",
            "2001-01-01T01:01:03.0",
            1,
        );
    }

    #[test]
    fn num_steps_sec_7() {
        test_num_steps(
            TimeGranularity::Seconds,
            10,
            "2001-01-01T01:01:01.0",
            "2001-01-01T01:02:12.0",
            7,
        );
    }

    #[test]
    fn test_iter_h_0() {
        let t_1 = TimeInstance::from(NaiveDate::from_ymd(2001, 1, 1).and_hms(0, 1, 1));
        let t_2 = TimeInstance::from(NaiveDate::from_ymd(2001, 1, 1).and_hms(0, 1, 1));

        let t_step = TimeStep {
            granularity: TimeGranularity::Hours,
            step: 1,
        };

        let iter = TimeStepIter::new_with_interval_incl_start(
            TimeInterval::new_unchecked(t_1, t_2),
            t_step,
        )
        .unwrap();

        let t_vec: Vec<TimeInstance> = iter.collect();

        assert_eq!(&t_vec, &[t_1]);
    }

    #[test]
    fn test_iter_h_3() {
        let t_1 = TimeInstance::from(NaiveDate::from_ymd(2001, 1, 1).and_hms(0, 1, 1));
        let t_2 = TimeInstance::from(NaiveDate::from_ymd(2001, 1, 1).and_hms(3, 1, 1));

        let t_step = TimeStep {
            granularity: TimeGranularity::Hours,
            step: 1,
        };

        let iter = TimeStepIter::new_with_interval_incl_start(
            TimeInterval::new_unchecked(t_1, t_2),
            t_step,
        )
        .unwrap();

        let t_vec: Vec<TimeInstance> = iter.collect();

        assert_eq!(
            &t_vec,
            &[
                t_1,
                TimeInstance::from(NaiveDate::from_ymd(2001, 1, 1).and_hms(1, 1, 1)),
                TimeInstance::from(NaiveDate::from_ymd(2001, 1, 1).and_hms(2, 1, 1)),
            ]
        );
    }

    #[test]
    fn snap_neg_millis() {
        test_snap(
            TimeGranularity::Millis,
            3,
            "2000-01-01T00:00:00.0",
            "1999-12-31T23:59:59.999",
            "1999-12-31T23:59:59.997",
        );
    }

    #[test]
    fn snap_neg_secs() {
        test_snap(
            TimeGranularity::Seconds,
            3,
            "2000-01-01T00:00:00.0",
            "1999-12-31T00:00:59.0",
            "1999-12-31T00:00:57",
        );
    }

    #[test]
    fn snap_neg_mins() {
        test_snap(
            TimeGranularity::Minutes,
            3,
            "2000-01-01T00:00:00.0",
            "1999-12-31T00:59:00.0",
            "1999-12-31T00:57:00.0",
        );
    }

    #[test]
    fn snap_neg_hours() {
        test_snap(
            TimeGranularity::Hours,
            3,
            "2000-01-01T00:00:00.0",
            "1999-12-31T23:00:00.0",
            "1999-12-31T21:00:0",
        );
    }

    #[test]
    fn snap_neg_days() {
        test_snap(
            TimeGranularity::Days,
            3,
            "2000-01-01T00:00:00.0",
            "1999-12-31T00:00:00.0",
            "1999-12-29T00:00:0",
        );
    }

    #[test]
    fn snap_neg_months() {
        test_snap(
            TimeGranularity::Months,
            3,
            "2000-01-01T00:00:00.0",
            "1999-12-31T00:00:00.0",
            "1999-10-01T00:00:0",
        );
    }

    #[test]
    fn snap_neg_years() {
        test_snap(
            TimeGranularity::Years,
            3,
            "2000-01-01T00:00:00.0",
            "1999-01-01T00:00:00.0",
            "1997-01-01T00:00:00.0",
        );
    }

    #[test]
    fn snap_m_12m() {
        test_snap(
            TimeGranularity::Months,
            1,
            "2014-01-01T00:00:00.0",
            "2014-12-01T12:00:00.0",
            "2014-12-01T00:00:00.0",
        );
    }

    #[test]
    fn snap_m_13m() {
        test_snap(
            TimeGranularity::Months,
            1,
            "2014-01-01T00:00:00.0",
            "2015-01-01T12:00:00.0",
            "2015-01-01T00:00:00.0",
        );
    }

    #[test]
    fn snap_m_24m() {
        test_snap(
            TimeGranularity::Months,
            1,
            "2014-01-01T00:00:00.0",
            "2015-12-01T12:00:00.0",
            "2015-12-01T00:00:00.0",
        );
    }

    #[test]
    fn snap_m_12m_neg() {
        test_snap(
            TimeGranularity::Months,
            1,
            "2014-01-01T00:00:00.0",
            "2013-01-01T12:00:00.0",
            "2013-01-01T00:00:00.0",
        );
    }
}
