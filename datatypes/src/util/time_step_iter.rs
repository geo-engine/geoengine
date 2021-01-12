use crate::primitives::{TimeInstance, TimeInterval, TimeStep};
use crate::util::Result;

/// An `Iterator` to iterate over time in steps
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
    /// The `Iterator` produces an `Error` if the end of the `TimeInterval` is not valid.
    pub fn try_as_intervals(
        self,
        step_to_t_2: TimeStep,
    ) -> impl Iterator<Item = Result<TimeInterval>> {
        self.map(move |t_1| {
            let t_2 = t_1 + step_to_t_2;
            t_2.map(|t_2| TimeInterval::new_unchecked(t_1, t_2))
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
    use chrono::NaiveDate;

    use crate::primitives::TimeGranularity;

    use super::*;

    #[test]
    fn test_h_0() {
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

        assert_eq!(&t_vec, &[t_1])
    }

    #[test]
    fn test_h_3() {
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
        )
    }
}
