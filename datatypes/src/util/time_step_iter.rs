use crate::primitives::{TimeInstance, TimeInterval, TimeStep};
use crate::util::Result;

pub struct TimeStepIter {
    reference_time: TimeInstance,
    time_step: TimeStep,
    curr: u32,
    max: u32,
}

impl TimeStepIter {
    pub fn new(reference_time: TimeInstance, time_step: TimeStep, steps: u32) -> Self {
        // TODO: check if max is in range. Else Error

        Self {
            reference_time,
            time_step,
            curr: 0,
            max: steps,
        }
    }

    pub fn new_with_time_interval(
        time_interval: TimeInterval,
        time_step: TimeStep,
    ) -> Result<Self> {
        let num_steps = time_step.num_steps_in_interval(time_interval)?;

        Ok(Self::new(time_interval.start(), time_step, num_steps))
    }
}

impl Iterator for TimeStepIter {
    type Item = TimeInstance;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr >= self.max {
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
    fn test_name() {
        let t_1 = TimeInstance::from(NaiveDate::from_ymd(2001, 1, 1).and_hms(0, 1, 1));
        let t_2 = TimeInstance::from(NaiveDate::from_ymd(2001, 1, 1).and_hms(3, 2, 2));

        let t_step = TimeStep {
            granularity: TimeGranularity::Hours,
            step: 1,
        };

        let iter =
            TimeStepIter::new_with_time_interval(TimeInterval::new_unchecked(t_1, t_2), t_step)
                .unwrap();

        let t_vec: Vec<TimeInstance> = iter.collect();

        assert_eq!(
            &t_vec,
            &[
                t_1,
                TimeInstance::from(NaiveDate::from_ymd(2001, 1, 1).and_hms(1, 1, 1)),
                TimeInstance::from(NaiveDate::from_ymd(2001, 1, 1).and_hms(2, 1, 1)),
                // t_2
            ]
        )
    }
}
