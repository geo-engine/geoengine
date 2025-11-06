use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};

use crate::{
    delegate_from_to_sql,
    error::Error,
    primitives::{TimeInstance, TimeInterval, TimeStep, TimeStepIter},
    util::Result,
};

#[derive(Debug, Copy, Clone, Serialize, Deserialize, ToSql, FromSql, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RegularTimeDimension {
    pub origin: TimeInstance,
    pub step: TimeStep,
}

impl RegularTimeDimension {
    pub fn new_with_epoch_origin(step: TimeStep) -> Self {
        Self {
            origin: TimeInstance::EPOCH_START,
            step,
        }
    }

    pub fn new(origin: TimeInstance, step: TimeStep) -> Self {
        Self { origin, step }
    }

    /// Checks whether two `RegularTimeDimension`s are compatible, i.e. have the same step and
    /// their origins align on the same time steps.
    pub fn compatible_with(&self, other: RegularTimeDimension) -> bool {
        let step_identical = self.step == other.step;
        // TODO: handle special cases like 12 months and 1 year, or 60 seconds and 1 minute
        let Ok(snapped_origin_other) = self.snap_prev(other.origin) else {
            return false;
        };
        let origin_compatible = snapped_origin_other == other.origin;
        step_identical && origin_compatible
    }

    pub fn merge(&self, other: RegularTimeDimension) -> Option<RegularTimeDimension> {
        if self.compatible_with(other) {
            let origin = if self.origin < other.origin {
                self.origin
            } else {
                other.origin
            };
            // TODO: handle special cases like 12 months and 1 year, or 60 seconds and 1 minute where we must decide which step to take
            Some(RegularTimeDimension {
                origin,
                step: self.step,
            })
        } else {
            None
        }
    }

    pub fn valid_step(&self, time: TimeInstance) -> bool {
        self.snap_prev(time)
            .map(|snapped| snapped == time)
            .unwrap_or(false)
    }

    pub fn valid_interval(&self, time: TimeInterval) -> bool {
        self.valid_step(time.start()) && self.valid_step(time.end())
    }

    /// Snaps the given `time_instance` to the previous (smaller or equal) time instance
    pub fn snap_prev(&self, time_instance: TimeInstance) -> Result<TimeInstance> {
        self.step.snap_relative(self.origin, time_instance)
    }

    /// Snaps the given `time_instance` to the next (larger or equal) time instance
    pub fn snap_next(&self, time_instance: TimeInstance) -> Result<TimeInstance> {
        let snapped = self.snap_prev(time_instance)?;
        if snapped < time_instance {
            snapped + self.step
        } else {
            Ok(snapped)
        }
    }

    /// Returns the largest `TimeInterval` that is fully contained in the given `time_interval`.
    /// If no time steps are contained, `Ok(None)` is returned.
    pub fn contained_interval(&self, time_interval: TimeInterval) -> Result<Option<TimeInterval>> {
        let intersected = self.intersected_interval(time_interval)?;

        let start = if time_interval.start() == intersected.start() {
            intersected.start()
        } else {
            (intersected.start() + self.step)?
        };

        let end = if time_interval.end() == intersected.end() {
            intersected.end()
        } else {
            (intersected.end() - self.step)?
        };

        if end <= start {
            Ok(None)
        } else {
            TimeInterval::new(start, end).map(Some)
        }
    }

    /// Returns the smallest `TimeInterval` that contains the given `time_interval`
    pub fn intersected_interval(&self, time_interval: TimeInterval) -> Result<TimeInterval> {
        let start = self.snap_prev(time_interval.start())?;
        let end = self.snap_next(time_interval.end())?;
        TimeInterval::new(start, end)
    }

    /// Returns the number of time steps that are fully contained in the given `time_interval`
    /// If no time steps are contained, `0` is returned.
    pub fn steps_contained_in(&self, time_interval: TimeInterval) -> Result<u32> {
        let Some(contained) = self.contained_interval(time_interval)? else {
            return Ok(0);
        };
        self.step.num_steps_in_interval(contained)
    }

    /// Returns the number of time steps that intersect with the given `time_interval`
    pub fn steps_intersecting(&self, time_interval: TimeInterval) -> Result<u32> {
        let intersected = self.intersected_interval(time_interval)?;
        self.step.num_steps_in_interval(intersected)
    }

    /// Returns an iterator over all time steps that are fully contained in the given `time_interval`
    /// If no time steps are contained, `Ok(None)` is returned.
    ///
    /// # Panics
    /// IF `time_interval` is not valid
    pub fn contained_intervals(
        &self,
        time_interval: TimeInterval,
    ) -> Result<Option<impl Iterator<Item = TimeInterval>>> {
        let Some(contained) = self.contained_interval(time_interval)? else {
            return Ok(None);
        };
        let iter = TimeStepIter::new_with_interval(contained, self.step)?;
        let intervals = iter.map(|start| {
            TimeInterval::new_unchecked(
                start,
                (start + self.step).expect("is included in valid interval"),
            )
        });
        Ok(Some(intervals))
    }

    /// Returns an iterator over all time steps that intersect with the given `time_interval`
    ///
    /// # Panics
    /// IF `time_interval` is not valid
    pub fn intersecting_intervals(
        &self,
        time_interval: TimeInterval,
    ) -> Result<impl Iterator<Item = TimeInterval> + use<>> {
        let intersected = self.intersected_interval(time_interval)?;
        let iter = TimeStepIter::new_with_interval(intersected, self.step)?;
        let step = self.step;
        let intervals = iter.map(move |start| {
            TimeInterval::new_unchecked(
                start,
                (start + step).expect("is included in valid interval"),
            )
        });
        Ok(intervals)
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum TimeDimension {
    Regular(RegularTimeDimension),
    Irregular,
}

impl TimeDimension {
    pub fn is_regular(&self) -> bool {
        matches!(self, TimeDimension::Regular(_))
    }

    pub fn origin(&self) -> Option<TimeInstance> {
        match self {
            TimeDimension::Regular(r) => Some(r.origin),
            TimeDimension::Irregular => None,
        }
    }

    pub fn new_irregular() -> Self {
        TimeDimension::Irregular
    }

    pub fn new_regular(origin: TimeInstance, step: TimeStep) -> Self {
        TimeDimension::Regular(RegularTimeDimension { origin, step })
    }

    pub fn new_regular_with_epoch(step: TimeStep) -> Self {
        TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(step))
    }

    pub fn unwrap_regular(self) -> Option<RegularTimeDimension> {
        match self {
            TimeDimension::Regular(r) => Some(r),
            TimeDimension::Irregular => None,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "TimeDimensionDiscriminator")]
pub enum TimeDimensionDiscriminatorDbType {
    Regular,
    Irregular,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "TimeDimension")]
pub struct TimeDimensionDbType {
    pub regular_dimension: Option<RegularTimeDimension>,
    pub discriminant: TimeDimensionDiscriminatorDbType,
}

impl From<&TimeDimension> for TimeDimensionDbType {
    fn from(value: &TimeDimension) -> Self {
        match value {
            TimeDimension::Regular(r) => Self {
                regular_dimension: Some(*r),
                discriminant: TimeDimensionDiscriminatorDbType::Regular,
            },
            TimeDimension::Irregular => Self {
                regular_dimension: None,
                discriminant: TimeDimensionDiscriminatorDbType::Irregular,
            },
        }
    }
}

impl TryFrom<TimeDimensionDbType> for TimeDimension {
    type Error = Error;

    fn try_from(value: TimeDimensionDbType) -> Result<Self> {
        match value.discriminant {
            TimeDimensionDiscriminatorDbType::Regular => Ok(TimeDimension::Regular(
                value
                    .regular_dimension
                    .ok_or(Error::UnexpectedInvalidDbTypeConversion)?,
            )),
            TimeDimensionDiscriminatorDbType::Irregular => Ok(TimeDimension::Irregular),
        }
    }
}

delegate_from_to_sql!(TimeDimension, TimeDimensionDbType);

#[cfg(test)]
mod tests {

    use crate::primitives::DateTime;

    use super::*;

    #[test]
    fn test_compatible() {
        let dim1 = RegularTimeDimension::new_with_epoch_origin(TimeStep::years(1).unwrap());
        let dim2 = RegularTimeDimension::new_with_epoch_origin(TimeStep::years(1).unwrap());
        assert!(dim1.compatible_with(dim2));
        assert!(dim2.compatible_with(dim1));
        let dim3 = RegularTimeDimension::new(
            DateTime::new_utc(2020, 1, 1, 0, 0, 0).into(),
            TimeStep::years(1).unwrap(),
        );
        assert!(dim1.compatible_with(dim3));
        assert!(dim3.compatible_with(dim1));
        let dim4 = RegularTimeDimension::new(
            DateTime::new_utc(2021, 2, 1, 0, 0, 0).into(),
            TimeStep::years(1).unwrap(),
        );
        assert!(!dim1.compatible_with(dim4));
        assert!(!dim4.compatible_with(dim1));

        let dim5 = RegularTimeDimension::new(
            DateTime::new_utc(2021, 2, 1, 0, 0, 0).into(),
            TimeStep::years(7).unwrap(),
        );
        assert!(!dim1.compatible_with(dim5));
        assert!(!dim5.compatible_with(dim1));
    }

    #[test]
    fn test_merge() {
        let dim1 = RegularTimeDimension::new_with_epoch_origin(TimeStep::years(1).unwrap());
        let dim2 = RegularTimeDimension::new_with_epoch_origin(TimeStep::years(1).unwrap());
        assert_eq!(dim1.merge(dim2).unwrap(), dim1);
        let dim3 = RegularTimeDimension::new(
            DateTime::new_utc(2020, 1, 1, 0, 0, 0).into(),
            TimeStep::years(1).unwrap(),
        );
        assert_eq!(dim1.merge(dim3).unwrap(), dim1);
        let dim4 = RegularTimeDimension::new(
            DateTime::new_utc(2021, 1, 1, 0, 0, 0).into(),
            TimeStep::years(2).unwrap(),
        );
        assert!(dim1.merge(dim4).is_none());
    }

    #[test]
    fn test_snap() {
        let dim = RegularTimeDimension::new_with_epoch_origin(TimeStep::years(1).unwrap());
        let time = DateTime::new_utc(2023, 6, 15, 12, 0, 0).into();
        let snapped_prev = dim.snap_prev(time).unwrap();
        assert_eq!(snapped_prev, DateTime::new_utc(2023, 1, 1, 0, 0, 0).into());
        let snapped_next = dim.snap_next(time).unwrap();
        assert_eq!(snapped_next, DateTime::new_utc(2024, 1, 1, 0, 0, 0).into());
    }

    #[test]
    fn test_contained_interval() {
        let dim = RegularTimeDimension::new_with_epoch_origin(TimeStep::years(1).unwrap());
        let interval = TimeInterval::new(
            DateTime::new_utc(2020, 6, 15, 0, 0, 0),
            DateTime::new_utc(2023, 6, 15, 0, 0, 0),
        )
        .unwrap();
        let contained = dim.contained_interval(interval).unwrap().unwrap();
        assert_eq!(
            contained,
            TimeInterval::new(
                DateTime::new_utc(2021, 1, 1, 0, 0, 0),
                DateTime::new_utc(2023, 1, 1, 0, 0, 0)
            )
            .unwrap()
        );
    }

    #[test]
    fn test_intersected_interval() {
        let dim = RegularTimeDimension::new_with_epoch_origin(TimeStep::years(1).unwrap());
        let interval = TimeInterval::new(
            DateTime::new_utc(2020, 6, 15, 0, 0, 0),
            DateTime::new_utc(2023, 6, 15, 0, 0, 0),
        )
        .unwrap();
        let intersected = dim.intersected_interval(interval).unwrap();
        assert_eq!(
            intersected,
            TimeInterval::new(
                DateTime::new_utc(2020, 1, 1, 0, 0, 0),
                DateTime::new_utc(2024, 1, 1, 0, 0, 0)
            )
            .unwrap()
        );
    }

    #[test]
    fn test_steps_contained_in() {
        let dim = RegularTimeDimension::new_with_epoch_origin(TimeStep::years(1).unwrap());
        let interval = TimeInterval::new(
            DateTime::new_utc(2020, 6, 15, 0, 0, 0),
            DateTime::new_utc(2023, 6, 15, 0, 0, 0),
        )
        .unwrap();
        let steps = dim.steps_contained_in(interval).unwrap();
        assert_eq!(steps, 2);
    }

    #[test]
    fn test_steps_intersecting() {
        let dim = RegularTimeDimension::new_with_epoch_origin(TimeStep::years(1).unwrap());
        let interval = TimeInterval::new(
            DateTime::new_utc(2020, 6, 15, 0, 0, 0),
            DateTime::new_utc(2023, 6, 15, 0, 0, 0),
        )
        .unwrap();
        let steps = dim.steps_intersecting(interval).unwrap();
        assert_eq!(steps, 4);
    }

    #[test]
    fn test_contained_intervals() {
        let dim = RegularTimeDimension::new_with_epoch_origin(TimeStep::years(1).unwrap());
        let interval = TimeInterval::new(
            DateTime::new_utc(2020, 6, 15, 0, 0, 0),
            DateTime::new_utc(2023, 6, 15, 0, 0, 0),
        )
        .unwrap();
        let contained = dim.contained_intervals(interval).unwrap().unwrap();
        let contained: Vec<TimeInterval> = contained.collect();
        assert_eq!(
            contained,
            vec![
                TimeInterval::new(
                    DateTime::new_utc(2021, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2022, 1, 1, 0, 0, 0)
                )
                .unwrap(),
                TimeInterval::new(
                    DateTime::new_utc(2022, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2023, 1, 1, 0, 0, 0)
                )
                .unwrap(),
            ]
        );
    }

    #[test]
    fn test_intersecting_intervals() {
        let dim = RegularTimeDimension::new_with_epoch_origin(TimeStep::years(1).unwrap());
        let interval = TimeInterval::new(
            DateTime::new_utc(2020, 6, 15, 0, 0, 0),
            DateTime::new_utc(2023, 6, 15, 0, 0, 0),
        )
        .unwrap();
        let intersecting = dim.intersecting_intervals(interval).unwrap();
        let intersecting: Vec<TimeInterval> = intersecting.collect();
        assert_eq!(
            intersecting,
            vec![
                TimeInterval::new(
                    DateTime::new_utc(2020, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2021, 1, 1, 0, 0, 0)
                )
                .unwrap(),
                TimeInterval::new(
                    DateTime::new_utc(2021, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2022, 1, 1, 0, 0, 0)
                )
                .unwrap(),
                TimeInterval::new(
                    DateTime::new_utc(2022, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2023, 1, 1, 0, 0, 0)
                )
                .unwrap(),
                TimeInterval::new(
                    DateTime::new_utc(2023, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2024, 1, 1, 0, 0, 0)
                )
                .unwrap(),
            ]
        );
    }
}
