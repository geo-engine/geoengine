use crate::error;
use crate::primitives::TimeInstance;
use crate::util::arrow::ArrowTyped;
use crate::util::Result;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};

/// Stores time intervals in ms in close-open semantic [start, end)
#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[repr(C)]
pub struct TimeInterval {
    start: TimeInstance,
    end: TimeInstance,
}

impl Default for TimeInterval {
    /// The default time interval is always valid.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{TimeInterval, TimeInstance};
    ///
    /// assert!(TimeInterval::default().contains(&TimeInterval::new_unchecked(0, 0)));
    /// assert!(TimeInterval::default().intersects(&TimeInterval::default()));
    /// assert_eq!(TimeInterval::default().union(&TimeInterval::default()).unwrap(), TimeInterval::default());
    /// ```
    fn default() -> Self {
        Self {
            start: i64::min_value().into(),
            end: i64::max_value().into(),
        }
    }
}

impl TimeInterval {
    /// Creates a new time interval from inputs implementing Into<TimeInstance>
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{TimeInterval, TimeInstance};
    ///
    /// TimeInterval::new(0, 0).unwrap();
    /// TimeInterval::new(0, 1).unwrap();
    ///
    /// TimeInterval::new(1, 0).unwrap_err();
    /// ```
    ///
    /// # Errors
    ///
    /// This constructor fails if `end` is before `start`
    ///
    pub fn new<A, B>(start: A, end: B) -> Result<Self>
    where
        A: Into<TimeInstance>,
        B: Into<TimeInstance>,
    {
        let start_instant = start.into();
        let end_instant = end.into();
        ensure!(
            start_instant <= end_instant,
            error::TimeIntervalEndBeforeStart {
                start: start_instant,
                end: end_instant
            }
        );
        Ok(Self {
            start: start_instant,
            end: end_instant,
        })
    }

    /// Creates a new time interval without bound checks from inputs implementing Into<TimeInstance>
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{TimeInterval};
    ///
    /// let time_unchecked = TimeInterval::new_unchecked(0, 1);
    ///
    /// assert_eq!(time_unchecked, TimeInterval::new(0, 1).unwrap());
    /// ```
    ///
    pub fn new_unchecked<A, B>(start: A, end: B) -> Self
    where
        A: Into<TimeInstance>,
        B: Into<TimeInstance>,
    {
        Self {
            start: start.into(),
            end: end.into(),
        }
    }

    /// Returns whether the other `TimeInterval` is contained (smaller or equal) within this interval
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{TimeInterval, TimeInstance};
    ///
    /// let valid_pairs = vec![
    ///     ((0, 1), (0, 1)),
    ///     ((0, 3), (1, 2)),
    ///     ((0, 2), (0, 1)),
    ///     ((0, 2), (1, 2)),
    /// ];
    ///
    /// for ((t1, t2), (t3, t4)) in valid_pairs {
    ///     let i1 = TimeInterval::new(t1, t2).unwrap();
    ///     let i2 = TimeInterval::new(t3, t4).unwrap();
    ///     assert!(i1.contains(&i2), "{:?} should contain {:?}", i1, i2);
    /// }
    ///
    /// let invalid_pairs = vec![((0, 1), (-1, 2))];
    ///
    /// for ((t1, t2), (t3, t4)) in invalid_pairs {
    ///     let i1 = TimeInterval::new(t1, t2).unwrap();
    ///     let i2 = TimeInterval::new(t3, t4).unwrap();
    ///     assert!(!i1.contains(&i2), "{:?} should not contain {:?}", i1, i2);
    /// }
    /// ```
    ///
    pub fn contains(&self, other: &Self) -> bool {
        self.start <= other.start && self.end >= other.end
    }

    /// Returns whether the given interval intersects this interval
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{TimeInterval, TimeInstance};
    ///
    /// let valid_pairs = vec![
    ///     ((0, 1), (0, 1)),
    ///     ((0, 3), (1, 2)),
    ///     ((0, 2), (1, 3)),
    ///     ((0, 1), (0, 2)),
    ///     ((0, 2), (-2, 1)),
    /// ];
    ///
    /// for ((t1, t2), (t3, t4)) in valid_pairs {
    ///     let i1 = TimeInterval::new(t1, t2).unwrap();
    ///     let i2 = TimeInterval::new(t3, t4).unwrap();
    ///     assert!(i1.intersects(&i2), "{:?} should intersect {:?}", i1, i2);
    /// }
    ///
    /// let invalid_pairs = vec![
    ///     ((0, 1), (-1, 0)), //
    ///     ((0, 1), (1, 2)),
    ///     ((0, 1), (2, 3)),
    /// ];
    ///
    /// for ((t1, t2), (t3, t4)) in invalid_pairs {
    ///     let i1 = TimeInterval::new(t1, t2).unwrap();
    ///     let i2 = TimeInterval::new(t3, t4).unwrap();
    ///     assert!(
    ///         !i1.intersects(&i2),
    ///         "{:?} should not intersect {:?}",
    ///         i1,
    ///         i2
    ///     );
    /// }
    /// ```
    ///
    pub fn intersects(&self, other: &Self) -> bool {
        self.start < other.end && self.end > other.start
    }

    /// Unites this interval with another one.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{TimeInterval, TimeInstance};
    ///
    /// let i1 = TimeInterval::new(0, 2).unwrap();
    /// let i2 = TimeInterval::new(1, 3).unwrap();
    /// let i3 = TimeInterval::new(2, 4).unwrap();
    /// let i4 = TimeInterval::new(3, 5).unwrap();
    ///
    /// assert_eq!(i1.union(&i2).unwrap(), TimeInterval::new(0, 3).unwrap());
    /// assert_eq!(i1.union(&i3).unwrap(), TimeInterval::new(0, 4).unwrap());
    /// i1.union(&i4).unwrap_err();
    /// ```
    ///
    /// # Errors
    /// This method fails if the other `TimeInterval` does not intersect or touch the current interval.
    ///
    pub fn union(&self, other: &Self) -> Result<Self> {
        ensure!(
            self.intersects(other) || self.start == other.end || self.end == other.start,
            error::TimeIntervalUnmatchedIntervals {
                i1: *self,
                i2: *other,
            }
        );
        Ok(Self {
            start: i64::min(self.start.inner(), other.start.inner()).into(),
            end: i64::max(self.end.inner(), other.end.inner()).into(),
        })
    }

    pub fn start(&self) -> TimeInstance {
        self.start
    }

    pub fn end(&self) -> TimeInstance {
        self.end
    }

    /// Creates a geo json event from a time interval
    ///
    /// according to `GeoJSON` event extension (<https://github.com/sgillies/geojson-events>)
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{TimeInterval, TimeInstance};
    ///
    /// assert_eq!(
    ///     TimeInterval::new_unchecked(0, 1585069448 * 1000).to_geo_json_event(),
    ///     serde_json::json!({
    ///         "start": "1970-01-01T00:00:00+00:00",
    ///         "end": "2020-03-24T17:04:08+00:00",
    ///         "type": "Interval",
    ///     })
    /// );
    /// ```
    pub fn to_geo_json_event(&self) -> serde_json::Value {
        // TODO: Use proper time handling, e.g., define a BOT/EOT, …

        serde_json::json!({
            "start": self.start.as_rfc3339(),
            "end": self.end.as_rfc3339(),
            "type": "Interval"
        })
    }
}

impl Debug for TimeInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "TimeInterval [{}, {})",
            self.start.inner(),
            &self.end.inner()
        )
    }
}

impl Display for TimeInterval {
    /// Display the interval in its close-open form
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::TimeInterval;
    ///
    /// assert_eq!(format!("{}", TimeInterval::new(0, 1).unwrap()), "[0, 1)");
    /// ```
    ///
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "[{}, {})", self.start.inner(), self.end.inner())
    }
}

impl PartialOrd for TimeInterval {
    /// Order intervals whether they are completely before, equal or after each other or in-between (unordered)
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::TimeInterval;
    ///
    /// assert_eq!(
    ///     TimeInterval::new(0, 1).unwrap(),
    ///     TimeInterval::new(0, 1).unwrap()
    /// );
    /// assert_ne!(
    ///     TimeInterval::new(0, 1).unwrap(),
    ///     TimeInterval::new(1, 2).unwrap()
    /// );
    ///
    /// assert!(TimeInterval::new(0, 1).unwrap() <= TimeInterval::new(0, 1).unwrap());
    /// assert!(TimeInterval::new(0, 1).unwrap() <= TimeInterval::new(1, 2).unwrap());
    /// assert!(TimeInterval::new(0, 1).unwrap() < TimeInterval::new(1, 2).unwrap());
    ///
    /// assert!(TimeInterval::new(0, 1).unwrap() >= TimeInterval::new(0, 1).unwrap());
    /// assert!(TimeInterval::new(1, 2).unwrap() >= TimeInterval::new(0, 1).unwrap());
    /// assert!(TimeInterval::new(1, 2).unwrap() > TimeInterval::new(0, 1).unwrap());
    ///
    /// assert!(TimeInterval::new(0, 2)
    ///     .unwrap()
    ///     .partial_cmp(&TimeInterval::new(1, 3).unwrap())
    ///     .is_none());
    ///
    /// assert!(TimeInterval::new(0, 1)
    ///     .unwrap()
    ///     .partial_cmp(&TimeInterval::new(0, 2).unwrap())
    ///     .is_none());
    /// ```
    ///
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.eq(other) {
            Some(Ordering::Equal)
        } else if self.end <= other.start {
            Some(Ordering::Less)
        } else if self.start >= other.end {
            Some(Ordering::Greater)
        } else {
            None
        }
    }
}

impl ArrowTyped for TimeInterval {
    type ArrowArray = arrow::array::FixedSizeListArray;
    type ArrowBuilder = arrow::array::FixedSizeListBuilder<arrow::array::Date64Builder>;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::FixedSizeList(
            arrow::datatypes::DataType::Date64(arrow::datatypes::DateUnit::Millisecond).into(),
            2,
        )
    }

    fn arrow_builder(capacity: usize) -> Self::ArrowBuilder {
        arrow::array::FixedSizeListBuilder::new(arrow::array::Date64Builder::new(2 * capacity), 2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_geo_json_event() {
        let min_visualizable_value = -8_334_632_851_200_001 + 1;
        let max_visualizable_value = 8_210_298_412_800_000 - 1;

        assert_eq!(
            TimeInterval::new_unchecked(min_visualizable_value, max_visualizable_value)
                .to_geo_json_event(),
            serde_json::json!({
                "start": "-262144-01-01T00:00:00+00:00",
                "end": "+262143-12-31T23:59:59.999+00:00",
                "type": "Interval",
            })
        );
        assert_eq!(
            TimeInterval::new_unchecked(min_visualizable_value - 1, max_visualizable_value + 1)
                .to_geo_json_event(),
            serde_json::json!({
                "start": "-262144-01-01T00:00:00+00:00",
                "end": "+262143-12-31T23:59:59.999+00:00",
                "type": "Interval",
            })
        );
        assert_eq!(
            TimeInterval::new_unchecked(i64::MIN, i64::MAX).to_geo_json_event(),
            serde_json::json!({
                "start": "-262144-01-01T00:00:00+00:00",
                "end": "+262143-12-31T23:59:59.999+00:00",
                "type": "Interval",
            })
        );
    }
}
