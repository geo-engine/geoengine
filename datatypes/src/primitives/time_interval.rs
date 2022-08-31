use crate::primitives::TimeInstance;
use crate::util::arrow::{downcast_array, ArrowTyped};
use crate::util::Result;
use crate::{error, util::ranges::value_in_range};
use arrow::array::{Array, ArrayBuilder, BooleanArray};
use arrow::datatypes::{DataType, Field};
use arrow::error::ArrowError;
#[cfg(feature = "postgres")]
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::fmt::{Debug, Display};
use std::{cmp::Ordering, convert::TryInto};

/// Stores time intervals in ms in close-open semantic [start, end)
#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq, utoipa::ToSchema)]
#[cfg_attr(feature = "postgres", derive(ToSql, FromSql))]
#[repr(C)]
pub struct TimeInterval {
    start: TimeInstance,
    end: TimeInstance,
}

impl Default for TimeInterval {
    /// The default time interval is always valid.
    ///
    /// It aligns with `chrono`'s minimum and maximum datetime.
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
            start: TimeInstance::MIN,
            end: TimeInstance::MAX,
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
        A: TryInto<TimeInstance>,
        B: TryInto<TimeInstance>,
        error::Error: From<A::Error> + From<B::Error>,
    {
        let start_instant = start.try_into()?;
        let end_instant = end.try_into()?;

        ensure!(
            start_instant <= end_instant,
            error::TimeIntervalEndBeforeStart {
                start: start_instant,
                end: end_instant
            }
        );
        ensure!(
            start_instant >= TimeInstance::MIN && end_instant <= TimeInstance::MAX,
            error::TimeIntervalOutOfBounds {
                start: start_instant,
                end: end_instant,
                min: TimeInstance::MIN,
                max: TimeInstance::MAX,
            }
        );

        Ok(Self {
            start: start_instant,
            end: end_instant,
        })
    }

    /// Creates a new time interval from a single input that implements `Into<TimeInstance>`.
    /// After instanciation, start and end are equal.
    pub fn new_instant<A>(start_and_end: A) -> Result<Self>
    where
        A: TryInto<TimeInstance>,
        error::Error: From<A::Error>,
    {
        let start_and_end = start_and_end.try_into()?;
        Ok(Self {
            start: start_and_end,
            end: start_and_end,
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
    /// # Panics
    /// Panics if start and end are not compatible to [chrono].
    ///
    pub fn new_unchecked<A, B>(start: A, end: B) -> Self
    where
        A: TryInto<TimeInstance>,
        B: TryInto<TimeInstance>,
        A::Error: Debug,
        B::Error: Debug,
    {
        let start = start.try_into().unwrap();
        let end = end.try_into().unwrap();
        debug_assert!(start <= end);
        Self { start, end }
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
        other == self
            || value_in_range(self.start, other.start, other.end)
            || value_in_range(other.start, self.start, self.end)
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
            start: TimeInstance::min(self.start, other.start),
            end: TimeInstance::max(self.end, other.end),
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
    ///     TimeInterval::new_unchecked(0, 1585069448 * 1000).as_geo_json_event(),
    ///     serde_json::json!({
    ///         "start": "1970-01-01T00:00:00+00:00",
    ///         "end": "2020-03-24T17:04:08+00:00",
    ///         "type": "Interval",
    ///     })
    /// );
    /// ```
    pub fn as_geo_json_event(&self) -> serde_json::Value {
        serde_json::json!({
            "start": self.start.as_rfc3339(),
            "end": self.end.as_rfc3339(),
            "type": "Interval"
        })
    }

    /// Return a new time interval that is the intersection with the `other` time interval, or
    /// `None` if the intervals are disjoint
    pub fn intersect(self, other: &Self) -> Option<TimeInterval> {
        if self.intersects(other) {
            let start = std::cmp::max(self.start, other.start);
            let end = std::cmp::min(self.end, other.end);
            Some(Self::new_unchecked(start, end))
        } else {
            None
        }
    }

    /// Returns the duration of the interval
    /// This is the difference between the start and end time.
    /// If the start and end time are equal i.e. the interval is an instant, the duration is 0.
    pub fn duration_ms(&self) -> u64 {
        self.end.inner().wrapping_sub(self.start.inner()) as u64
    }

    pub fn is_instant(&self) -> bool {
        self.start == self.end
    }

    /// Extends a time interval with the bounds of another time interval.
    /// The result has the smaller `start` and the larger `end`.
    #[must_use]
    pub fn extend(&self, other: &Self) -> TimeInterval {
        Self {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
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

impl From<TimeInstance> for TimeInterval {
    fn from(time_instance: TimeInstance) -> Self {
        Self::new_unchecked(time_instance, time_instance)
    }
}

impl ArrowTyped for TimeInterval {
    type ArrowArray = arrow::array::FixedSizeListArray;
    // TODO: use date if dates out-of-range is fixed for us
    // type ArrowBuilder = arrow::array::FixedSizeListBuilder<arrow::array::Date64Builder>;
    type ArrowBuilder = arrow::array::FixedSizeListBuilder<arrow::array::Int64Builder>;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        // TODO: use date if dates out-of-range is fixed for us
        // DataType::FixedSizeList(Box::new(Field::new("item", DataType::Date64(arrow::datatypes::DateUnit::Millisecond), nullable)), 2)

        let nullable = true; // TODO: should actually be false, but arrow's builders set it to `true` currently

        DataType::FixedSizeList(Box::new(Field::new("item", DataType::Int64, nullable)), 2)
    }

    fn builder_byte_size(builder: &mut Self::ArrowBuilder) -> usize {
        builder.values().len() * std::mem::size_of::<i64>()
    }

    fn arrow_builder(capacity: usize) -> Self::ArrowBuilder {
        // TODO: use date if dates out-of-range is fixed for us
        // arrow::array::FixedSizeListBuilder::new(arrow::array::Date64Builder::new(2 * capacity), 2)

        arrow::array::FixedSizeListBuilder::new(arrow::array::Int64Builder::new(2 * capacity), 2)
    }

    fn concat(a: &Self::ArrowArray, b: &Self::ArrowArray) -> Result<Self::ArrowArray, ArrowError> {
        let new_length = a.len() + b.len();
        let mut new_time_intervals = TimeInterval::arrow_builder(new_length);

        {
            // TODO: use date if dates out-of-range is fixed for us
            // use arrow::array::Date64Array;
            use arrow::array::Int64Array;

            let int_builder = new_time_intervals.values();

            let ints_a_ref = a.values();
            let ints_b_ref = b.values();

            let ints_a: &Int64Array = downcast_array(&ints_a_ref);
            let ints_b: &Int64Array = downcast_array(&ints_b_ref);

            int_builder.append_slice(ints_a.values());
            int_builder.append_slice(ints_b.values());
        }

        for _ in 0..new_length {
            new_time_intervals.append(true);
        }

        Ok(new_time_intervals.finish())
    }

    fn filter(
        time_intervals: &Self::ArrowArray,
        filter_array: &BooleanArray,
    ) -> Result<Self::ArrowArray, ArrowError> {
        // TODO: use date if dates out-of-range is fixed for us
        // use arrow::array::Date64Array;
        use arrow::array::Int64Array;

        let mut new_time_intervals = Self::arrow_builder(0);

        for feature_index in 0..time_intervals.len() {
            if !filter_array.value(feature_index) {
                continue;
            }

            let old_timestamps_ref = time_intervals.value(feature_index);
            let old_timestamps: &Int64Array = downcast_array(&old_timestamps_ref);

            let date_builder = new_time_intervals.values();
            date_builder.append_slice(old_timestamps.values());

            new_time_intervals.append(true);
        }

        Ok(new_time_intervals.finish())
    }

    fn from_vec(time_intervals: Vec<Self>) -> Result<Self::ArrowArray, ArrowError>
    where
        Self: Sized,
    {
        // TODO: build faster(?) without builder

        let mut builder = Self::arrow_builder(time_intervals.len());
        for time_interval in time_intervals {
            let date_builder = builder.values();
            date_builder.append_value(time_interval.start().into());
            date_builder.append_value(time_interval.end().into());
            builder.append(true);
        }

        Ok(builder.finish())
    }
}

/// Compute the extent of all input time intervals. If one time interval is None, the output will also be None
pub fn time_interval_extent<I: Iterator<Item = Option<TimeInterval>>>(
    mut times: I,
) -> Option<TimeInterval> {
    let mut extent = if let Some(Some(first)) = times.next() {
        first
    } else {
        return None;
    };

    for time in times {
        if let Some(time) = time {
            extent = extent.extend(&time);
        } else {
            return None;
        }
    }

    Some(extent)
}

#[cfg(test)]
mod tests {
    use crate::primitives::DateTime;

    use super::*;

    #[test]
    fn to_geo_json_event() {
        let min_visualizable_value = -8_334_632_851_200_001 + 1;
        let max_visualizable_value = 8_210_298_412_800_000 - 1;

        assert_eq!(
            TimeInterval::new_unchecked(min_visualizable_value, max_visualizable_value)
                .as_geo_json_event(),
            serde_json::json!({
                "start": "-262144-01-01T00:00:00+00:00",
                "end": "+262143-12-31T23:59:59.999+00:00",
                "type": "Interval",
            })
        );
        assert_eq!(
            TimeInterval::new_unchecked(
                TimeInstance::from_millis_unchecked(min_visualizable_value - 1),
                TimeInstance::from_millis_unchecked(max_visualizable_value + 1)
            )
            .as_geo_json_event(),
            serde_json::json!({
                "start": "-262144-01-01T00:00:00+00:00",
                "end": "+262143-12-31T23:59:59.999+00:00",
                "type": "Interval",
            })
        );
        assert_eq!(
            TimeInterval::new_unchecked(
                TimeInstance::from_millis_unchecked(i64::MIN),
                TimeInstance::from_millis_unchecked(i64::MAX)
            )
            .as_geo_json_event(),
            serde_json::json!({
                "start": "-262144-01-01T00:00:00+00:00",
                "end": "+262143-12-31T23:59:59.999+00:00",
                "type": "Interval",
            })
        );
    }

    #[test]
    fn duration_millis() {
        assert_eq!(
            TimeInterval::default().duration_ms(),
            16_544_931_263_999_999
        );

        let time_interval = TimeInterval::new(
            TimeInstance::from(DateTime::new_utc(1990, 1, 1, 0, 0, 0)),
            TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0)),
        )
        .unwrap();

        assert_eq!(time_interval.duration_ms(), 315_532_800_000);

        assert_eq!(
            TimeInterval::new(-1, TimeInstance::MAX)
                .unwrap()
                .duration_ms(),
            8_210_298_412_800_000
        );
        assert_eq!(
            TimeInterval::new(0, TimeInstance::MAX)
                .unwrap()
                .duration_ms(),
            8_210_298_412_799_999
        );

        assert_eq!(
            TimeInterval::new(TimeInstance::MIN, -1)
                .unwrap()
                .duration_ms(),
            8_334_632_851_199_999
        );
        assert_eq!(
            TimeInterval::new(TimeInstance::MIN, 0)
                .unwrap()
                .duration_ms(),
            8_334_632_851_200_000
        );
        assert_eq!(
            TimeInterval::new(TimeInstance::MIN, 1)
                .unwrap()
                .duration_ms(),
            8_334_632_851_200_001
        );
    }

    #[test]
    fn bounds() {
        let t = TimeInterval::default();

        assert_eq!(t.start(), TimeInstance::MIN);
        assert_eq!(t.end(), TimeInstance::MAX);
    }

    #[test]
    fn intersects_same() {
        let a = TimeInterval::new(2, 4).unwrap();
        let b = TimeInterval::new(2, 4).unwrap();

        assert!(a.intersects(&b));
    }

    #[test]
    fn intersects_before() {
        let a = TimeInterval::new(1, 2).unwrap();
        let b = TimeInterval::new(2, 3).unwrap();

        assert!(!a.intersects(&b));
    }

    #[test]
    fn intersects_overlap_left() {
        let a = TimeInterval::new(1, 3).unwrap();
        let b = TimeInterval::new(2, 4).unwrap();

        assert!(a.intersects(&b));
    }

    #[test]
    fn intersects_inside() {
        let a = TimeInterval::new(3, 4).unwrap();
        let b = TimeInterval::new(2, 4).unwrap();

        assert!(a.intersects(&b));
    }

    #[test]
    fn intersects_inside_instance_a() {
        let a = TimeInterval::new_instant(3).unwrap();
        let b = TimeInterval::new(2, 4).unwrap();

        assert!(a.intersects(&b));
    }

    #[test]
    fn intersects_inside_instance_b() {
        let a = TimeInterval::new_instant(3).unwrap();
        let b = TimeInterval::new(2, 4).unwrap();

        assert!(b.intersects(&a));
    }

    #[test]
    fn intersects_overlap_right() {
        let a = TimeInterval::new(1, 3).unwrap();
        let b = TimeInterval::new(2, 4).unwrap();

        assert!(b.intersects(&a));
    }

    #[test]
    fn intersects_after() {
        let a = TimeInterval::new(1, 2).unwrap();
        let b = TimeInterval::new(2, 3).unwrap();

        assert!(!b.intersects(&a));
    }

    #[test]
    fn extend() {
        let a = TimeInterval::new(1, 2).unwrap();
        let b = TimeInterval::new(2, 3).unwrap();

        assert_eq!(a.extend(&b), TimeInterval::new_unchecked(1, 3));
    }

    #[test]
    fn is_instant() {
        let a = TimeInterval::new(1, 1).unwrap();
        let b = TimeInterval::new(2, 3).unwrap();

        assert!(a.is_instant());
        assert!(!b.is_instant());
    }

    #[test]
    fn extent() {
        assert_eq!(time_interval_extent([None].into_iter()), None);
        assert_eq!(
            time_interval_extent(
                [
                    Some(TimeInterval::new(1, 2).unwrap()),
                    Some(TimeInterval::new(5, 6).unwrap())
                ]
                .into_iter()
            ),
            Some(TimeInterval::new_unchecked(1, 6))
        );
        assert_eq!(
            time_interval_extent([Some(TimeInterval::new(1, 2).unwrap()), None].into_iter()),
            None
        );
        assert_eq!(
            time_interval_extent([None, Some(TimeInterval::new(5, 6).unwrap())].into_iter()),
            None
        );
    }
}
