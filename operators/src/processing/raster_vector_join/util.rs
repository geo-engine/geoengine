use geoengine_datatypes::primitives::TimeInterval;
use std::iter::Enumerate;

/// A `FeatureTimeSpan` combines a `TimeInterval` with a set of features it spans over.
/// Thus, it is used in combination with a `FeatureCollection`.
///
/// Both, `feature_index_start` and `feature_index_end` are inclusive.
///
#[derive(Debug, Clone, PartialEq)]
pub struct FeatureTimeSpan {
    pub feature_index_start: usize,
    pub feature_index_end: usize,
    pub time_interval: TimeInterval,
}

/// An iterator over `FeatureTimeSpan`s of `TimeInterval`s of a sorted `FeatureCollection`.
///
/// The `TimeInterval`s must be sorted ascending in order for the iterator to work.
///
pub struct FeatureTimeSpanIter<'c> {
    time_intervals: Enumerate<std::slice::Iter<'c, TimeInterval>>,
    current_time_span: Option<FeatureTimeSpan>,
}

impl<'c> FeatureTimeSpanIter<'c> {
    pub fn new(time_intervals: &'c [TimeInterval]) -> Self {
        Self {
            time_intervals: time_intervals.iter().enumerate(),
            current_time_span: None,
        }
    }
}

impl<'c> Iterator for FeatureTimeSpanIter<'c> {
    type Item = FeatureTimeSpan;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((idx, time_interval)) = self.time_intervals.next() {
            match self.current_time_span.take() {
                None => {
                    // nothing there yet? store it as the beginning

                    self.current_time_span = Some(FeatureTimeSpan {
                        feature_index_start: idx,
                        feature_index_end: idx,
                        time_interval: *time_interval,
                    });
                }
                Some(mut time_span) => {
                    if let Ok(combined_time_interval) =
                        time_span.time_interval.union(&time_interval)
                    {
                        // merge time intervals if possible

                        time_span.time_interval = combined_time_interval;
                        time_span.feature_index_end = idx;

                        self.current_time_span = Some(time_span);
                    } else {
                        // store current time interval for next span

                        self.current_time_span = Some(FeatureTimeSpan {
                            feature_index_start: idx,
                            feature_index_end: idx,
                            time_interval: *time_interval,
                        });

                        return Some(time_span);
                    }
                }
            }
        }

        // output last time span or `None`
        self.current_time_span.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_spans() {
        let time_spans = FeatureTimeSpanIter::new(&[
            TimeInterval::new_unchecked(0, 4),
            TimeInterval::new_unchecked(2, 6),
            TimeInterval::new_unchecked(7, 9),
            TimeInterval::new_unchecked(9, 11),
            TimeInterval::new_unchecked(13, 14),
        ])
        .collect::<Vec<_>>();

        assert_eq!(
            time_spans,
            vec![
                FeatureTimeSpan {
                    feature_index_start: 0,
                    feature_index_end: 1,
                    time_interval: TimeInterval::new_unchecked(0, 6)
                },
                FeatureTimeSpan {
                    feature_index_start: 2,
                    feature_index_end: 3,
                    time_interval: TimeInterval::new_unchecked(7, 11)
                },
                FeatureTimeSpan {
                    feature_index_start: 4,
                    feature_index_end: 4,
                    time_interval: TimeInterval::new_unchecked(13, 14)
                }
            ]
        );
    }
}
