use geoengine_datatypes::primitives::TimeInterval;
use std::iter::Enumerate;

#[derive(Debug, Clone, PartialEq)]
pub struct TimeSpan {
    pub idx_from: usize,
    pub idx_to: usize,
    pub time_interval: TimeInterval,
}

pub struct TimeSpanIter<'c> {
    time_intervals: Enumerate<std::slice::Iter<'c, TimeInterval>>,
    current_time_span: Option<TimeSpan>,
}

impl<'c> TimeSpanIter<'c> {
    pub fn new(time_intervals: &'c [TimeInterval]) -> Self {
        Self {
            time_intervals: time_intervals.iter().enumerate(),
            current_time_span: None,
        }
    }
}

impl<'c> Iterator for TimeSpanIter<'c> {
    type Item = TimeSpan;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((idx, time_interval)) = self.time_intervals.next() {
            match self.current_time_span.take() {
                None => {
                    // nothing there yet? store it as the beginning

                    self.current_time_span = Some(TimeSpan {
                        idx_from: idx,
                        idx_to: idx,
                        time_interval: *time_interval,
                    });
                }
                Some(mut time_span) => {
                    if let Ok(combined_time_interval) =
                        time_span.time_interval.union(&time_interval)
                    {
                        // merge time intervals if possible

                        time_span.time_interval = combined_time_interval;
                        time_span.idx_to = idx;

                        self.current_time_span = Some(time_span);
                    } else {
                        // store current time interval for next span

                        self.current_time_span = Some(TimeSpan {
                            idx_from: idx,
                            idx_to: idx,
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
        let time_spans = TimeSpanIter::new(&[
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
                TimeSpan {
                    idx_from: 0,
                    idx_to: 1,
                    time_interval: TimeInterval::new_unchecked(0, 6)
                },
                TimeSpan {
                    idx_from: 2,
                    idx_to: 3,
                    time_interval: TimeInterval::new_unchecked(7, 11)
                },
                TimeSpan {
                    idx_from: 4,
                    idx_to: 4,
                    time_interval: TimeInterval::new_unchecked(13, 14)
                }
            ]
        );
    }
}
