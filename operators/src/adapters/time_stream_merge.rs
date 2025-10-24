use futures::stream::{Fuse, Stream};
use geoengine_datatypes::primitives::TimeInterval;
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

#[pin_project]
pub struct TimeIntervalStreamMerge<S, E>
where
    S: Stream<Item = Result<TimeInterval, E>>,
{
    #[pin]
    streams: Vec<Fuse<S>>,
    current: Vec<Option<TimeInterval>>,
    current_time: Option<TimeInterval>,
}

impl<S, E> TimeIntervalStreamMerge<S, E>
where
    S: Stream<Item = Result<TimeInterval, E>> + Unpin,
{
    pub fn new(streams: Vec<S>) -> Self {
        let len = streams.len();
        Self {
            streams: streams.into_iter().map(futures::StreamExt::fuse).collect(),
            current: vec![None; len],
            current_time: None,
        }
    }
}

impl<S, E> Stream for TimeIntervalStreamMerge<S, E>
where
    S: Stream<Item = Result<TimeInterval, E>> + Unpin,
{
    type Item = Result<TimeInterval, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Now, fill the current intervals from the streams if they are None.
        // This happens either at the start, or after producing an interval.
        // TODO: do this concurrently
        for (i, stream) in this.streams.iter_mut().enumerate() {
            if this.current[i].is_none() && !stream.is_done() {
                match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(interval))) => this.current[i] = Some(interval),
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => this.current[i] = None,
                    Poll::Pending => return Poll::Pending,
                }
            }
        }

        // If all streams are exhausted, we are done
        if this.current.iter().all(std::option::Option::is_none) {
            return Poll::Ready(None);
        }

        // Collect all available intervals
        let mut active: Vec<(usize, TimeInterval)> = Vec::new();
        for (i, interval) in this.current.iter().enumerate() {
            if let Some(interval) = interval {
                active.push((i, *interval));
            }
        }

        // If no active intervals, we are done
        // This is already covered by the all-none check above, but for safety...
        if active.is_empty() {
            return Poll::Ready(None);
        }

        // Find all intervals that contain the end of the current_time. The one with the minimum end is a candidate for the next merged interval.
        let candidate_case_a = this.current_time.and_then(|current_t| {
            active
                .iter()
                .filter(|(_a_i, a)| a.contains_instance(current_t.end()))
                .min_by_key(|(_a_i, a)| a.end())
                .map(|(i, interval)| {
                    (
                        *i,
                        TimeInterval::new_unchecked(current_t.end(), interval.end()),
                    )
                })
        });

        let candidate_case_b = this.current_time.and_then(|current_t| {
            active
                .iter()
                .filter(|(_, interval)| interval.start() > current_t.end())
                .min_by_key(|(_, interval)| interval.start())
                .map(|(i, interval)| (*i, *interval))
        });

        let candidate_case_c = active
            .iter()
            .min_by_key(|(_, interval)| interval.start())
            .map(|(i, interval)| (*i, *interval));

        let candidate = candidate_case_a.or(candidate_case_b).or(candidate_case_c);

        let Some((_candidate_i, candidate)) = candidate else {
            // This should not happen since we checked active is not empty
            return Poll::Ready(None);
        };

        // Now, we have a candidate but we need to check if there are other intervals that start before its end
        let candidate_start_as_end = active
            .iter()
            .filter(|(_a_i, a)| {
                candidate.contains_instance(a.start()) && a.start() > candidate.start()
            })
            .min_by_key(|(_a_i, a)| a.start())
            .map(|(i, interval)| (*i, *interval));

        // Produce the merged interval
        let merged_interval = if let Some((_, csae)) = candidate_start_as_end {
            TimeInterval::new_unchecked(candidate.start(), csae.start())
        } else {
            candidate
        };

        for interval in this.current.iter_mut() {
            if let Some(iv) = interval {
                if iv.end() <= merged_interval.end() {
                    *interval = None;
                }
            }
        }

        // Update current_time
        *this.current_time = Some(merged_interval);

        // Return the merged interval
        Poll::Ready(Some(Ok(merged_interval)))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[test]
    fn test_non_overlapping_intervals() {
        let stream1 = futures::stream::iter(vec![
            Result::<TimeInterval, &str>::Ok(TimeInterval::new_unchecked(1, 2)),
            Ok(TimeInterval::new_unchecked(3, 4)),
            Ok(TimeInterval::new_unchecked(9, 10)),
        ]);
        let stream2 = futures::stream::iter(vec![
            Ok(TimeInterval::new_unchecked(5, 6)),
            Ok(TimeInterval::new_unchecked(7, 8)),
            Ok(TimeInterval::new_unchecked(11, 12)),
        ]);

        let mut merged_stream = TimeIntervalStreamMerge::new(vec![stream1, stream2]);

        let expected = vec![
            TimeInterval::new_unchecked(1, 2),
            TimeInterval::new_unchecked(3, 4),
            TimeInterval::new_unchecked(5, 6),
            TimeInterval::new_unchecked(7, 8),
            TimeInterval::new_unchecked(9, 10),
            TimeInterval::new_unchecked(11, 12),
        ];

        for expected_interval in expected {
            let polled = futures::executor::block_on(merged_stream.next());
            assert_eq!(polled.unwrap().unwrap(), expected_interval);
        }

        assert!(futures::executor::block_on(merged_stream.next()).is_none());
    }

    #[test]
    fn test_touching_intervals() {
        let stream1 = futures::stream::iter(vec![
            Result::<TimeInterval, &str>::Ok(TimeInterval::new_unchecked(1, 3)),
            Ok(TimeInterval::new_unchecked(5, 7)),
            Ok(TimeInterval::new_unchecked(9, 11)),
        ]);
        let stream2 = futures::stream::iter(vec![
            Ok(TimeInterval::new_unchecked(3, 5)),
            Ok(TimeInterval::new_unchecked(7, 9)),
            Ok(TimeInterval::new_unchecked(11, 13)),
        ]);

        let mut merged_stream = TimeIntervalStreamMerge::new(vec![stream1, stream2]);

        let expected = vec![
            TimeInterval::new_unchecked(1, 3),
            TimeInterval::new_unchecked(3, 5),
            TimeInterval::new_unchecked(5, 7),
            TimeInterval::new_unchecked(7, 9),
            TimeInterval::new_unchecked(9, 11),
            TimeInterval::new_unchecked(11, 13),
        ];

        for expected_interval in expected {
            let polled = futures::executor::block_on(merged_stream.next());
            assert_eq!(polled.unwrap().unwrap(), expected_interval);
        }

        assert!(futures::executor::block_on(merged_stream.next()).is_none());
    }

    #[test]
    fn test_overlapping_intervals() {
        let stream1 = futures::stream::iter(vec![
            Result::<TimeInterval, &str>::Ok(TimeInterval::new_unchecked(1, 4)),
            Ok(TimeInterval::new_unchecked(6, 8)),
            Ok(TimeInterval::new_unchecked(10, 12)),
        ]);
        let stream2 = futures::stream::iter(vec![
            Ok(TimeInterval::new_unchecked(2, 5)),
            Ok(TimeInterval::new_unchecked(7, 9)),
            Ok(TimeInterval::new_unchecked(11, 13)),
        ]);
        let mut merged_stream = TimeIntervalStreamMerge::new(vec![stream1, stream2]);
        let expected = vec![
            TimeInterval::new_unchecked(1, 2),
            TimeInterval::new_unchecked(2, 4),
            TimeInterval::new_unchecked(4, 5),
            TimeInterval::new_unchecked(6, 7),
            TimeInterval::new_unchecked(7, 8),
            TimeInterval::new_unchecked(8, 9),
            TimeInterval::new_unchecked(10, 11),
            TimeInterval::new_unchecked(11, 12),
            TimeInterval::new_unchecked(12, 13),
        ];
        for expected_interval in expected {
            let polled = futures::executor::block_on(merged_stream.next());
            assert_eq!(polled.unwrap().unwrap(), expected_interval);
        }
        assert!(futures::executor::block_on(merged_stream.next()).is_none());
    }

    #[test]
    fn test_synchronized_non_overlapping_intervals() {
        let stream1 = futures::stream::iter(vec![
            Result::<TimeInterval, &str>::Ok(TimeInterval::new_unchecked(1, 5)),
            Ok(TimeInterval::new_unchecked(6, 10)),
            Ok(TimeInterval::new_unchecked(11, 15)),
        ]);
        let stream2 = futures::stream::iter(vec![
            Ok(TimeInterval::new_unchecked(1, 5)),
            Ok(TimeInterval::new_unchecked(6, 10)),
            Ok(TimeInterval::new_unchecked(11, 15)),
        ]);
        let mut merged_stream = TimeIntervalStreamMerge::new(vec![stream1, stream2]);
        let expected = vec![
            TimeInterval::new_unchecked(1, 5),
            TimeInterval::new_unchecked(6, 10),
            TimeInterval::new_unchecked(11, 15),
        ];
        for expected_interval in expected {
            let polled = futures::executor::block_on(merged_stream.next());
            assert_eq!(polled.unwrap().unwrap(), expected_interval);
        }
        assert!(futures::executor::block_on(merged_stream.next()).is_none());
    }

    #[test]
    fn test_synchronized_touching_intervals() {
        let stream1 = futures::stream::iter(vec![
            Result::<TimeInterval, &str>::Ok(TimeInterval::new_unchecked(1, 3)),
            Ok(TimeInterval::new_unchecked(3, 6)),
            Ok(TimeInterval::new_unchecked(6, 9)),
        ]);
        let stream2 = futures::stream::iter(vec![
            Ok(TimeInterval::new_unchecked(1, 3)),
            Ok(TimeInterval::new_unchecked(3, 6)),
            Ok(TimeInterval::new_unchecked(6, 9)),
        ]);
        let mut merged_stream = TimeIntervalStreamMerge::new(vec![stream1, stream2]);
        let expected = vec![
            TimeInterval::new_unchecked(1, 3),
            TimeInterval::new_unchecked(3, 6),
            TimeInterval::new_unchecked(6, 9),
        ];
        for expected_interval in expected {
            let polled = futures::executor::block_on(merged_stream.next());
            assert_eq!(polled.unwrap().unwrap(), expected_interval);
        }
        assert!(futures::executor::block_on(merged_stream.next()).is_none());
    }
}
