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

/// The interval that follows `current_time` in the merge of several time streams, where `active`
/// are the intervals at the head of each stream. The merge splits at every start and end, so
/// a stream's interval either covers the result completely or does not overlap it.
///
/// Shared with the merge of footprint streams, so that both produce the same time steps.
pub(crate) fn next_merged_interval(
    current_time: Option<TimeInterval>,
    active: &[TimeInterval],
) -> Option<TimeInterval> {
    // The next interval starts where the current one ends if an input goes on there, else at the
    // earliest start after it.
    let after = current_time.map(|current| current.end());
    let start = after
        .filter(|&end| active.iter().any(|a| a.contains_instance(end)))
        .or_else(|| {
            active
                .iter()
                .map(TimeInterval::start)
                .filter(|&start| after.is_none_or(|end| start >= end))
                .min()
        })
        .or_else(|| active.iter().map(TimeInterval::start).min())?;

    // It ends at the next start or end of any input, so inputs that start together but end at
    // different times split it.
    let end = active
        .iter()
        .flat_map(|a| [a.start(), a.end()])
        .filter(|&instant| instant > start)
        .min()
        .unwrap_or(start);

    Some(TimeInterval::new_unchecked(start, end))
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
        let active: Vec<TimeInterval> = this.current.iter().flatten().copied().collect();

        let Some(merged_interval) = next_merged_interval(*this.current_time, &active) else {
            return Poll::Ready(None);
        };

        for interval in this.current.iter_mut() {
            if let Some(iv) = interval
                && iv.end() <= merged_interval.end()
            {
                *interval = None;
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

    #[test]
    fn it_intervals_with_the_same_start_and_different_ends() {
        let long = || {
            futures::stream::iter(vec![Result::<TimeInterval, &str>::Ok(
                TimeInterval::new_unchecked(0, 10),
            )])
        };
        let short = || {
            futures::stream::iter(vec![
                Ok(TimeInterval::new_unchecked(0, 5)),
                Ok(TimeInterval::new_unchecked(5, 10)),
            ])
        };

        for streams in [vec![long(), short()], vec![short(), long()]] {
            let merged: Vec<_> = futures::executor::block_on(
                TimeIntervalStreamMerge::new(streams)
                    .map(Result::unwrap)
                    .collect(),
            );
            assert_eq!(
                merged,
                [
                    TimeInterval::new_unchecked(0, 5),
                    TimeInterval::new_unchecked(5, 10)
                ]
            );
        }
    }
}
