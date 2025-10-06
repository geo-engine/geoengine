use crate::primitives::TimeStep;
use crate::primitives::{TimeInstance, TimeInterval};
use std::iter::Peekable;

pub trait TimeFilledItem {
    fn create_fill_element(ti: TimeInterval) -> Self;
    fn time(&self) -> TimeInterval;
}

impl TimeFilledItem for TimeInterval {
    fn create_fill_element(ti: TimeInterval) -> Self {
        ti
    }

    fn time(&self) -> TimeInterval {
        // TODO: use Result here? Then, we coult impl TimeFilledItem for Option<TimeInterval> and Result<TimeInterval, E>...
        *self
    }
}

pub trait IrregularTimeFillIterExt<T: TimeFilledItem>: Iterator<Item = T>
where
    Self: Iterator<Item = T> + Sized,
{
    /// This creates an Iterator where time gaps between items are filled with a single tile inteval
    fn irregular_time_gap_fill(self) -> TimeGapFillAdapterIter<T, Self> {
        TimeGapFillAdapterIter::new(self)
    }

    /// This creates an Iterator where a single fill item is prepended if the first item from the source starts after the specified start
    fn irregular_time_start_prepend(
        self,
        first_element_start_required: TimeInstance,
    ) -> TimeSinglePrependAdapterIter<T, Self> {
        TimeSinglePrependAdapterIter::new(self, first_element_start_required)
    }

    /// This creates an Iterator where a single fill item is appended if the last item from the source ends after the specified end
    fn irregular_time_end_append(
        self,
        last_element_end_required: TimeInstance,
    ) -> TimeSingleAppendAdapterIter<T, Self> {
        TimeSingleAppendAdapterIter::new(self, last_element_end_required)
    }

    /// This creates an Iterator where:
    /// - IF the source is empty
    ///     - a single fill item is returned covering the source is empty
    /// - ELSE
    ///     - a single fill item is prepended if the first item from the source starts after the specified start
    ///     - time gaps between items are filled with a single tile inteval
    ///     - a single fill item is appended if the last item from the source ends after the specified end
    ///
    fn irregular_time_range_covered_or_default_range(
        self,
        range_to_cover: TimeInterval,
    ) -> TimeGapFillRangeAdapterIter<T, Self> {
        TimeGapFillRangeAdapterIter::new(self, range_to_cover)
    }
}

impl<T: TimeFilledItem, I: Iterator<Item = T>> IrregularTimeFillIterExt<T> for I {}

pub struct TimeGapFillAdapterIter<T: TimeFilledItem, I: Iterator<Item = T>> {
    source: Peekable<I>,
    last_time_end: Option<TimeInstance>,
}

impl<T: TimeFilledItem, I: Iterator<Item = T>> TimeGapFillAdapterIter<T, I> {
    pub fn new(source: I) -> Self {
        let peekable_source = Iterator::peekable(source);

        Self {
            source: peekable_source,
            last_time_end: None,
        }
    }
}

impl<T: TimeFilledItem, I: Iterator<Item = T>> Iterator for TimeGapFillAdapterIter<T, I> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let peek_time = self.source.peek().map(T::time);
        let nn = match (self.last_time_end, peek_time) {
            // the source produced None in a previous next() call
            (None, None) => None,
            // the next element in the source is None. Nothing to fill here
            (Some(_), None) => None,
            // the source produces its first element
            (None, Some(_)) => self.source.next(),
            // the source produces an element that starts later then the prevous element ended
            (Some(last_end), Some(peek)) if last_end < peek.start() => Some(
                T::create_fill_element(TimeInterval::new_unchecked(last_end, peek.start())),
            ),
            // the next element aligns with the previous element
            (Some(_), Some(_)) => self.source.next(),
        };

        self.last_time_end = nn.as_ref().map(|t| t.time().end());

        nn
    }
}

pub struct TimeSinglePrependAdapterIter<T: TimeFilledItem, I: Iterator<Item = T>> {
    source: Peekable<I>,
    start: Option<TimeInstance>,
}

impl<T: TimeFilledItem, I: Iterator<Item = T>> TimeSinglePrependAdapterIter<T, I> {
    pub fn new(source: I, first_element_start_required: TimeInstance) -> Self {
        let peekable_source: Peekable<I> = Iterator::peekable(source);

        Self {
            source: peekable_source,
            start: Some(first_element_start_required),
        }
    }
}

impl<T: TimeFilledItem, I: Iterator<Item = T>> Iterator for TimeSinglePrependAdapterIter<T, I> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let peek_time = self.source.peek().map(T::time);
        let nn = match (self.start, peek_time) {
            // default case where the start has already been checked
            (None, _) => self.source.next(),
            // the source never produced an element, can't prepend an element here
            (Some(_start), None) => None,
            // the first element starts after the required start so a single fill element is inserted here
            (Some(start), Some(peek)) if start < peek.start() => Some(T::create_fill_element(
                TimeInterval::new_unchecked(start, peek.start()),
            )),
            // the first element starts bevore or equal to the requred first start
            (Some(_start), Some(_peek)) => self.source.next(),
        };

        self.start = None;
        nn
    }
}

pub struct TimeSingleAppendAdapterIter<T: TimeFilledItem, I: Iterator<Item = T>> {
    source: I,
    req_end: TimeInstance,
    last_end: Option<TimeInstance>,
}

impl<T: TimeFilledItem, I: Iterator<Item = T>> TimeSingleAppendAdapterIter<T, I> {
    pub fn new(source: I, last_element_end: TimeInstance) -> Self {
        Self {
            source,
            req_end: last_element_end,
            last_end: None,
        }
    }
}

impl<T: TimeFilledItem, I: Iterator<Item = T>> Iterator for TimeSingleAppendAdapterIter<T, I> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let nn = match (self.source.next(), self.last_end) {
            (Some(nn), _) => Some(nn),
            (None, Some(last_time)) if last_time < self.req_end => Some(T::create_fill_element(
                TimeInterval::new_unchecked(last_time, self.req_end),
            )),
            (None, _) => None,
        };

        self.last_end = nn.as_ref().map(|t| t.time().end());

        nn
    }
}

pub struct TimeGapFillRangeAdapterIter<T: TimeFilledItem, I: Iterator<Item = T>> {
    source: TimeSingleAppendAdapterIter<
        T,
        TimeSinglePrependAdapterIter<T, TimeGapFillAdapterIter<T, I>>,
    >,
    range_to_cover: TimeInterval,
    pristine: bool,
}

impl<T: TimeFilledItem, I: Iterator<Item = T>> TimeGapFillRangeAdapterIter<T, I> {
    pub fn new(source: I, range_to_cover: TimeInterval) -> Self {
        Self {
            source: source
                .irregular_time_gap_fill()
                .irregular_time_start_prepend(range_to_cover.start())
                .irregular_time_end_append(range_to_cover.end()),
            range_to_cover,
            pristine: true,
        }
    }
}

impl<T: TimeFilledItem, I: Iterator<Item = T>> Iterator for TimeGapFillRangeAdapterIter<T, I> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let is_fist_call = self.pristine;
        self.pristine = false;

        let nn = self.source.next();

        if is_fist_call && nn.is_none() {
            return Some(T::create_fill_element(self.range_to_cover));
        } else {
            nn
        }
    }
}

/*
impl<T: TimeFilledItem, I: Iterator<Item = T>> Iterator for TimeGapFillAdapterIter<T, I> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let nn = match (self.last_element_time, self.source.peek().map(T::time)) {
            // case where no TI is availabe from source
            (None, None) => Some(T::create_fill_element(self.req_range)),
            // first element from source, start is later then hint
            (None, Some(&s)) if s.start() > self.req_range.start() => Some(T::create_fill_element(
                TimeInterval::new_unchecked(self.req_range.start(), s.start()),
            )),
            // first element from source, start is before or equal to hint
            (None, Some(_)) => self.source.next(),
            // no new element from source
            (Some(last), None) if last.end() < self.req_range.end() => {
                Some(T::create_fill_element(TimeInterval::new_unchecked(
                    last.end(),
                    self.req_range.end(),
                )))
            }
            (Some(_), None) => None,
            (Some(last), Some(&peek)) if last.end() < peek.start() => Some(T::create_fill_element(
                TimeInterval::new_unchecked(last.end(), peek.start()),
            )),
            (Some(_), Some(_)) => self.source.next(),
        };

        self.last_element_time = nn.as_ref().map(|n| *n.time()); // TODO: maybe this could be done simpler

        nn
    }
}

     */

pub struct TimeGapFillRegularAdapterIter<T: TimeFilledItem, I: Iterator<Item = T>> {
    source: Peekable<I>,
    step: TimeStep,
    last_time: Option<TimeInterval>,
    finished: bool,
}

impl<T: TimeFilledItem, I: Iterator<Item = T>> TimeGapFillRegularAdapterIter<T, I> {
    pub fn new(source: I, step: TimeStep) -> Self {
        let peekable_source = Iterator::peekable(source);

        Self {
            source: peekable_source,
            step,
            last_time: None,
            finished: false,
        }
    }
}

impl<T: TimeFilledItem, I: Iterator<Item = T>> Iterator for TimeGapFillRegularAdapterIter<T, I> {
    type Item = crate::util::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let nn = match (self.last_time, self.source.peek().map(T::time)) {
            (None, None) => None,
            (None, Some(_)) => self.source.next().map(|n| Ok(n)),
            (Some(_last), None) => None,
            (Some(last), Some(next)) if last.end() < next.start() => Some(
                (last.end() + self.step)
                    .and_then(|nt| TimeInterval::new(last.end(), nt))
                    .map(|ni| T::create_fill_element(ni)),
            ),
            (Some(_last), Some(_next)) => {
                // TODO: add check that gap is timestep long
                self.source.next().map(|n| Ok(n))
            }
        };

        match nn.as_ref() {
            Some(Err(_)) | None => self.finished = true,
            Some(Ok(next)) => self.last_time = Some(next.time()),
        };

        nn
    }
}

pub struct TimeGapRegularPrependAdapterIter<I, T>
where
    I: Iterator<Item = T>,
{
    source: Peekable<I>,
    step: TimeStep,
    next_time: Option<TimeInstance>,
}

impl<I, T> TimeGapRegularPrependAdapterIter<I, T>
where
    I: Iterator<Item = T>,
{
    pub fn new(source: I, step: TimeStep, first_element_start_required: TimeInstance) -> Self {
        let peekable_source = Iterator::peekable(source);

        Self {
            source: peekable_source,
            step,
            next_time: Some(first_element_start_required),
        }
    }
}

impl<I, T> Iterator for TimeGapRegularPrependAdapterIter<I, T>
where
    I: Iterator<Item = T>,
    T: TimeFilledItem,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let peek_time = self.source.peek().map(|ti| ti.time().start());

        let nn = match (self.next_time, peek_time) {
            (None, _) => self.source.next().map(|n| n),
            (Some(_), None) => None,
            (Some(next_time), Some(peek)) if next_time < peek => {
                Some(TimeInterval::new_unchecked(
                    next_time,
                    (next_time + self.step).expect("TimeStep addition overflowed"), // TODO: check range overflow?
                ))
                .map(|ni| T::create_fill_element(ni))
                // TODO: add check that gap is timestep long
            }
            (Some(_), Some(_)) => self.source.next().map(|n| n), // TODO: add check that gap is timestep long
        };

        self.next_time = nn.as_ref().map(|n| n.time().end());

        nn
    }
}

pub struct TimeGapRegularAppendAdapterIter<I, T>
where
    I: Iterator<Item = T>,
{
    source: I,
    step: TimeStep,
    last_time: TimeInstance,
    last_seen: Option<TimeInstance>,
}

impl<I, T> TimeGapRegularAppendAdapterIter<I, T>
where
    I: Iterator<Item = T>,
{
    pub fn new(source: I, step: TimeStep, last_element_end_required: TimeInstance) -> Self {
        Self {
            source,
            step,
            last_time: last_element_end_required,
            last_seen: None,
        }
    }
}

impl<I, T> Iterator for TimeGapRegularAppendAdapterIter<I, T>
where
    I: Iterator<Item = T>,
    T: TimeFilledItem,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let nn = self.source.next();

        if let Some(nn) = nn {
            self.last_seen = Some(nn.time().end());
            return Some(nn);
        }

        if let Some(last_seen) = self.last_seen {
            if last_seen < self.last_time {
                let next_start = last_seen;
                let next_end = (last_seen + self.step).expect("TimeStep addition overflowed"); // TODO: check range overflow?

                self.last_seen = Some(next_end);
                return Some(T::create_fill_element(TimeInterval::new_unchecked(
                    next_start, next_end,
                )));
            } else {
                self.last_seen = None; // to avoid multiple appends
            }
        }

        return None;
    }
}

pub trait RegularTimeFillIterExt<T: TimeFilledItem>: Iterator<Item = T>
where
    Self: Iterator<Item = T> + Sized,
{
    /// This creates an Iterator where time gaps between items are filled with a single tile inteval
    fn regular_time_gap_fill(self, step: TimeStep) -> TimeGapFillRegularAdapterIter<T, Self> {
        TimeGapFillRegularAdapterIter::new(self, step)
    }

    /// This creates an Iterator where a single fill item is prepended if the first item from the source starts after the specified start
    fn regular_time_start_prepend(
        self,
        step: TimeStep,
        first_element_start_required: TimeInstance,
    ) -> TimeGapRegularPrependAdapterIter<Self, T> {
        TimeGapRegularPrependAdapterIter::new(self, step, first_element_start_required)
    }

    /// This creates an Iterator where a single fill item is appended if the last item from the source ends after the specified end
    fn regular_time_end_append(
        self,
        step: TimeStep,
        last_element_end_required: TimeInstance,
    ) -> TimeGapRegularAppendAdapterIter<Self, T> {
        TimeGapRegularAppendAdapterIter::new(self, step, last_element_end_required)
    }
}

impl<T: TimeFilledItem, I: Iterator<Item = T>> RegularTimeFillIterExt<T> for I {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_source_intervals() {
        let start = TimeInstance::from_millis(0).unwrap();
        let end = TimeInstance::from_millis(100).unwrap();
        let iter: TimeGapFillRangeAdapterIter<TimeInterval, _> = vec![]
            .into_iter()
            .irregular_time_range_covered_or_default_range(TimeInterval::new_unchecked(start, end));
        let result: Vec<_> = iter.collect();
        assert_eq!(
            result,
            vec![TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(100).unwrap()
            )]
        );
    }

    #[test]
    fn test_source_covers_entire_range() {
        let start = TimeInstance::from_millis(0).unwrap();
        let end = TimeInstance::from_millis(100).unwrap();
        let intervals = vec![TimeInterval::new_unchecked(
            TimeInstance::from_millis(0).unwrap(),
            TimeInstance::from_millis(100).unwrap(),
        )];
        let iter: TimeGapFillRangeAdapterIter<TimeInterval, _> = intervals
            .into_iter()
            .irregular_time_range_covered_or_default_range(TimeInterval::new_unchecked(start, end));
        let result: Vec<_> = iter.collect();
        assert_eq!(
            result,
            vec![TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(100).unwrap()
            )]
        );
    }

    #[test]
    fn test_source_starts_after_hint() {
        let start = TimeInstance::from_millis(0).unwrap();
        let end = TimeInstance::from_millis(100).unwrap();
        let intervals = vec![TimeInterval::new_unchecked(
            TimeInstance::from_millis(20).unwrap(),
            TimeInstance::from_millis(40).unwrap(),
        )];
        let iter: TimeGapFillRangeAdapterIter<TimeInterval, _> = intervals
            .into_iter()
            .irregular_time_range_covered_or_default_range(TimeInterval::new_unchecked(start, end));
        let result: Vec<_> = iter.collect();
        assert_eq!(
            result,
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(40).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(40).unwrap(),
                    TimeInstance::from_millis(100).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_source_starts_before_hint() {
        let start = TimeInstance::from_millis(10).unwrap();
        let end = TimeInstance::from_millis(50).unwrap();
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(20).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(30).unwrap(),
                TimeInstance::from_millis(40).unwrap(),
            ),
        ];
        let iter: TimeGapFillRangeAdapterIter<TimeInterval, _> = intervals
            .into_iter()
            .irregular_time_range_covered_or_default_range(TimeInterval::new_unchecked(start, end));
        let result: Vec<_> = iter.collect();
        assert_eq!(
            result,
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(30).unwrap(),
                    TimeInstance::from_millis(40).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(40).unwrap(),
                    TimeInstance::from_millis(50).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_source_with_gaps() {
        let start = TimeInstance::from_millis(0).unwrap();
        let end = TimeInstance::from_millis(100).unwrap();
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(10).unwrap(),
                TimeInstance::from_millis(20).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(40).unwrap(),
                TimeInstance::from_millis(60).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(80).unwrap(),
                TimeInstance::from_millis(90).unwrap(),
            ),
        ];
        let iter: TimeGapFillRangeAdapterIter<TimeInterval, _> = intervals
            .into_iter()
            .irregular_time_range_covered_or_default_range(TimeInterval::new_unchecked(start, end));
        let result: Vec<_> = iter.collect();
        assert_eq!(
            result,
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(10).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(40).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(40).unwrap(),
                    TimeInstance::from_millis(60).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(60).unwrap(),
                    TimeInstance::from_millis(80).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(80).unwrap(),
                    TimeInstance::from_millis(90).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(90).unwrap(),
                    TimeInstance::from_millis(100).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_source_ends_before_hint() {
        let start = TimeInstance::from_millis(0).unwrap();
        let end = TimeInstance::from_millis(100).unwrap();
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(20).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(20).unwrap(),
                TimeInstance::from_millis(40).unwrap(),
            ),
        ];
        let iter: TimeGapFillRangeAdapterIter<TimeInterval, _> = intervals
            .into_iter()
            .irregular_time_range_covered_or_default_range(TimeInterval::new_unchecked(start, end));
        let result: Vec<_> = iter.collect();
        assert_eq!(
            result,
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(40).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(40).unwrap(),
                    TimeInstance::from_millis(100).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_source_exactly_matches_hint() {
        let start = TimeInstance::from_millis(10).unwrap();
        let end = TimeInstance::from_millis(20).unwrap();
        let intervals = vec![TimeInterval::new_unchecked(
            TimeInstance::from_millis(10).unwrap(),
            TimeInstance::from_millis(20).unwrap(),
        )];
        let iter: TimeGapFillRangeAdapterIter<TimeInterval, _> = intervals
            .into_iter()
            .irregular_time_range_covered_or_default_range(TimeInterval::new_unchecked(start, end));
        let result: Vec<_> = iter.collect();
        assert_eq!(
            result,
            vec![TimeInterval::new_unchecked(
                TimeInstance::from_millis(10).unwrap(),
                TimeInstance::from_millis(20).unwrap()
            )]
        );
    }

    #[test]
    fn test_source_overlapping_start() {
        let start = TimeInstance::from_millis(10).unwrap();
        let end = TimeInstance::from_millis(50).unwrap();
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(30).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(40).unwrap(),
                TimeInstance::from_millis(60).unwrap(),
            ),
        ];
        let iter: TimeGapFillRangeAdapterIter<TimeInterval, _> = intervals
            .into_iter()
            .irregular_time_range_covered_or_default_range(TimeInterval::new_unchecked(start, end));
        let result: Vec<_> = iter.collect();
        assert_eq!(
            result,
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(30).unwrap(),
                    TimeInstance::from_millis(40).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(40).unwrap(),
                    TimeInstance::from_millis(60).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_regular_time_gap_fill_no_gaps() {
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(10).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(10).unwrap(),
                TimeInstance::from_millis(20).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(20).unwrap(),
                TimeInstance::from_millis(30).unwrap(),
            ),
        ];
        let step = TimeStep::millis(10);
        let iter = intervals.into_iter().regular_time_gap_fill(step);
        let result: Vec<_> = iter.map(|r| r.unwrap()).collect();
        assert_eq!(
            result,
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(10).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_regular_time_gap_fill_with_gaps() {
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(10).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(30).unwrap(),
                TimeInstance::from_millis(40).unwrap(),
            ),
        ];
        let step = TimeStep::millis(10);
        let iter = intervals.into_iter().regular_time_gap_fill(step);
        let result: Vec<_> = iter.map(|r| r.unwrap()).collect();
        assert_eq!(
            result,
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(10).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(30).unwrap(),
                    TimeInstance::from_millis(40).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_regular_time_start_prepend() {
        let intervals = vec![TimeInterval::new_unchecked(
            TimeInstance::from_millis(20).unwrap(),
            TimeInstance::from_millis(30).unwrap(),
        )];
        let step = TimeStep::millis(10);
        let start = TimeInstance::from_millis(0).unwrap();
        let iter = intervals
            .into_iter()
            .regular_time_start_prepend(step, start);
        let result: Vec<_> = iter.collect();
        assert_eq!(
            result,
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(10).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_regular_time_end_append() {
        let intervals = vec![TimeInterval::new_unchecked(
            TimeInstance::from_millis(0).unwrap(),
            TimeInstance::from_millis(10).unwrap(),
        )];
        let step = TimeStep::millis(10);
        let end = TimeInstance::from_millis(30).unwrap();
        let iter = intervals.into_iter().regular_time_end_append(step, end);
        let result: Vec<_> = iter.collect();
        assert_eq!(
            result,
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(10).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_regular_time_start_prepend_and_end_append() {
        let intervals = vec![TimeInterval::new_unchecked(
            TimeInstance::from_millis(20).unwrap(),
            TimeInstance::from_millis(30).unwrap(),
        )];
        let step = TimeStep::millis(10);
        let start = TimeInstance::from_millis(10).unwrap();
        let end = TimeInstance::from_millis(40).unwrap();
        let iter = intervals
            .into_iter()
            .regular_time_start_prepend(step, start)
            .regular_time_end_append(step, end);
        let result: Vec<_> = iter.collect();
        assert_eq!(
            result,
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(30).unwrap(),
                    TimeInstance::from_millis(40).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_regular_time_gap_fill_empty_source() {
        let intervals: Vec<TimeInterval> = vec![];
        let step = TimeStep::millis(10);
        let iter = intervals.into_iter().regular_time_gap_fill(step);
        let result: Vec<TimeInterval> = iter.map(|r| r.unwrap()).collect();
        assert_eq!(result, vec![]);
    }
}
